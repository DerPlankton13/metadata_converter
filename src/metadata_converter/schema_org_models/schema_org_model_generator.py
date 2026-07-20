"""
Generate Pydantic v2 models from schema.org JSON-LD.

Run once to produce a static `schemaorg_models.py` file with no runtime cost.

Usage
-----
Generate all schema.org types::

    python schemaorg_codegen.py

Enforce exact field types (no str fallback for typed fields)::

    python schemaorg_codegen.py --strict

Write to a custom path::

    python schemaorg_codegen.py --out my_models.py

Then in your application::

    from schemaorg_models import Person

    person = Person(name="Ada Lovelace", email="ada@example.com")
"""

from __future__ import annotations

import argparse
import functools
import inspect
import json
import re
import subprocess
import sys
import textwrap
import urllib.request
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from keyword import iskeyword
from pathlib import Path
from types import UnionType
from typing import Any, TypedDict, Union, get_args, get_origin

from pydantic import AnyUrl, BaseModel, ConfigDict, Field, model_validator
from pydantic.fields import FieldInfo

SCHEMA_URL = "https://schema.org/version/latest/schemaorg-current-https.jsonld"
SCHEMA_PREFIX = "https://schema.org/"
DEFAULT_OUTPUT_PATH = Path(__file__).parent / "schemaorg_models.py"

# Maps primitive schema.org type names to Python types.
PRIMITIVE_TYPE_MAP: dict[str, Any] = {
    "Text": str,
    "URL": AnyUrl,
    "Boolean": bool,
    "Number": float,
    "Integer": int,
    "Float": float,
    "Date": date,
    "DateTime": datetime,
    "Time": time,
    "Duration": timedelta,
    "XPathType": str,
    "CssSelectorType": str,
    "PronounceableText": str,
}

# Maps Python type objects to their source-code names for the generated module.
PRIMITIVE_SOURCE: dict[Any, str] = {
    str: "str",
    bool: "bool",
    float: "float",
    int: "int",
    date: "date",
    datetime: "datetime",
    time: "time",
    timedelta: "timedelta",
    AnyUrl: "AnyUrl",
}


class ClassDef(TypedDict):
    """Parsed metadata for a single schema.org class."""

    parents: list[str]
    schema_name: str
    comment: str


class FieldDef(TypedDict):
    """Parsed metadata for a single schema.org property."""

    name: str
    schema_name: str
    allowed_types: list[str]
    comment: str


_SCHEMA_TYPE_REGISTRY: dict[str, type] = {}


class SchemaOrgBase(BaseModel):
    """
    Base class for all schema.org based Pydantic models.

    Provides the fields common to all JSON-LD nodes (``@context``, ``@type``,
    ``@id``, ``additionalProperty``) and registers every subclass in
    ``_SCHEMA_TYPE_REGISTRY`` as it is defined (see ``__init_subclass__``). That
    registration also covers project-specific ``PropertyValue`` subtypes such as
    ``Orcid`` and ``DOI`` in ``custom_models.py``.

    Notes
    -----
    Each ``model_config`` setting serves a distinct purpose:

    - ``extra="allow"`` keeps unknown fields in ``model_extra`` instead of
      rejecting them, so source properties outside the schema.org definition
      survive (``validate_strict`` opts back into strict checking).
    - ``populate_by_name`` accepts both Python attribute names and the JSON-LD
      ``@``-prefixed aliases interchangeably.
    - ``defer_build`` postpones schema build until first validation, massively
      reducing run time since most models are never used.
    - ``validate_assignment`` re-validates each attribute on assignment.
    - ``polymorphic_serialization`` is required because a field typed as a bare
      schema.org class (e.g. ``Thing``) holding a subtype instance (e.g.
      ``Product``, resolved by ``discriminate_typed_fields``) would otherwise
      serialize using the declared class's fields only, silently dropping
      subtype-only fields like ``category`` — see
      ``tests/schema/test_polymorphic_serialization.py``.
    """

    model_config = ConfigDict(
        extra="allow",
        populate_by_name=True,
        defer_build=True,
        validate_assignment=True,
        polymorphic_serialization=True,
    )

    # these are all not schema.org properties, but they are needed for jsonld
    context: str | dict[str, Any] | None = Field(default=None, alias="@context")
    # The schema.org class name, will be set automatically by each generated subclass.
    type: str = Field(alias="@type")
    id: str | None = Field(default=None, alias="@id")

    # this is our modification of schema.org, saying, that we always allow additionalProperty
    additionalProperty: PropertyValue | str | list[str | PropertyValue] | None = Field(
        default=None
    )

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Register every subclass in ``_SCHEMA_TYPE_REGISTRY`` as it is built.

        This hook is called by Python at class-creation time (i.e. the instantiation of
        the metaclass for classes).
        As soon as a SchemaOrgBase subclass is built (e.g. during an import), this hook
        is called and each subclass is registering itself, independent of where it is
        defined. In this manner, the subclasses defined outside of this module (e.g.
        ``custom_models.py``'s ``Orcid``/``DOI``) are registered as well after importing
        these modules.
        This hook is also used by Pydantic, so we do the ``super()`` call to not
        interfere with pydantic.
        """
        super().__init_subclass__(**kwargs)
        _SCHEMA_TYPE_REGISTRY[cls.__name__] = cls

    @model_validator(mode="before")
    @classmethod
    def discriminate_typed_fields(cls, data: Any) -> Any:
        """Discriminate pydantic model fields by the @type/type values from input data.

        This method looks into pydantic input data (e.g. a dictionary containing
        key-value pairs that map to the properties and values of a schema.org object)
        and attempts to discover if any keys in that dictionary map to values that are,
        themselves, dictionaries that contain a type (@type/type) key. Both spellings
        are supported. When found, this nested dictionary is converted into an instance
        of a pydantic class corresponding to the name of the type (e.g. Product). Thus,
        the key in the top level dictionary (e.g. instrument) no longer maps to a nested
        dictionary as a value, but to an instance of a pydantic class. Since pydantic
        does not re-validate already built models, this replaces pydantic's default type
        discrimination.
        This is necessary since pydantic fails to correctly discriminate child types
        for complex type annotations containing unions, as it is the case with the
        schema.org models. In this manner we can ensure that e.g. in the "Action" model
        the instrument property (which is annotated as "Thing") is built correctly as
        "Product" (which is a subclass of "Thing") if so declared in the input.
        Otherwise, the instrument would be built as an instance of "Thing" with
        additional properties and a value of "Product" for type.

        Parameters
        ----------
        cls :

        data : Any
            Raw input which is often a dict[str, Any] but could also be an instance of
            the model itself or anything else since you can pass arbitrary objects into model_validate

        Returns
        -------
        data : Any
            The possibly modified input from which the pydantic model will be built.

        """
        if not isinstance(data, dict):
            return data
        data = dict(data)
        for field_name, field_info in cls.model_fields.items():
            if field_name in data:
                key = field_name
            elif field_info.alias in data:
                key = field_info.alias
            else:
                continue
            data[key] = _discriminate_value(data[key], field_info.annotation)
        return data


def _discriminate_value(raw_input: Any, annotation: Any) -> Any:
    """
    Resolve the field type using ``@type/type`` value from the input if available.

    If the input specifies its own model type via its ``@type/type`` key, it is checked
    that this specified model is a) a registered schema model (i.e. a member of
    _SCHEMA_TYPE_REGISTRY) and b) a child class of one of the model types specified in
    the corresponding field annotation (``annotation``). If this is the case, it builds
    a validated model of the specified type and returns it. In this manner, pydantic
    will skip validation for this property of the owning model.
    Nested "typed" dicts and lists as input are resolved recursively.

    Parameters
    ----------
    raw_input : Any
        The raw, unvalidated data assigned to a field — a dict, a list, or anything
        else.
    annotation : Any
        The field's resolved (non-string) type annotation, passed to
        ``_collect_annotated_schema_types`` to determine the candidate subtypes.

    Returns
    -------
    Any
        ``raw_input`` with any resolvable dict(s) replaced by parsed subtype
        instances. Non-dict input, and dicts with no ``@type``/``type`` key, pass
        through unchanged.

    Raises
    ------
    ValueError
        If ``@type``/``type`` names a type not in ``_SCHEMA_TYPE_REGISTRY``, or a
        registered type that is not a subtype of any of the types referenced by
        ``annotation``.
    """
    # this inline call makes this function call part of the recursion, but since this
    # function is cashed, this should be cheap
    annotated_schema_types = _collect_annotated_schema_types(annotation)
    if isinstance(raw_input, list):
        return [_discriminate_value(item, annotation) for item in raw_input]
    # just return the raw input for non-dict (str, float, class) values and no
    # annotated model
    if isinstance(raw_input, dict) and annotated_schema_types:
        type_name = raw_input.get("@type") or raw_input.get("type")
        if type_name is not None:
            cls = _SCHEMA_TYPE_REGISTRY.get(type_name)
            if cls is None or not any(
                issubclass(cls, schema_type) for schema_type in annotated_schema_types
            ):
                declared = ", ".join(
                    sorted(
                        schema_type.__name__ for schema_type in annotated_schema_types
                    )
                )
                raise ValueError(f"{type_name!r} is not a known subtype of {declared}")
            return cls.model_validate(raw_input)
    return raw_input


@functools.lru_cache(maxsize=None)
def _collect_annotated_schema_types(type_annotation: Any) -> set[type]:
    """Collect all ``SchemaOrgBase`` subclasses contained in the type annotation.

    If the type annotation is a ``Union``/``X | Y`` or ``list``/``set``/``tuple`` it
    recursively searches for the SchemaOrgBase subclasses. Note that any class
    subclassing from SchemaOrgBase is accepted, independent of the fact, whether it is
    an actual representation of a schema.org type or a custom addition.

    This function is cashed (memoized) because it is called many times with exactly the
    same input and thus cashing saves a lot of redundant computation. The schema.org
    models share many same type annotations, which would have to be recomputed during
    each instantiation of a model.

    Parameters
    ----------
    type_annotation : Any
        A resolved (non-string) type annotation for a single pydantic model field —
        e.g. a class, a ``list``/``set``/``tuple`` generic alias, or a union.

    Returns
    -------
    set[type]
        The SchemaOrgBase child classes referenced in ``type_annotation``,
        possibly empty.

    Notes
    -----
    ``lru_cache`` returns the same ``set`` object on every call and not a copy,
    so callers must not mutate the returned set.
    """
    annotated_schema_types: set[type] = set()
    if _is_wrapped(type_annotation):
        for arg in get_args(type_annotation):
            annotated_schema_types |= _collect_annotated_schema_types(arg)
    # we need to check that type_annotation is a class to safely call issubclass
    elif isinstance(type_annotation, type) and issubclass(
        type_annotation, SchemaOrgBase
    ):
        annotated_schema_types.add(type_annotation)
    return annotated_schema_types


def _is_wrapped(type_annotation: Any) -> bool:
    """Return whether ``type_annotation`` is a union or a ``list``/``set``/``tuple``.

    Both wrap other types in their ``get_args()`` rather than being a class
    themselves, so the caller must unwrap them first to access the types.

    Parameters
    ----------
    type_annotation : Any
        A resolved (non-string) type annotation for a single pydantic model field —
        e.g. a class, a ``list``/``set``/``tuple`` generic alias, or a union.

    Returns
    -------
    bool
        ``True`` if ``type_annotation`` is a union or a ``list``/``set``/``tuple``,
        ``False`` otherwise.
    """
    type_origin = get_origin(type_annotation)
    # Union is looked for twice, since it has two spellings with different origins
    # (``typing.Union`` and ``types.UnionType``).
    is_union = type_origin is Union or type_origin is UnionType
    return is_union or type_origin in (list, set, tuple)


def validate_strict(model: object) -> None:
    """Raise if the schema.org model has additional, unspecified properties.

    Schema.org models are configured with ``extra="allow"``, which keeps unknown fields
    in ``model_extra`` instead of rejecting them. This function recursively checks that
    the model and any nested models have no ``model_extra`` field, raising on the first
    offender at any depth.

    Parameters
    ----------
    model : object
        The schema.org model to validate. Lists are walked element-wise and properties
        which are not pydantic models are skipped.

    Raises
    ------
    ValueError
        If the model or any nested model carries fields outside its schema.org
        definition as implemented in the pydantic models.
    """
    if isinstance(model, list):
        for item in model:
            validate_strict(item)
        return
    if not isinstance(model, BaseModel):
        return
    if model.model_extra:
        raise ValueError(
            f"{type(model).__name__} has fields outside the schema.org model: "
            f"{sorted(model.model_extra)}"
        )
    for field_name in type(model).model_fields:
        validate_strict(getattr(model, field_name))


def local(iri: str) -> str:
    """Extract local name from a schema.org IRI."""
    return iri.removeprefix(SCHEMA_PREFIX).removeprefix("schema:")


def safe_name(name: str) -> str:
    """
    Return a valid Python identifier for a schema.org local name.

    Applies the minimum change needed to make the name usable as a Python
    class name or attribute:

    - Names starting with a digit get a leading underscore (e.g.
      ``"3DModel"`` → ``"_3DModel"``).
    - Python reserved keywords get a trailing underscore (e.g.
      ``"yield"`` → ``"yield_"``, following PEP 8 convention).
    - Names that are already valid are returned unchanged.

    When the safe name differs from the original, it needs to be stored
    as a Pydantic ``alias`` so serialisation uses the correct
    schema.org name.

    Parameters
    ----------
    name : str
        A schema.org local name.

    Returns
    -------
    str
        A valid Python identifier.
    """
    if not name[0].isalpha() and name[0] != "_":
        name = "_" + name
    if iskeyword(name):
        name = name + "_"
    return name


def clean_comment(comment: str) -> str:
    """
    Sanitise a raw schema.org rdfs:comment for use as a Python docstring.

    Schema.org comments contain MediaWiki markup that is not valid in Python
    source. This function removes or replaces the patterns that would cause
    ``SyntaxWarning`` or ``SyntaxError`` in the generated file, and wraps
    long lines to keep the generated source readable.

    Parameters
    ----------
    comment : str
        The raw comment string from the JSON-LD ``rdfs:comment`` field.

    Returns
    -------
    str
        A clean string safe to embed in a Python docstring, with lines
        wrapped to at most 84 characters (leaving room for 4-space indentation
        to stay within the conventional 88-character line limit).
    """
    # [[ClassName]] -> ClassName  (MediaWiki internal links)
    comment = re.sub(r"\[\[([^\]]+)\]\]", r"\1", comment)
    # Remove backslashes to avoid invalid escape sequence warnings
    comment = comment.replace("\\", "")
    # Replace triple quotes to avoid breaking the docstring delimiter
    comment = comment.replace('"""', "'''")
    # Wrap each paragraph individually, preserving existing newlines
    paragraphs = comment.split("\n")
    wrapped = [textwrap.fill(p, width=84) if p.strip() else "" for p in paragraphs]
    return "\n".join(wrapped)


def schema_ids(node: dict, key: str) -> list[str]:
    """
    Extract safe Python names from a JSON-LD node's @id references.

    Reads the value at ``key`` from ``node``, normalises it to a list,
    filters to schema.org IRIs only, and returns the local name of each
    as a safe Python identifier.

    Parameters
    ----------
    node : dict
        A single entry from the JSON-LD ``@graph``.
    key : str
        The predicate to read, e.g. ``"schema:domainIncludes"``.

    Returns
    -------
    list[str]
        Safe Python identifiers for each referenced schema.org type.
    """
    val = node.get(key, [])
    if isinstance(val, dict):
        val = [val]
    return [
        safe_name(local(item["@id"]))
        for item in val
        if isinstance(item, dict) and item.get("@id", "").startswith("schema:")
    ]


def parse_schema(data: dict) -> tuple[dict[str, ClassDef], dict[str, list[FieldDef]]]:
    """
    Extract classes and properties from schema.org JSON-LD.

    Parameters
    ----------
    data : dict
        Parsed JSON-LD document.

    Returns
    -------
    tuple[dict[str, ClassDef], dict[str, list[FieldDef]]]
        (classes, class_fields)

    Notes
    -----
    - `classes` contains metadata per class
    - `class_fields` maps class → list of property definitions
    """
    graph = data.get("@graph", [])
    classes: dict[str, ClassDef] = {}
    class_fields: dict[str, list[FieldDef]] = defaultdict(list)

    for node in graph:
        node_id = node.get("@id", "")
        if not node_id.startswith("schema:"):
            continue

        schema_name = local(node_id)
        py_name = safe_name(schema_name)

        rdf_types = node.get("@type", [])
        if isinstance(rdf_types, str):
            rdf_types = [rdf_types]

        comment_raw = node.get("rdfs:comment", "")
        if isinstance(comment_raw, dict):
            comment_text = comment_raw.get("@value", "")
        else:
            comment_text = str(comment_raw)
        comment = clean_comment(comment_text)

        if "rdfs:Class" in rdf_types:
            parents = schema_ids(node, "rdfs:subClassOf")
            classes[py_name] = {
                "parents": parents,
                "schema_name": schema_name,
                "comment": comment,
            }

        elif "rdf:Property" in rdf_types:
            owner_classes = schema_ids(node, "schema:domainIncludes")
            allowed_types = schema_ids(node, "schema:rangeIncludes")

            for owner_class in owner_classes:
                class_fields[owner_class].append(
                    {
                        "name": py_name,
                        "schema_name": schema_name,
                        "allowed_types": allowed_types,
                        "comment": comment,
                    }
                )

    class_fields = {
        class_name: fields
        for class_name, fields in class_fields.items()
        if class_name in classes
    }

    return classes, class_fields


def resolve_type(allowed_types: list[str], strict: bool) -> str:
    """
    Translate a list of schema.org allowed type names into a type annotation string.

    Parameters
    ----------
    allowed_types : list[str]
        Local schema.org type names, e.g. ``["Text", "URL"]``.
    strict : bool
        When ``False``, ``str`` is appended as a fallback type.

    Returns
    -------
    str
        A type annotation string for the generated module, e.g.
        ``"str | list[str] | None"``. Non-primitive (schema.org class) types are
        referenced directly by name — see ``SchemaOrgBase.discriminate_typed_fields``
        for how those are then matched to the correct subtype. Falls back to
        ``"Any | None"`` when ``allowed_types`` is empty.
    """
    if not allowed_types:
        # No rangeIncludes declared in schema.org — type is unknown.
        return "Any | None"

    source_names: list[str] = []
    for type_name in allowed_types:
        if type_name in PRIMITIVE_TYPE_MAP:
            source_names.append(PRIMITIVE_SOURCE[PRIMITIVE_TYPE_MAP[type_name]])
        else:
            source_names.append(type_name)

    if not strict and "str" not in source_names:
        source_names.append("str")

    src = " | ".join(source_names)
    return f"{src} | list[{src}] | None"


def build_models(
    classes: dict[str, ClassDef],
    class_fields: dict[str, list[FieldDef]],
    strict: bool,
) -> dict[str, dict]:
    """
    Build metadata for all models to be rendered.

    Parameters
    ----------
    classes : dict[str, ClassDef]
        Parsed class metadata from ``parse_schema``.
    class_fields : dict[str, list[FieldDef]]
        Inverted property index from ``parse_schema``.
    strict : bool

    Returns
    -------
    dict[str, dict]
        Metadata for each class, keyed by Python-safe name.
    """
    return {
        class_name: {
            "parents": class_def["parents"],
            "schema_name": class_def["schema_name"],
            "comment": class_def["comment"],
            "fields": {
                field["name"]: (
                    resolve_type(field["allowed_types"], strict),
                    Field(
                        default=None,
                        alias=field["schema_name"]
                        if field["name"] != field["schema_name"]
                        else None,
                    ),
                )
                for field in class_fields.get(class_name, [])
            },
        }
        for class_name, class_def in classes.items()
    }


def render_field(name: str, type_str: str, field_info: FieldInfo) -> str:
    """Render a single schema.org property field as a source code line."""
    args = ["default=None"]
    if field_info.alias:
        args.append(f'alias="{field_info.alias}"')
    return f"{name}: {type_str} = Field({', '.join(args)})"


def topological_sort(models: dict[str, dict]) -> list[str]:
    """Return model names sorted so every parent appears before its children."""
    visited: set[str] = set()
    order: list[str] = []

    def visit(class_name: str) -> None:
        if class_name in visited or class_name not in models:
            return
        visited.add(class_name)
        for parent in models[class_name]["parents"]:
            visit(parent)
        order.append(class_name)

    for class_name in sorted(models):
        visit(class_name)
    return order


def render_module(models: dict[str, dict], strict: bool) -> str:
    """
    Render all models into a Python module string.

    Parameters
    ----------
    models : dict[str, dict]
        Model metadata as returned by ``build_models``.
    strict : bool
        Recorded in the module header for documentation purposes.

    Returns
    -------
    str
        The full source of the generated Python module.
    """
    order = topological_sort(models)

    lines = [
        '"""',
        "Auto-generated schema.org Pydantic models.",
        "Generated by schemaorg_codegen.py -- do not edit manually.",
        "",
        "Settings",
        "--------",
        f"strict : {strict}",
        '"""',
        "from __future__ import annotations",
        "",
        "import functools",
        "from datetime import date, datetime, time, timedelta",
        "from types import UnionType",
        "from typing import Any, Union, get_args, get_origin",
        "",
        "from pydantic import AnyUrl, BaseModel, ConfigDict, Field, model_validator",
        "",
        "",
        "_SCHEMA_TYPE_REGISTRY: dict[str, type] = {}",
        "",
        "",
        inspect.getsource(SchemaOrgBase),
        "",
        inspect.getsource(validate_strict),
        "",
        inspect.getsource(_is_wrapped),
        "",
        inspect.getsource(_collect_annotated_schema_types),
        "",
        inspect.getsource(_discriminate_value),
        "",
    ]

    for class_name in order:
        cls = models[class_name]

        if not cls["parents"]:
            parent = "SchemaOrgBase"
        else:
            # ensures that class inheritance is respecting subclass-before-superclass
            parent_order = {p: order.index(p) for p in cls["parents"]}
            parent = ", ".join(
                sorted(cls["parents"], key=parent_order.get, reverse=True)
            )
        lines.append(f"class {class_name}({parent}):")

        doc = cls["comment"] or f"schema.org/{class_name}"
        indented_doc = doc.replace("\n", "\n    ")
        lines.append(f'    """{indented_doc}"""')
        lines.append("")

        schema_name = cls["schema_name"]
        lines.append(f'    type: str = Field(default="{schema_name}", alias="@type")')

        for field_name, (type_str, field_info) in cls["fields"].items():
            lines.append(f"    {render_field(field_name, type_str, field_info)}")

        lines.append("")

    return "\n".join(lines)


def generate(
    strict: bool = False,
    out: Path = DEFAULT_OUTPUT_PATH,
) -> None:
    """
    Run the full generation pipeline and write the output module.

    Parameters
    ----------
    strict : bool, optional
        When ``True``, field types are enforced exactly as declared in
        schema.org. When ``False`` (the default), ``str`` is accepted as
        a fallback for any typed field.
    out : Path, optional
        Destination path for the generated Python module.
    """
    print(f"Downloading schema.org from {SCHEMA_URL} ...")
    with urllib.request.urlopen(SCHEMA_URL) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    print("Parsing schema.org vocabulary ...")
    classes, class_fields = parse_schema(data)
    print(f"  Found {len(classes)} classes")

    print(f"Building {len(classes)} model definitions ...")
    models = build_models(classes, class_fields, strict)

    print("Rendering source code...")
    source = render_module(models, strict)

    out.write_text(source, encoding="utf-8")
    print(f"Written to {out} ({len(models)} models)")

    print("Verifying all classes can be instantiated ...")
    verify_script = Path(__file__).parent / "check_schemaorg_models.py"
    subprocess.run([sys.executable, str(verify_script)], check=True)


def main() -> None:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description="Generate static Pydantic models from schema.org"
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Enforce exact schema.org field types (no str fallback).",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help=f"Output file (default: {DEFAULT_OUTPUT_PATH})",
    )
    args = parser.parse_args()
    generate(strict=args.strict, out=args.out)


if __name__ == "__main__":
    main()
