"""
schema.org Pydantic model generator
=====================================
Generates a self-contained Python module of Pydantic v2 models directly
from the schema.org JSON-LD vocabulary.

Run this script once to produce a static ``schemaorg_models.py`` file that
can be imported instantly in any subsequent run with no network access and
no runtime overhead.

Usage
-----
Generate all schema.org types::

    python schemaorg_codegen.py

Enforce exact field types (no str fallback for typed fields)::

    python schemaorg_codegen.py --strict

Write to a custom path::

    python schemaorg_codegen.py --out my_models.py

Then in your application::

    from schemaorg_models import Person, Product

    person = Person(name="Ada Lovelace", email="ada@example.com")
"""

import argparse
import inspect
import json
import re
import textwrap
import urllib.request
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from keyword import iskeyword
from pathlib import Path
from typing import Any, Optional

from pydantic import AnyUrl, BaseModel, ConfigDict, Field, create_model
from pydantic.fields import FieldInfo

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCHEMA_URL = "https://schema.org/version/latest/schemaorg-current-https.jsonld"
SCHEMA_PREFIX = "https://schema.org/"

DEFAULT_OUTPUT_PATH = Path(__file__).parent / "schemaorg_models.py"

# Maps primitive schema.org type names to Python types.
# Class names like "Person" or "Organization" are not listed here —
# those are resolved recursively during model construction.
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

# ---------------------------------------------------------------------------
# Base model
# ---------------------------------------------------------------------------
# Defined as real Python code so there is a single source of truth.
# render_module() extracts the source with inspect.getsource() and writes
# it verbatim into the generated file.


class SchemaOrgBase(BaseModel):
    """
    Base class for all schema.org Pydantic models.

    Provides the two fields common to all JSON-LD nodes and configures
    Pydantic to accept both Python attribute names and JSON-LD @-prefixed
    aliases interchangeably.

    Note: ``@context`` is intentionally omitted here. When serialising a
    top-level JSON-LD document, add it at that point rather than on every
    nested node — including it on nested objects produces invalid JSON-LD.
    """

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
    )

    # The schema.org class name. Set automatically by each generated subclass.
    type: str = Field(alias="@type")
    # Optional IRI that uniquely identifies this node.
    id: Optional[str] = Field(default=None, alias="@id")


def get_schema(type_name: str) -> "type[SchemaOrgBase]":
    """
    Return the Pydantic model class for a schema.org type name.

    Useful when the type name is not known until runtime, e.g. when
    it comes from a config file or user input.

    Parameters
    ----------
    type_name : str
        A schema.org class name, e.g. "Person" or "Product".

    Returns
    -------
    type[SchemaOrgBase]
        The corresponding Pydantic model class.

    Raises
    ------
    KeyError
        If type_name was not included when this file was generated.

    Examples
    --------
    ::

        cls = get_schema(config["schema_type"])
        instance = cls(**data)
    """
    cls = globals().get(type_name)
    if cls is None or not (isinstance(cls, type) and issubclass(cls, SchemaOrgBase)):
        raise KeyError(
            f"{type_name!r} is not a known schema.org type in this generated file."
        )
    return cls


# Maps each primitive Python type to its source-code name for the rendered module.
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


# ---------------------------------------------------------------------------
# Parse classes and properties from the @graph
# ---------------------------------------------------------------------------


def _local(iri: str) -> str:
    """
    Strip the schema.org namespace prefix from a fully-qualified IRI.

    Parameters
    ----------
    iri : str
        A schema.org IRI such as ``"https://schema.org/Person"`` or
        ``"schema:Person"``.

    Returns
    -------
    str
        The local name, e.g. ``"Person"``.
    """
    return iri.removeprefix(SCHEMA_PREFIX).removeprefix("schema:")


def _safe_name(name: str) -> str:
    """
    Return a valid Python identifier for a schema.org local name.

    Applies the minimum change needed to make the name usable as a Python
    class name or attribute:

    - Names starting with a digit get a leading underscore (e.g.
      ``"3DModel"`` → ``"_3DModel"``).
    - Python reserved keywords get a trailing underscore (e.g.
      ``"yield"`` → ``"yield_"``, following PEP 8 convention).
    - Names that are already valid are returned unchanged.

    When the safe name differs from the original, callers should store the
    original as a Pydantic ``alias`` so serialisation uses the correct
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


def _clean_comment(comment: str) -> str:
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


def parse_schema(data: dict) -> tuple[dict[str, dict], dict[str, list[dict]]]:
    """
    Walk the schema.org JSON-LD ``@graph`` and extract classes and a
    class-to-fields index in a single pass.

    Parameters
    ----------
    data : dict
        The fully parsed JSON-LD document.

    Returns
    -------
    tuple[dict, dict]
        A ``(classes, class_fields)`` pair.

        ``classes`` is keyed by the safe Python class name; each value
        contains:

        - ``"parents"``       -- list of safe Python names of direct superclasses
        - ``"schema_name"``   -- original schema.org name (may differ from key)
        - ``"comment"``       -- the rdfs:comment string, or ""

        ``class_fields`` maps each safe class name to the list of its
        property dicts, each with ``"name"``, ``"schema_name"``,
        ``"allowed_types"``, and ``"comment"``.
    """
    graph = data.get("@graph", [])
    classes: dict[str, dict] = {}
    class_fields: dict[str, list[dict]] = defaultdict(list)

    def _ids(node: dict, key: str) -> list[str]:
        val = node.get(key, [])
        # Normalise single dict to list so the comprehension always works
        if isinstance(val, dict):
            val = [val]
        return [
            _safe_name(_local(item["@id"])) for item in val if isinstance(item, dict)
        ]

    for node in graph:
        node_id: str = node.get("@id", "")
        if not node_id.startswith("schema:"):
            continue

        schema_name = _local(node_id)
        py_name = _safe_name(schema_name)
        rdf_types = node.get("@type", [])
        if isinstance(rdf_types, str):
            rdf_types = [rdf_types]

        comment_raw = node.get("rdfs:comment", "")
        comment = _clean_comment(
            comment_raw.get("@value", "")
            if isinstance(comment_raw, dict)
            else str(comment_raw)
        )

        if "rdfs:Class" in rdf_types:
            subclass_of = node.get("rdfs:subClassOf", [])
            if isinstance(subclass_of, dict):
                subclass_of = [subclass_of]
            parents = [
                _safe_name(_local(parent_dict["@id"]))
                for parent_dict in subclass_of
                if isinstance(parent_dict, dict)
                and parent_dict.get("@id", "").startswith("schema:")
            ]
            classes[py_name] = {
                "parents": parents,
                "schema_name": schema_name,
                "comment": comment,
            }

        elif "rdf:Property" in rdf_types:
            owner_classes = _ids(node, "schema:domainIncludes")
            allowed_types = _ids(node, "schema:rangeIncludes")

            for owner_class in owner_classes:
                class_fields[owner_class].append(
                    {
                        "name": py_name,
                        "schema_name": schema_name,
                        "allowed_types": allowed_types,
                        "comment": comment,
                    }
                )

    # Remove class_fields entries for classes not in the schema
    class_fields = {
        class_name: fields
        for class_name, fields in class_fields.items()
        if class_name in classes
    }

    return classes, class_fields


# ---------------------------------------------------------------------------
# Build Pydantic model classes in memory
# ---------------------------------------------------------------------------


def _resolve_type(
    allowed_types: list[str],
    model_cache: dict[str, type[BaseModel] | None],
    strict: bool,
) -> tuple[Any, str]:
    """
    Translate a list of schema.org allowed type names into a Python type and
    its source-code string representation, returned together as a pair.

    Returning both avoids the need to reflect on the constructed type object
    later when rendering the generated module — the string is built here
    directly from the same information used to build the type itself.

    Each property is wrapped as ``Optional[T | list[T]]`` so callers may
    supply either a single value or a list, reflecting the reality that all
    schema.org properties are potentially multi-valued.

    Parameters
    ----------
    allowed_types : list[str]
        Local schema.org type names, e.g. ``["Text", "URL"]``.
    model_cache : dict
        The partially-built model cache. May contain ``None`` placeholders
        for classes currently being built (circular references) — these
        are filtered out.
    strict : bool
        When ``False``, ``str`` is appended as a fallback type, reflecting
        the reality that schema.org publishers often use plain strings for
        typed fields.

    Returns
    -------
    tuple[Any, str]
        ``(python_type, source_string)`` — e.g.
        ``(Optional[date | list[date]], "Optional[date | list[date]]")``.
        Falls back to ``(Optional[Any], "Optional[Any]")`` when
        ``allowed_types`` is empty or all types are circular-ref placeholders.
    """
    fallback = (Optional[Any], "Optional[Any]")

    if not allowed_types:
        return fallback

    python_types: list[Any] = []
    source_names: list[str] = []
    for type_name in allowed_types:
        if type_name in PRIMITIVE_TYPE_MAP:
            tp = PRIMITIVE_TYPE_MAP[type_name]
            python_types.append(tp)
            source_names.append(PRIMITIVE_SOURCE[tp])
        else:
            # Another schema.org class. May be None if currently being built
            # due to a circular reference — skip it in that case.
            resolved = model_cache.get(type_name)
            if resolved is not None:
                python_types.append(resolved)
                source_names.append(type_name)

    if not python_types:
        return fallback

    if not strict and str not in python_types:
        python_types.append(str)
        source_names.append("str")

    # Build T1 | T2 | ... then produce Optional[T1 | T2 | ... | list[T1 | T2 | ...]]
    # so every property accepts either a single value or a list of values.
    scalar_union = python_types[0]
    for t in python_types[1:]:
        scalar_union |= t

    scalar_source = " | ".join(source_names)
    full_source = f"Optional[{scalar_source} | list[{scalar_source}]]"

    return Optional[scalar_union | list[scalar_union]], full_source


def build_models(
    classes: dict[str, dict],
    class_fields: dict[str, list[dict]],
    strict: bool,
) -> dict[str, type[BaseModel]]:
    """
    Construct all Pydantic model classes in memory, respecting inheritance.

    Uses a ``None`` placeholder in the cache to handle circular references —
    any class currently being built returns ``None`` when referenced as a
    field type, which ``_resolve_type`` treats as "not yet available" and
    skips.

    Parameters
    ----------
    classes : dict[str, dict]
        Parsed class metadata from ``parse_schema``.
    class_fields : dict[str, list[dict]]
        Inverted property index from ``parse_schema``.
    strict : bool
        Passed through to ``_resolve_type``.

    Returns
    -------
    dict[str, type[BaseModel]]
        All successfully built Pydantic model classes, keyed by name.
    """
    cache: dict[str, type[BaseModel] | None] = {}

    def build(class_name: str) -> type[BaseModel] | None:
        if class_name in cache:
            return cache[class_name]

        # Placeholder to break circular references
        cache[class_name] = None

        class_def = classes[class_name]

        parents = []
        for parent_name in class_def["parents"]:
            if parent_name in classes:
                parent_model = build(parent_name)
                if parent_model is not None:
                    parents.append(parent_model)
        base = parents[0] if parents else SchemaOrgBase

        field_defs: dict[str, Any] = {}
        for field in class_fields.get(class_name, []):
            py_type, source = _resolve_type(field["allowed_types"], cache, strict)
            schema_name = field["schema_name"]
            # If the property was renamed, add an alias so Pydantic uses
            # the original schema.org name for serialisation.
            alias = schema_name if field["name"] != schema_name else None
            field_defs[field["name"]] = (
                py_type,
                Field(default=None, alias=alias, json_schema_extra={"source": source}),
            )

        # The type field default is always the original schema.org class name.
        field_defs["type"] = (
            str,
            Field(default=class_def["schema_name"], alias="@type"),
        )

        model = create_model(
            class_name,
            __base__=base,
            __doc__=class_def["comment"] or f"schema.org/{class_name}",
            **field_defs,
        )
        cache[class_name] = model
        return model

    for class_name in classes:
        build(class_name)

    return {
        class_name: model for class_name, model in cache.items() if model is not None
    }


# ---------------------------------------------------------------------------
# Render the generated Python module
# ---------------------------------------------------------------------------


def _render_field(name: str, field_info: FieldInfo) -> str:
    """
    Render a single schema.org property field as a source code line.

    The type annotation string was computed by ``_resolve_type`` at build
    time and stored in ``field_info.json_schema_extra["source"]``, so no
    type reflection is needed here.

    Parameters
    ----------
    name : str
        The Python attribute name.
    field_info : FieldInfo
        Pydantic field info carrying alias, default, and the pre-rendered
        type annotation string in ``json_schema_extra["source"]``.

    Returns
    -------
    str
        A source line such as
        ``'birthDate: Optional[date | list[date]] = Field(default=None)'``.
    """
    source = field_info.json_schema_extra["source"]

    field_args = ["default=None"]
    if field_info.alias:
        field_args.append(f'alias="{field_info.alias}"')

    return f"{name}: {source} = Field({', '.join(field_args)})"


def _topological_sort(models: dict[str, type[BaseModel]]) -> list[str]:
    """
    Return model names sorted so every parent appears before its children.

    Parameters
    ----------
    models : dict[str, type[BaseModel]]
        Built model classes to sort.

    Returns
    -------
    list[str]
        Names in dependency order.
    """
    visited: set[str] = set()
    order: list[str] = []

    def visit(class_name: str) -> None:
        if class_name in visited or class_name not in models:
            return
        visited.add(class_name)
        for base_cls in models[class_name].__bases__:
            visit(base_cls.__name__)
        order.append(class_name)

    for class_name in sorted(models):
        visit(class_name)

    return order


def render_module(
    models: dict[str, type[BaseModel]],
    strict: bool,
) -> str:
    """
    Render all model classes as a complete Python module source string.

    Parameters
    ----------
    models : dict[str, type[BaseModel]]
        Built Pydantic model classes to render.
    strict : bool
        Recorded in the module header for documentation purposes.

    Returns
    -------
    str
        The full source of the generated Python module.
    """
    order = _topological_sort(models)
    generated_class_names = set(order)

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
        "from datetime import date, datetime, time, timedelta",
        "from typing import Any, Optional",
        "",
        "from pydantic import AnyUrl, BaseModel, ConfigDict, Field",
        "",
        "",
        inspect.getsource(SchemaOrgBase),
        "",
    ]

    for class_name in order:
        cls = models[class_name]

        parent = next(
            (
                base_cls.__name__
                for base_cls in cls.__bases__
                if base_cls.__name__ in generated_class_names
            ),
            "SchemaOrgBase",
        )

        lines.append(f"class {class_name}({parent}):")

        doc = cls.__doc__ or f"schema.org/{class_name}"
        # Indent continuation lines so the docstring is valid Python
        indented_doc = doc.replace("\n", "\n    ")
        lines.append(f'    """{indented_doc}"""')
        lines.append("")

        parent_cls = cls.__bases__[0]
        own_fields = {
            name: cls.model_fields[name]
            for name in cls.model_fields
            if name not in parent_cls.model_fields
        }

        # Always emit type explicitly — we know the correct schema.org name
        # (which may differ from the Python class name for renamed classes
        # like _3DModel) and every class must declare its own @type default.
        schema_name = cls.model_fields["type"].default
        lines.append(f'    type: str = Field(default="{schema_name}", alias="@type")')

        for field_name, field_info in own_fields.items():
            rendered = _render_field(field_name, field_info)
            lines.append(f"    {rendered}")

        lines.append("")

    lines.append("")
    lines.append("# Resolve forward references between models")
    lines.extend(f"{class_name}.model_rebuild()" for class_name in order)
    lines.append("")
    lines.append("")
    lines.append(
        "# ---------------------------------------------------------------------------"
    )
    lines.append("# Dynamic lookup")
    lines.append(
        "# ---------------------------------------------------------------------------"
    )
    lines.append("")
    lines.append(inspect.getsource(get_schema))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


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
    # Download
    print(f"Downloading schema.org from {SCHEMA_URL} ...")
    with urllib.request.urlopen(SCHEMA_URL) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    # Parse
    print("Parsing schema.org vocabulary ...")
    classes, class_fields = parse_schema(data)
    print(f"  Found {len(classes)} classes")

    # Build
    print(f"Building {len(classes)} Pydantic model classes ...")
    models = build_models(classes, class_fields, strict)

    # Render and write
    print("Rendering source ...")
    source = render_module(models, strict)
    out.write_text(source, encoding="utf-8")
    size_kb = out.stat().st_size // 1024
    print(f"Written to {out}  ({size_kb} KB, {len(models)} classes)")
    print()
    print("Import in your application:")
    print(f"    from {out.stem} import Person, Product  # etc.")


def main() -> None:
    """Parse command-line arguments and run the generator."""
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

    generate(
        strict=args.strict,
        out=args.out,
    )


if __name__ == "__main__":
    main()
