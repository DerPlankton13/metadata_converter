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
import urllib.request
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from functools import reduce
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
# it verbatim into the generated file, substituting the extra_mode value.


class SchemaOrgBase(BaseModel):
    """
    Base class for all schema.org Pydantic models.

    Provides the three fields common to all JSON-LD nodes and configures
    Pydantic to accept both Python attribute names and JSON-LD @-prefixed
    aliases interchangeably.
    """

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
    )

    # Fixed JSON-LD context -- always schema.org.
    context: str = Field(default="https://schema.org", alias="@context")
    # The schema.org class name. Set automatically by each generated subclass.
    type: str = Field(alias="@type")
    # Optional IRI that uniquely identifies this node.
    id: Optional[str] = Field(default=None, alias="@id")


# Source for the get() lookup function written into the generated module.
_GET_FUNCTION_SOURCE = """\
# ---------------------------------------------------------------------------
# Dynamic lookup
# ---------------------------------------------------------------------------

import sys as _sys


def get(type_name: str) -> type[SchemaOrgBase]:
    \"\"\"
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

        cls = get(config["schema_type"])
        instance = cls(**data)
    \"\"\"
    cls = getattr(_sys.modules[__name__], type_name, None)
    if cls is None or not (isinstance(cls, type) and issubclass(cls, SchemaOrgBase)):
        raise KeyError(f"{type_name!r} is not a known schema.org type in this generated file.")
    return cls
"""
TYPE_TO_SOURCE: dict[Any, str] = {
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
# Step 2: parse classes and properties from the @graph
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


def parse_schema(
    data: dict,
) -> tuple[dict[str, dict], dict[str, dict], dict[str, list[dict]]]:
    """
    Walk the schema.org JSON-LD ``@graph`` and extract classes, properties,
    and a class-to-fields index in a single pass.

    Parameters
    ----------
    data : dict
        The fully parsed JSON-LD document.

    Returns
    -------
    tuple[dict, dict, dict]
        A ``(classes, props, class_fields)`` triple.

        ``classes`` is keyed by local class name; each value contains:

        - ``"parents"``       -- list of local names of direct superclasses
        - ``"comment"``       -- the rdfs:comment string, or ""

        ``props`` is keyed by local property name; each value contains:

        - ``"owner_classes"`` -- list of local class names from domainIncludes
        - ``"allowed_types"`` -- list of value type names from rangeIncludes
        - ``"comment"``       -- the rdfs:comment string, or ""

        ``class_fields`` maps each class name to the list of its property
        dicts, each with ``"name"``, ``"allowed_types"``, and ``"comment"``.
    """
    graph = data.get("@graph", [])
    classes: dict[str, dict] = {}
    props: dict[str, dict] = {}
    class_fields: dict[str, list[dict]] = defaultdict(list)

    for node in graph:
        node_id: str = node.get("@id", "")
        if not node_id.startswith("schema:"):
            continue

        name = _local(node_id)
        types = node.get("@type", [])
        if isinstance(types, str):
            types = [types]

        comment_raw = node.get("rdfs:comment", "")
        comment = (
            comment_raw.get("@value", "")
            if isinstance(comment_raw, dict)
            else str(comment_raw)
        )

        if "rdfs:Class" in types:
            subclass_of = node.get("rdfs:subClassOf", [])
            if isinstance(subclass_of, dict):
                subclass_of = [subclass_of]
            parents = [
                _local(p["@id"])
                for p in subclass_of
                if isinstance(p, dict) and p.get("@id", "").startswith("schema:")
            ]
            classes[name] = {"parents": parents, "comment": comment}

        elif "rdf:Property" in types:

            def _ids(key: str) -> list[str]:
                val = node.get(key, [])
                # Normalise single dict to list so the comprehension always works
                if isinstance(val, dict):
                    val = [val]
                return [_local(v["@id"]) for v in val if isinstance(v, dict)]

            owner_classes = _ids("schema:domainIncludes")
            allowed_types = _ids("schema:rangeIncludes")

            props[name] = {
                "owner_classes": owner_classes,
                "allowed_types": allowed_types,
                "comment": comment,
            }
            for owner_class in owner_classes:
                class_fields[owner_class].append(
                    {
                        "name": name,
                        "allowed_types": allowed_types,
                        "comment": comment,
                    }
                )

    # Remove class_fields entries for classes not in the schema
    class_fields = {k: v for k, v in class_fields.items() if k in classes}

    return classes, props, class_fields


# ---------------------------------------------------------------------------
# Step 3: build Pydantic model classes in memory
# ---------------------------------------------------------------------------


def _resolve_type(
    allowed_types: list[str],
    model_cache: dict[str, type[BaseModel] | None],
    strict: bool,
) -> Any:
    """
    Translate a list of schema.org allowed type names into a Python type.

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
    Any
        A Python type annotation. Returns ``Any`` when ``allowed_types``
        is empty or all resolved types are circular-reference placeholders.
    """
    if not allowed_types:
        return Any

    parts = []
    for t in allowed_types:
        if t in PRIMITIVE_TYPE_MAP:
            parts.append(PRIMITIVE_TYPE_MAP[t])
        else:
            # Another schema.org class. May be None if currently being built
            # due to a circular reference — skip it in that case.
            resolved = model_cache.get(t)
            if resolved is not None:
                parts.append(resolved)

    if not parts:
        return Any

    if not strict and str not in parts:
        parts.append(str)

    return Optional[reduce(lambda a, b: a | b, parts)]


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

        parents = [
            built
            for p in class_def["parents"]
            if p in classes
            for built in [build(p)]
            if built is not None
        ]
        base = parents[0] if parents else SchemaOrgBase

        field_defs: dict[str, Any] = {
            f["name"]: (_resolve_type(f["allowed_types"], cache, strict), None)
            for f in class_fields.get(class_name, [])
        }
        field_defs["type"] = (str, Field(default=class_name, alias="@type"))

        model = create_model(
            class_name,
            __base__=base,
            __doc__=class_def["comment"] or f"schema.org/{class_name}",
            **field_defs,
        )
        cache[class_name] = model
        return model

    for name in classes:
        build(name)

    return {k: v for k, v in cache.items() if v is not None}


# ---------------------------------------------------------------------------
# Step 4: render the generated Python module
# ---------------------------------------------------------------------------


def _type_to_source(tp: Any) -> str:
    """
    Render a Python type object as its source code string.

    Parameters
    ----------
    tp : Any
        A Python type such as ``str``, ``Optional[date | str]``, or a
        Pydantic model class.

    Returns
    -------
    str
        Source code representation, e.g. ``"Optional[date | str]"``.
    """
    from typing import get_args, get_origin

    if tp is type(None):
        return "None"
    if tp in TYPE_TO_SOURCE:
        return TYPE_TO_SOURCE[tp]

    args = get_args(tp)
    if args:
        non_none = [a for a in args if a is not type(None)]
        has_none = len(non_none) < len(args)
        inner = (
            _type_to_source(non_none[0])
            if len(non_none) == 1
            else " | ".join(_type_to_source(a) for a in non_none)
        )
        return f"Optional[{inner}]" if has_none else inner

    if hasattr(tp, "__name__"):
        return tp.__name__

    return repr(tp)


def _render_field(name: str, field_info: FieldInfo, annotation: Any) -> str:
    """
    Render a single field as a source code line.

    Parameters
    ----------
    name : str
        The Python attribute name.
    field_info : FieldInfo
        Pydantic field info carrying alias and default.
    annotation : Any
        The field's type annotation.

    Returns
    -------
    str
        A source line such as ``'birthDate: Optional[date | str] = Field(default=None)'``.
    """
    type_str = _type_to_source(annotation)

    parts = []
    if field_info.default not in (None, ...):
        parts.append(f"default={field_info.default!r}")
    else:
        parts.append("default=None")

    if field_info.alias:
        parts.append(f'alias="{field_info.alias}"')

    return f"{name}: {type_str} = Field({', '.join(parts)})"


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

    def visit(name: str) -> None:
        if name in visited or name not in models:
            return
        visited.add(name)
        for base in models[name].__bases__:
            visit(base.__name__)
        order.append(name)

    for name in sorted(models):
        visit(name)

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
    all_names = set(order)

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
        "from typing import Optional",
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
            (b.__name__ for b in cls.__bases__ if b.__name__ in all_names),
            "SchemaOrgBase",
        )

        lines.append(f"class {class_name}({parent}):")

        doc = (cls.__doc__ or f"schema.org/{class_name}").replace('"""', "'''")
        lines.append(f'    """{doc}"""')
        lines.append("")

        parent_fields = set(cls.__bases__[0].model_fields)
        own_fields = {
            name: cls.model_fields[name]
            for name in cls.model_fields
            if name not in parent_fields
        }

        if not own_fields:
            lines.append("    pass")
        else:
            for field_name, field_info in own_fields.items():
                rendered = _render_field(field_name, field_info, field_info.annotation)
                lines.append(f"    {rendered}")

        lines.append("")

    lines.append("")
    lines.append("# Resolve forward references between models")
    lines.extend(f"{name}.model_rebuild()" for name in order)
    lines.append("")
    lines.append("")
    lines.append(_GET_FUNCTION_SOURCE)

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
    # Step 1: download
    print(f"Downloading schema.org from {SCHEMA_URL} ...")
    with urllib.request.urlopen(SCHEMA_URL) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    # Step 2: parse
    print("Parsing schema.org vocabulary ...")
    classes, props, class_fields = parse_schema(data)
    print(f"  Found {len(classes)} classes and {len(props)} properties")

    # Step 3: build
    print(f"Building {len(classes)} Pydantic model classes ...")
    models = build_models(classes, class_fields, strict)

    # Step 4: render and write
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
