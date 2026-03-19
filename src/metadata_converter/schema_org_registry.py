"""
schema.org → Pydantic models at runtime
========================================
No code generation, no files to maintain.

Usage
-----
    pip install pydantic

    from schemaorg_runtime import SchemaRegistry

    registry = SchemaRegistry()          # downloads schema.org once, caches it
    Person = registry.get("Person")      # returns a Pydantic model class

    # Validate data — unknown fields rejected automatically
    person = Person(name="Ada Lovelace", email="ada@example.com")

    # This raises ValidationError:
    Person(name="Ada", helicopter="red one")
"""

import json
import urllib.request
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from functools import cached_property, reduce
from pathlib import Path
from typing import Any, Optional

from pydantic import AnyUrl, BaseModel, ConfigDict, Field, create_model

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCHEMA_URL = "https://schema.org/version/latest/schemaorg-current-https.jsonld"
SCHEMA_PREFIX = "https://schema.org/"

# Default location for the local JSON-LD cache file.
# Sits next to this source file so it is found regardless of the working
# directory the caller happens to be in.
DEFAULT_CACHE_PATH = Path(__file__).parent / "schemaorg-current.jsonld"

# Maps primitive schema.org type names → Python types.
# Class names like "Person" or "Organization" are not listed here;
# those fall through to the registry in _resolve_type.
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
# Helpers
# ---------------------------------------------------------------------------


def _local(iri: str) -> str:
    """
    Strip the schema.org namespace prefix from a fully-qualified IRI.

    Handles both the full HTTPS prefix (``https://schema.org/``) and the
    compact ``schema:`` prefix used inside the JSON-LD ``@graph``.

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


def _resolve_type(
    allowed_types: list[str], registry: "SchemaRegistry", strict: bool
) -> Any:
    """
    Translate a list of schema.org allowed type names into a single Python type.

    Each name is looked up in ``PRIMITIVE_TYPE_MAP`` first. If it is not a
    primitive type it is assumed to be another schema.org class, and the
    registry is asked to build (or return a cached) Pydantic model for it.

    When multiple types are given they are combined with ``|``. The result is
    always wrapped in ``Optional`` so that every field defaults to ``None``,
    reflecting the fact that schema.org rarely declares minimum cardinality.

    If a class is currently being built (circular reference), the registry
    returns ``None`` as a placeholder. Such entries are filtered out here
    so they never reach the ``|`` union — the field falls back to ``Any``
    if all types resolve to ``None``.

    When ``strict`` is ``False``, ``str`` is appended as a final fallback
    type. This reflects the reality that schema.org publishers often use
    plain strings for typed fields (e.g. ``"1815-12-10"`` instead of a
    ``date`` object), so lenient validation accepts both.

    Parameters
    ----------
    allowed_types : list[str]
        Local schema.org type names, e.g. ``["Text", "URL"]`` or
        ``["Person", "Organization"]``.
    registry : SchemaRegistry
        The active registry, used to resolve schema.org class references
        recursively.
    strict : bool
        When ``True``, only the declared schema.org types are accepted.
        When ``False``, ``str`` is added as an additional allowed type.

    Returns
    -------
    Any
        A Python type annotation such as ``Optional[str]``,
        ``Optional[date | str]``, or ``Optional[Person | str]``.
        Returns ``Any`` when ``allowed_types`` is empty or all resolved
        types are circular-reference placeholders.
    """
    if not allowed_types:
        return Any

    parts = []
    for t in allowed_types:
        if t in PRIMITIVE_TYPE_MAP:
            parts.append(PRIMITIVE_TYPE_MAP[t])
        else:
            # Another schema.org class — get or create it.
            # May return None if the class is currently being built
            # (circular reference); those are filtered out below.
            resolved = registry.get(t)
            if resolved is not None:
                parts.append(resolved)

    if not parts:
        return Any

    # In lenient mode, add str as a fallback unless it is already present.
    # This lets publishers pass plain strings for any typed field.
    if not strict and str not in parts:
        parts.append(str)

    resolved = parts[0] if len(parts) == 1 else reduce(lambda a, b: a | b, parts)
    return Optional[resolved]


# ---------------------------------------------------------------------------
# Base model
# ---------------------------------------------------------------------------


class SchemaOrgBase(BaseModel):
    """
    Base class for all schema.org Pydantic models.

    Every model generated by ``SchemaRegistry`` inherits from this class.
    It defines the three fields common to all JSON-LD nodes: ``context``
    (``@context``), ``type`` (``@type``), and ``id`` (``@id``).

    This class is exported at module level so it can be used directly in
    type annotations::

        from schemaorg_runtime import SchemaOrgBase

        def process(item: SchemaOrgBase) -> None:
            ...

    Notes
    -----
    ``model_config`` is set to ``extra="forbid"`` by default. When
    ``SchemaRegistry`` builds a model class it injects its own
    ``ConfigDict`` via ``create_model(__config__=...)``, which overrides
    this default cleanly without requiring a subclass. See
    ``SchemaRegistry.__init__``.
    """

    model_config = ConfigDict(
        extra="forbid",
        # Allow construction via both Python names (type=) and
        # JSON-LD aliases (@type=) interchangeably.
        populate_by_name=True,
    )

    # Fixed JSON-LD context — always schema.org.
    context: str = Field(
        default="https://schema.org",
        alias="@context",
    )
    # The schema.org class name, e.g. "Person" or "Product".
    type: Optional[str] = Field(default=None, alias="@type")
    # Optional IRI that uniquely identifies this node.
    id: Optional[str] = Field(default=None, alias="@id")


# ---------------------------------------------------------------------------
# Core: SchemaRegistry
# ---------------------------------------------------------------------------


class SchemaRegistry:
    """
    Lazy registry that builds Pydantic model classes from schema.org on demand.

    The schema.org JSON-LD vocabulary is loaded once on first access and kept
    in memory. If a ``cache_path`` is given (or the default path is used), the
    raw JSON-LD is saved to disk after the first download so that subsequent
    runs do not need a network connection. Pass ``force_update=True`` to
    discard the local file and re-download regardless.

    Individual Pydantic classes are constructed with ``pydantic.create_model``
    the first time they are requested and then cached, so repeated calls to
    ``get`` are cheap.

    Parameters
    ----------
    allow_extra_fields : bool, optional
        When ``False`` (the default) the generated models use
        ``extra="forbid"``, which causes Pydantic to raise a
        ``ValidationError`` for any field not declared in the schema.
        Set to ``True`` to silently ignore unknown fields.
    strict : bool, optional
        When ``True``, field types are enforced exactly as declared in the
        schema.org vocabulary — e.g. ``birthDate`` must be a ``date``.
        When ``False`` (the default), a ``str`` fallback is added to every
        field type, reflecting the fact that schema.org publishers often
        use plain strings even for typed fields — e.g. ``"1815-12-10"``
        instead of a ``date`` object.
    schema_url : str, optional
        URL of the schema.org JSON-LD file. Override to pin a specific
        release or to point at a local ``file://`` copy.
    cache_path : Path or str or None, optional
        Path to the local JSON-LD cache file. Defaults to
        ``DEFAULT_CACHE_PATH`` (a file next to this module). Pass
        ``None`` to disable disk caching entirely and always download.
    force_update : bool, optional
        When ``True``, delete the existing cache file (if any) and
        download a fresh copy from ``schema_url``. Defaults to ``False``.

    Examples
    --------
    Basic usage::

        registry = SchemaRegistry()
        Person = registry.get("Person")
        person = Person(name="Ada Lovelace", email="ada@example.com")
        "Person" in registry  # True

    Force a fresh download and update the local cache::

        registry = SchemaRegistry(force_update=True)
    """

    def __init__(
        self,
        allow_extra_fields: bool = False,
        strict: bool = False,
        schema_url: str = SCHEMA_URL,
        cache_path: Optional[Path | str] = DEFAULT_CACHE_PATH,
        force_update: bool = False,
    ):
        self.allow_extra_fields = allow_extra_fields
        self.strict = strict
        self.schema_url = schema_url
        self.cache_path = Path(cache_path) if cache_path is not None else None
        self.force_update = force_update
        self._model_cache: dict[str, type[SchemaOrgBase]] = {}
        self._model_config = ConfigDict(
            extra="ignore" if allow_extra_fields else "forbid",
            populate_by_name=True,
        )

    # ------------------------------------------------------------------
    # Lazy-load the raw schema.org data
    # ------------------------------------------------------------------

    @cached_property
    def _raw(self) -> tuple[dict[str, dict], dict[str, dict]]:
        """
        Load and parse the schema.org JSON-LD, cached in memory after first call.

        The loading strategy is decided once at first access:

        1. If ``force_update`` is ``True`` and a cache file exists, it is
           deleted so the download always proceeds.
        2. If a ``cache_path`` is set and the file already exists, it is read
           from disk — no network request is made.
        3. Otherwise the JSON-LD is downloaded from ``schema_url``. If a
           ``cache_path`` is set the response is then written to disk for
           future runs.

        Returns
        -------
        tuple[dict[str, dict], dict[str, dict]]
            A ``(classes, props)`` pair where each is a dict keyed by the
            local schema.org name. ``classes`` values contain ``"parents"``
            and ``"comment"`` keys; ``props`` values contain ``"owner_classes"``,
            ``"allowed_types"``, and ``"comment"`` keys.
        """
        if self.force_update and self.cache_path and self.cache_path.exists():
            self.cache_path.unlink()
            print(f"Deleted outdated cache at {self.cache_path}")

        if self.cache_path and self.cache_path.exists():
            print(f"Loading schema.org from local cache at {self.cache_path} ...")
            data = json.loads(self.cache_path.read_text(encoding="utf-8"))
        else:
            print(f"Downloading schema.org from {self.schema_url} ...")
            with urllib.request.urlopen(self.schema_url) as resp:
                raw_bytes = resp.read()
            data = json.loads(raw_bytes.decode("utf-8"))
            if self.cache_path:
                self.cache_path.write_bytes(raw_bytes)
                print(f"Cached schema.org at {self.cache_path}")

        return self._parse(data)

    @property
    def _classes(self) -> dict[str, dict]:
        """
        All schema.org ``rdfs:Class`` entries, keyed by local name.

        Returns
        -------
        dict[str, dict]
            Keys are local class names (e.g. ``"Person"``). Values are dicts
            with ``"parents"`` (list of parent class names) and ``"comment"``
            (the rdfs:comment string).
        """
        return self._raw[0]

    @property
    def _props(self) -> dict[str, dict]:
        """
        All schema.org ``rdf:Property`` entries, keyed by local name.

        Returns
        -------
        dict[str, dict]
            Keys are local property names (e.g. ``"name"``). Values are dicts
            with ``"owner_classes"`` (list of class names), ``"allowed_types"``
            (list of permitted value type names), and ``"comment"``.
        """
        return self._raw[1]

    @cached_property
    def _class_fields(self) -> dict[str, list[dict]]:
        """
        Inverted index mapping each class name to its list of properties.

        Built once by iterating all properties and grouping them by their
        ``owner_classes``. This inverted structure means that when building a
        Pydantic model we can look up all fields for a class in O(1) rather
        than scanning every property.

        Returns
        -------
        dict[str, list[dict]]
            Keys are local class names. Values are lists of dicts, each with
            ``"name"`` (property name), ``"allowed_types"`` (list of permitted
            value type names), and ``"comment"`` (rdfs:comment string).
        """
        result: dict[str, list[dict]] = defaultdict(list)
        for prop_name, meta in self._props.items():
            for owner_class in meta["owner_classes"]:
                if owner_class in self._classes:
                    result[owner_class].append(
                        {
                            "name": prop_name,
                            "allowed_types": meta["allowed_types"],
                            "comment": meta["comment"],
                        }
                    )
        return result

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(self, class_name: str) -> type[BaseModel]:
        """
        Return the Pydantic model class for a schema.org type.

        The class is built on first access and cached for all subsequent
        calls. Building a class automatically builds any parent classes it
        inherits from, so the full ancestry chain is always resolved.

        Parameters
        ----------
        class_name : str
            The local schema.org class name, e.g. ``"Person"``,
            ``"Product"``, ``"Event"``. Case-sensitive.

        Returns
        -------
        type[BaseModel]
            A Pydantic v2 model class with all schema.org properties for
            that type declared as ``Optional`` fields.

        Raises
        ------
        KeyError
            If ``class_name`` is not present in the schema.org vocabulary.

        Examples
        --------
        ::

            Person = registry.get("Person")
            person = Person(name="Ada Lovelace")
            person.name  # 'Ada Lovelace'
        """
        if class_name not in self._classes:
            raise KeyError(f"{class_name!r} is not a known schema.org type.")
        return self._build(class_name)

    def __getitem__(self, class_name: str) -> type[BaseModel]:
        """
        Shorthand for ``get()``, enabling dict-style access.

        Parameters
        ----------
        class_name : str
            The local schema.org class name.

        Returns
        -------
        type[BaseModel]
            The corresponding Pydantic model class.

        Examples
        --------
        ::

            Article = registry["Article"]
        """
        return self.get(class_name)

    def __contains__(self, class_name: str) -> bool:
        """
        Return whether ``class_name`` is a known schema.org type.

        Parameters
        ----------
        class_name : str
            The local schema.org class name to look up.

        Returns
        -------
        bool
            ``True`` if the name exists in the schema.org vocabulary.

        Examples
        --------
        ::

            "Person" in registry      # True
            "Helicopter" in registry  # False
        """
        return class_name in self._classes

    # ------------------------------------------------------------------
    # Internal: build a model class (with caching)
    # ------------------------------------------------------------------

    def _build(self, class_name: str) -> type[BaseModel]:
        """
        Construct a Pydantic model for ``class_name`` and cache it.

        A ``None`` placeholder is written to the cache before any recursive
        work begins. This prevents infinite recursion when schema.org types
        reference themselves (e.g. ``Person.knows → Person``): the recursive
        call finds the placeholder and the outer call overwrites it with the
        finished class once construction completes.

        Parameters
        ----------
        class_name : str
            Local schema.org class name. Must already be validated as present
            in ``_classes`` by the caller.

        Returns
        -------
        type[SchemaOrgBase]
            The fully constructed and cached Pydantic model class.
        """
        if class_name in self._model_cache:
            # May still be None if we are in the middle of building this class
            # due to a circular reference (e.g. Person.knows → Person).
            # Returning None here is intentional: _resolve_type filters it out.
            return self._model_cache[class_name]  # type: ignore[return-value]

        # Reserve a slot immediately to handle circular references.
        # Any recursive call for this class will get None back, which
        # _resolve_type treats as "type not yet available" and skips.
        self._model_cache[class_name] = None  # type: ignore[assignment]

        meta = self._classes[class_name]

        # --- Base class(es) ---
        parents = [self._build(p) for p in meta["parents"] if p in self._classes]
        base: type[SchemaOrgBase] = parents[0] if parents else SchemaOrgBase

        # --- Fields ---
        # Pydantic create_model expects: { field_name: (annotation, default) }
        # Every field is Optional with None default — schema.org has no minCount.
        field_defs: dict[str, Any] = {
            f["name"]: (_resolve_type(f["allowed_types"], self, self.strict), None)
            for f in self._class_fields.get(class_name, [])
        }

        # --- Create the model ---
        model = create_model(
            class_name,
            __base__=base,
            __config__=self._model_config,
            __doc__=meta["comment"] or f"schema.org/{class_name}",
            **field_defs,
        )

        self._model_cache[class_name] = model
        return model

    # ------------------------------------------------------------------
    # Internal: parse the JSON-LD @graph
    # ------------------------------------------------------------------

    @staticmethod
    def _parse(data: dict) -> tuple[dict[str, dict], dict[str, dict]]:
        """
        Walk the schema.org JSON-LD ``@graph`` and extract classes and properties.

        Only nodes whose ``@id`` starts with ``"schema:"`` are processed;
        anything from external vocabularies (OWL, RDFS meta-properties, etc.)
        is skipped. For each node the ``@type`` field determines whether it is
        treated as a class (``rdfs:Class``) or a property (``rdf:Property``).

        Parameters
        ----------
        data : dict
            The fully parsed JSON-LD document as returned by ``json.loads``.

        Returns
        -------
        tuple[dict[str, dict], dict[str, dict]]
            A ``(classes, props)`` pair.

            ``classes`` is keyed by local class name; each value is a dict
            with:

            - ``"parents"`` — list of local names of direct superclasses
            - ``"comment"`` — the ``rdfs:comment`` string, or ``""``

            ``props`` is keyed by local property name; each value is a dict
            with:

            - ``"owner_classes"`` — list of local class names from ``domainIncludes``
            - ``"allowed_types"`` — list of permitted value type names from ``rangeIncludes``
            - ``"comment"``       — the ``rdfs:comment`` string, or ``""``
        """
        graph = data.get("@graph", [])
        classes: dict[str, dict] = {}
        props: dict[str, dict] = {}

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
                    # The JSON-LD value may be a single dict or a list of dicts;
                    # normalise to a list so the comprehension always works.
                    if isinstance(val, dict):
                        val = [val]
                    return [_local(v["@id"]) for v in val if isinstance(v, dict)]

                props[name] = {
                    "owner_classes": _ids("schema:domainIncludes"),
                    "allowed_types": _ids("schema:rangeIncludes"),
                    "comment": comment,
                }

        return classes, props
