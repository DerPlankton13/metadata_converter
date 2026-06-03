# metadata_converter — Implementation Reference

This document covers the internal architecture, key design decisions, and
non-obvious behaviors that are not visible from the public interface.

---

## Package layout

```
src/metadata_converter/
├── config.py                    # Pydantic config models for all workflows
├── main.py                      # CLI entry point — dispatches on (phase, source_type)
├── parse.py                     # CLI argument parsing (phase + config path)
├── load.py                      # load_to_jsonld: serialise SchemaOrgBase → file
├── extract.py                   # Tabular file extraction (Excel, CSV)
├── http.py                      # make_session: shared HTTP session factory
├── io.py                        # write_json: atomic JSON file writer
├── log_setup.py                 # setup_logging + _log_validation_error helper
├── schema_org_models/
│   ├── schemaorg_models.py      # Auto-generated Pydantic models for all schema.org types
│   └── custom_models.py         # Orcid, DOI, ISSN, ISBN, UrlIdentifier + get_schema()
├── flat_data/
│   ├── run.py                   # flat_data ingest + uplift entry points
│   ├── transform.py             # DataFrame cleaning and reshaping
│   ├── schema_builder.py        # Build schema.org Pydantic models from long-format data
│   ├── transform_helpers.py     # split_field helper
│   ├── uplifting.py             # LinkEngine + path evaluation primitives
│   └── cleaning_plugin.py       # CleaningPlugin base class + plugin loader
├── biosamples/
│   ├── run.py                   # fetch_biosamples, ingest_biosamples, uplift_biosamples
│   ├── fetch.py                 # EBI BioSamples API client (parallel fetch)
│   ├── schemas.py               # BioSamples-specific Pydantic schemas
│   └── uplifting.py             # ProductBuilder, ActionBuilder, SampleUplifter
└── api_fetching/
    ├── run.py                   # fetch_api_data, ingest_api_data
    ├── fetch.py                 # query_source + fetch_jsonld (Zenodo, DataCite, etc.)
    └── query_models.py          # QueryTerm, QueryGroup
```

---

## Schema.org models

### Auto-generated models (`schemaorg_models.py`)

All schema.org types are represented as Pydantic models inheriting from
`SchemaOrgBase`. The file is **not edited manually** — regenerate it with
`schema_org_model_generator.py` when the schema changes.

`SchemaOrgBase` defines the fields shared by every entity:

- `@context` — always `{"@vocab": "https://schema.org"}`; included only at
  serialisation time
- `@type` — set as a class-level default on each generated subclass
- `@id` (Python alias `id`) — the entity's identifier, typically a filename like
  `Person_abc123.jsonld`
- `additionalProperty` — optional list of `PropertyValue` instances carrying
  arbitrary metadata that has no dedicated schema.org property

`SchemaOrgBase` sets `validate_assignment=True`. This means that any direct
property assignment (including `setattr`) is immediately validated by Pydantic.
If the assigned value does not match the field's type, a `ValidationError` is
raised at the point of assignment, not at model construction time.

The `LinkEngine` relies on this behaviour: it catches `ValidationError` from
`setattr` to skip a mis-matched assignment rather than crash the run (see
`_apply_rule`).

### Custom identifiers (`custom_models.py`)

`PropertyValue` subclasses with built-in validation:

| Class | Validates |
|---|---|
| `Orcid` | Extracts the `XXXX-XXXX-XXXX-XXXX` pattern from any ORCID input; sets `url` automatically |
| `ISSN` | Checks ISSN format |
| `ISBN` | Checks ISBN-10/13 format |
| `DOI` | Extracts the `10.XXXX/…` pattern |
| `UrlIdentifier` | Validates as a URL |

`get_schema(type_name: str)` looks up a type by its string name across both
`schemaorg_models` and `custom_models`. Used by the `LinkEngine` and the
`flat_data` schema builder.

---

## flat_data ingest pipeline

`ingest_flat_data` in `run.py` orchestrates these steps for every sheet:

1. **Extract** — `extract_data` reads the file and returns a dict of DataFrames,
   one per sheet.
2. **Clean** — `clean_dataframe` applies the configured cleaning steps in order:
   plugins → strip headers → strip cells → replace sentinels → replace
   placeholders → infer dtypes → drop empty rows.
3. **Combine columns** — `combine_columns` scans the mapping for `"col1 + col2"`
   expressions, materialises the concatenated column in the DataFrame, and
   rewrites the mapping entry to the new column name so downstream code sees a
   plain column lookup. Recurses into nested dicts and lists.
4. **Add @id** — `add_id` appends an `@id` column of the form
   `<type>_<hash>.jsonld`. The hash is derived from the row's content (all
   non-null values, keys sorted, serialised to canonical JSON, then SHA-256 →
   first 22 base64url characters = 132 bits). Using a content hash rather than
   a random ID makes `@id` deterministic: the same real-world entity always
   receives the same `@id` regardless of which input file it came from or how
   many times the pipeline runs. This prevents entities that appear in multiple
   input files (e.g. an author shared across several datasets) from being
   written as separate files that the uplifting step would then link as
   spurious duplicates.
5. **Convert to long** — `convert_to_long` melts the wide DataFrame into
   `(id, header, value)` triples so `build_schema` can group them by row.
6. **Split fields** — `split_field` expands rows where one cell holds multiple
   delimited values; produces one row per value.
7. **Build schemas** — `extract_schemas` groups the long DataFrame by `id`, calls
   `build_schema` per group, and returns a list of validated Pydantic models.
8. **Cross-sheet refs** — After all sheets are built, `CrossSheetRef` rules inject
   typed references from one sheet's entities into another's (see *Cross-sheet
   references* below).
9. **Write** — `load_to_jsonld` serialises each model to `<output_path>/<@id>`.

### `build_schema` and `extract_properties`

`build_schema` strips the `type` key from the mapping, then delegates to
`extract_properties` which resolves each mapping entry:

- **String** — looks up the column in the entity dict. Values prefixed with
  `Literal:` are returned verbatim (prefix stripped) without a column lookup,
  even if a column with that name exists.
- **Dict** — recurses into `build_schema` as a nested entity. If all values in
  the nested entity are parallel lists of equal length, it splits them into
  separate instances (e.g. two agents from two parallel name/email columns).
- **List of dicts** — builds one nested instance per dict entry.

### Cross-sheet references

Cross-sheet refs are resolved at ingest time, not uplift time, because the
relationship is implicit: all entities in the source file are in scope, with no
explicit foreign key.

The engine:
1. Collects `@id` values from the source sheet (optionally filtered by a column
   value using the same normalisation as link rules).
2. Wraps each `@id` in a minimal stub of the type taken from `mapping[from_sheet].type`.
3. Calls `setattr` on every entity in the target sheet to inject the references.

---

## Uplift: LinkEngine

The `LinkEngine` in `uplifting.py` resolves cross-references between already-
ingested JSON-LD files. It is project-agnostic: all semantics are encoded in
`FlatDataUpliftConfig.links` (a list of `LinkRule` models).

### Three phases

**Phase 1 — load** (`_load_entities`)  
Reads every `*.jsonld` file in `input_path`. Each file is parsed as a dict and
then validated as its schema.org Pydantic model via `_load_as_model`. Validated
models are grouped into `by_type: dict[str, list[SchemaOrgBase]]`.

Files that fail to load or parse are logged and skipped; the run continues.
A warning is logged if the directory is empty.

**Phase 2 — apply rules** (`_apply_rule`, once per rule)  
For each rule:
1. `_build_candidates_by_value` indexes all candidates of `rule.in_type` by
   their normalised lookup value. This index is built once per rule call.
2. For each entity of `rule.on_type`, `_lookup_values_for` computes the lookup
   value(s): either `[match_literal]` or the result of evaluating `match_value`
   as a dot-selector.
3. `_find_unique_matches` looks up each value in the candidate index and returns
   the matched models, deduplicated by `@id`.
4. References are constructed and assigned via `setattr`. If `ref_id_template` is
   set, each ref's `@id` is rendered from the candidate's properties (see below);
   otherwise the candidate's own `@id` is used.
5. `setattr` triggers Pydantic's `validate_assignment` check immediately. If it
   fails, the assignment is skipped and a warning is logged.

Because all models in `by_type` are the same objects (not copies), modifications
made during phase 2 are visible to later rules in the same run.

**Phase 3 — write** (`_write_entities`)  
Iterates `by_type` and calls `load_to_jsonld` for each model. Types listed in
`config.drop_types` are skipped.

### `drop_types`

Types in `drop_types` are fully loaded and indexed — they can be matched by rules
during phase 2 — but they are not written to the output directory. The canonical
use case: sample stubs produced at datahub ingest carry a reverse-lookup key so
link rules can find them, but the authoritative Product files come from the
biosamples workflow. Listing `"Product"` in `drop_types` keeps the output clean.

### `ref_id_template`

When a link rule sets `ref_id_template`, the ref's `@id` is constructed from the
*matched candidate's* properties rather than taken directly from the candidate's
file. The template is rendered by `_render_ref_id`:

```
"Product_{identifier}.jsonld"  →  "Product_SAMEA001.jsonld"
```

Each `{prop}` placeholder is replaced by `select_values(candidate, prop)[0]`.
If any placeholder cannot be resolved, that candidate is skipped (warning logged).

This bridges the naming gap between stubs (content-hash `@id` from ingest) and
canonical files from another workflow (deterministic `@id` derived from a domain
identifier).

---

## Path evaluation primitives

These functions live in `uplifting.py` and are also used in tests.

### `select_values(obj, selector) -> list`

Walks a dot-separated selector on a Pydantic model (or any object). Always
returns a list because any field along the path may hold a list of models — the
selector fans out across list elements and all results are merged.

```python
select_values(action, "agent.identifier")
# returns ["0000-0001-2345-6789"] even for a single agent
```

The final field is passed through `_unwrap_value` before it reaches the caller,
so callers never need to handle `PropertyValue` wrappers themselves.

`obj` may be a `SchemaOrgBase` model, a list of models, or any scalar. `None`
produces `[]`. Passing a scalar where a model is expected also produces `[]`
(because `getattr` on a non-object returns `None`).

The `selector == ""` guard at the top of the function is technically dead code
(no caller ever passes an empty selector), but it is kept as a safety net.

### `_unwrap_value(value) -> list`

Duck-typed unwrapper. For any value:

- `None` → `[]`
- list → flattened, each element unwrapped recursively
- `BaseModel` with a non-`None` `value` attribute → unwrap that inner value
  (handles `Orcid`, `PropertyValue`, and any future `PropertyValue` subclass)
- anything else → `[value]`

The duck-typing on `BaseModel` is intentional: any model type may appear at the
end of a path (not only `PropertyValue`), and distinguishing "has a meaningful
.value" from "is some other model" is more reliable than checking the exact type.

### `_to_lookup_key(value) -> str | None`

Converts a raw value to a canonical string for comparison. Both the entity-side
value (from `match_value` / `match_literal`) and the candidate-side value (from
`in_property` / `in_additional_property`) pass through this function before being
compared, so the comparison is type-independent.

| Input | Output | Reason |
|---|---|---|
| `None` | `None` | Signals "skip this value" to the caller |
| `True` / `False` | `"true"` / `"false"` | So `match_literal = "true"` matches a bool flag |
| `1.0`, `2.0`, … | `"1"`, `"2"`, … | Pydantic coerces `int 1` to `float 1.0` in some union fields; this collapses them back |
| anything else | `str(v).strip().lower()` | Case- and whitespace-insensitive comparison |

Booleans and integers do **not** match each other: `True` → `"true"` (not `"1"`).

### `_render_ref_id(template, candidate) -> str | None`

Replaces `{prop}` placeholders in `template` with `select_values(candidate, prop)[0]`.
Returns `None` if any placeholder is unresolvable.

### `_find_additional_property(entity, name) -> list`

Scans `entity.additionalProperty` for items with `item.name == name` and returns
their unwrapped values. Used when a link rule specifies `in_additional_property`.

---

## `@id` and IRIs

In JSON-LD, `@id` is an **IRI** (Internationalized Resource Identifier) — a
globally unique, dereferenceable identifier for an entity in the knowledge graph.
It is not a filename; it is the entity's permanent identity in the linked-data
sense.

During development, before the output files are hosted anywhere, this package uses
local filenames as stand-in IRIs (e.g. `Person_abc123.jsonld`,
`Product_SAMEA001.jsonld`). These are *relative* IRI references. When the files
are published under a stable base IRI (e.g.
`https://data.example.org/metadata/`), every `@id` will resolve to a
dereferenceable URL — **without any changes to the data files**, as long as the
base IRI is applied consistently.

Design constraints that follow from this:

- **Cross-file refs** (`{"@type": "Person", "@id": "Person_abc123.jsonld"}`) are
  relative IRI references. They remain internally consistent as long as all files
  share the same base IRI context when loaded into a graph.
- **`ref_id_template`** constructs a ref `@id` from a domain identifier
  (`Product_{identifier}.jsonld` → `Product_SAMEA001.jsonld`). The template must
  produce the same relative IRI that the canonical file carries — so the ref
  resolves to the correct entity in the graph.
- **`SchemaOrgBase.id`** (`@id` in JSON) is typed as `str | None`. No URL
  validation is applied at this stage because the values are relative IRIs during
  development. Validation that they are well-formed absolute IRIs is a
  publish-time concern.

## Serialisation

`load_to_jsonld(schema, output_path)` serialises any `SchemaOrgBase` instance:

1. `model_dump(by_alias=True, exclude_none=True)` produces a plain dict using the
   JSON-LD field aliases (`@id`, `@type`, …) and drops `None` fields.
2. `@context: {"@vocab": "https://schema.org"}` is prepended. This context does
   not set a `@base`; resolving relative `@id` IRIs to absolute ones is left to
   the graph-build step or the eventual hosting layer.
3. The **output filename** is derived from the last path component of `@id`
   (everything after the final `/`). This keeps filenames stable across pipeline
   runs and means the relative `@id` value doubles as the filename — making it
   easy to locate the file for any entity whose `@id` you know.
4. The file is written atomically via `io.write_json`.

---

## Config loading

There are two separate loaders, selected by `parse_cli` based on the CLI phase:

- **`load_source_config(path)`** — used for `fetch` and `ingest` phases. Validates
  against the `SourceConfig` discriminated union
  (`FlatDataConfig | ApiFetchingConfig | BiosamplesConfig`), discriminated by
  `source_type`.
- **`load_uplift_config(path)`** — used for the `uplift` phase. Validates directly
  as `UpliftingConfig`, which has no `source_type` field.

On validation failure the first error is logged with its location and input, and
the process exits with code 1. This means config errors are always visible in the
log, even when running under Snakemake.

---

## Adding a new source type

1. Define a config model in `config.py` with `source_type: Literal["my_source"]`.
2. Add it to the `SourceConfig` union.
3. Write `fetch_my_source` and/or `ingest_my_source` functions.
4. Add the corresponding cases to the `match` block in `main.py`.

---

## Hardcoded project-specifics

This package was intended to be a general-purpose schema.org converter, but the
**biosamples uplift** has accumulated BIOcean5D-specific assumptions. Forking is
the recommended path for other projects. This section enumerates the coupling
so a fork knows what to change.

The `flat_data` and `api` source types are mostly project-agnostic
and may be reused as-is. The points below all live in
`src/metadata_converter/biosamples/uplifting.py`.

### Hardcoded identifiers and URIs

- **`ResearchProject` @id** — `ProductBuilder._build_manufacturer` and
  `BaseBuilder._build_research_project` set the manufacturer/participant to a
  hardcoded BIOcean5D project URL:
  ```
  https://github.com/DerPlankton13/B5D/blob/main/GeneralSchemas/project_b5d.jsonld
  ```
- **`MonetaryGrant` @id** — `ProductBuilder.build` injects a hardcoded
  BIOcean5D grant @id into every `Product`:
  ```
  https://github.com/DerPlankton13/B5D/blob/main/GeneralSchemas/grant_b5d.jsonld
  ```
- **Project-name de-duplication** uses string comparison against `"biocean5d"`
  and `"b5d"` to avoid adding the B5D project a second time when the same name
  appears in the sample metadata.

To generalize: lift these IRIs into config (e.g. a
`BiosamplesUpliftConfig.default_project: ResearchProject | None` and similar
for funding/grants).

### Hardcoded property names

`ProductBuilder` and `ActionBuilder` read specific BIOcean5D / MIxS property
names directly from each `SampleRecord`:

- `"target analysis type"`, `"organism"`, `"local environmental context"` —
  used for `Product.keywords`
- `"SRA accession"` — used to build the `SRA` identifier when present
- `"project name"` — used by the project-name de-duplication above
- `"sample collection device"`, `"sampling platform"` — used to build
  `Action.instrument`
- `"environmental medium"`, `"organism"` — used to build `Action.object`
- `"filtration volume"`, `"filtration time"`,
  `"size-fraction lower threshold"`, `"size-fraction upper threshold"` —
  used to build `Action.actionProcess` HowToSteps
- `"sampling design label"`, `"protocol label"`, `"checklist"`,
  `"ENA-CHECKLIST"` — used for `additionalProperty` enrichment
- `"geographic location (country and/or sea)"`,
  `"geographic location (region and locality)"`,
  `"geographic location (latitude/longitude)"`, `"elevation"`,
  `"broad-scale environmental context"`, `"local environmental context"`,
  `"depth"`, `"depth-max"`, `"depth-min"`, `"collection date"` — used to
  build `Action.location` and `Action.startTime`

These property names come from MIxS and ENA BioSamples checklists, so they are
not entirely arbitrary — but the *selection* of which properties to lift into
which schema.org fields is BIOcean5D-specific.

To generalize: replace the imperative builders with a declarative template
(JSON/TOML) mapping schema.org properties to BioSample property names, similar
to the `mapping` dict already used by `flat_data`.

### Hardcoded MIxS `propertyID` URIs

`ActionBuilder._build_location` injects MIxS-term URIs for selected fields:

```python
{
    "broad-scale environmental context": "https://w3id.org/mixs/0000012",
    "local environmental context":       "https://w3id.org/mixs/0000013",
    "depth":                             "https://w3id.org/mixs/0000018",
    ...
}
```

These are part of the BIOcean5D uplift policy. A general package would either
load these from an external table (MIxS provides one) or take them from config.

### Hardcoded `additionalType` and OBI codes

Every `Product` is tagged `additionalType = ["sample", OBI_0000747]` and every
`Action` is tagged `additionalType = ["sampling process", OBI_0000744]`. These
ontology references are sensible for environmental biosamples but presume that
all input records describe sampling events of physical specimens.

### Hardcoded sampling-design description

`ActionBuilder._build_location` embeds a literal description string for the
"sampling design label" `PropertyValue`, including a BIOcean5D-specific FAQ URL
(`https://biocean5d.embl.de/faq.cgi`).

### Builder design

The `BaseBuilder` / `ProductBuilder` / `ActionBuilder` inheritance was chosen
to share helpers but does not express polymorphism (no abstract `build()`
method, `_unwrap_single` is `@staticmethod`). If the BIOcean5D-specific code is
ever lifted out, this inheritance hierarchy should be replaced with plain
functions over a `SampleRecord`.

### What is *not* hardcoded

- The fetch step (`biosamples/fetch.py`) is project-agnostic — it speaks the
  EBI BioSamples API and writes raw `.ldjson` + `.json` files.
- `SampleRecord`, `Term`, `Terminology`, `build_property`,
  `build_defined_term`, `build_thing`, `build_subject_of` are also generic.
  Only the `*Builder.build()` methods carry project-specific assumptions.

So a fork that wants to keep using BioSamples but produce a different schema
shape only needs to replace `ProductBuilder.build()` and
`ActionBuilder.build()` (and their helpers `_build_*`).