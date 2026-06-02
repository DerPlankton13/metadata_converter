# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Workspace context

This repository is part of a two-repo workspace:

```
projects/
├── CLAUDE.md             # workspace overview & cross-repo conventions (loads automatically)
├── metadata_converter/   # this repo — reusable package, released on PyPI
└── paper/                # consumes this package: BIOcean5D metadata-graph pipeline & paper
```

- `../CLAUDE.md` holds workspace-level conventions and loads automatically — follow it.
- This is a **standalone, reusable package** with its own versioning and release
  cycle. It must **never** depend on the `paper` repo; the dependency direction
  is one-way (`paper` → `metadata_converter`).
- Changes here do not automatically propagate. `paper` pins a version explicitly,
  so keep the public API stable and documented.

## Commands

```bash
# Run all tests
python -m pytest

# Run a single test file
python -m pytest tests/biosamples/test_biosamples.py

# Run a single test by name
python -m pytest tests/biosamples/test_biosamples.py::test_extract_action

# Run a parametrized test for a specific sample
python -m pytest "tests/biosamples/test_biosamples.py::test_extract_action[SAMEA111477556]"

# Lint
ruff check src/

# Run the CLI
converter <config.toml>
```

Dependencies are managed with `uv`. The project uses `hatchling` as the build backend.

## Code Style

Docstrings use NumPy style. Simple functions get a single-line docstring; only use the full NumPy sections (Parameters,
Returns, etc.) when the function is non-trivial.

For Pydantic models, document fields with `Field(description=...)` instead of a class-level NumPy Parameters section —
the fields already express type and default declaratively, so a class docstring should be at most one line.

## Architecture

The tool converts metadata from various sources into JSON-LD files conforming to schema.org. It has three independent
workflows, selected by `workflow_type` in a TOML config:

- **`flat_data`** — reads tabular data (Excel/CSV), cleans it, and maps columns to schema.org types via a `mapping` dict
  in the config. Optional cleaning plugins (Python files in a `plugin_dir`) hook into the cleaning step.
- **`biosamples`** — fetches structured (`.ldjson`) and unstructured (`.json`) metadata from EBI BioSamples, fuses them
  to add units, then optionally "uplifts" the raw records into `Product` + `Action` JSON-LD pairs.
- **`metadata_collector`** — queries external APIs (currently Zenodo) and fetches JSON-LD records via either an export
  endpoint or HTML scraping.

### Schema.org models (`src/metadata_converter/schema_org_models/`)

- **`schemaorg_models.py`** — auto-generated Pydantic models for all schema.org types. Do not edit manually; regenerate
  with `schema_org_model_generator.py`. `SchemaOrgBase` is the root; it defines `@context`, `@type`, `@id`, and
  `additionalProperty` (which all our schema objects may carry).
- **`custom_models.py`** — project-specific `PropertyValue` subclasses (e.g. `Orcid`, `DOI`, `ISSN`, `ISBN`,
  `UrlIdentifier`) with validation logic. Also exposes `get_schema(type_name)` for dynamic type lookup by string name.
- **`schemaorg_models.py` (end)** — `make_strict()` creates a strict variant of any model; `rebuild_all_models()` forces
  Pydantic to resolve all forward references.

### BioSamples uplifting (`src/metadata_converter/biosamples/`)

The uplifting logic in `uplifting.py` is the most complex part:

- **`SampleRecord`** — wraps a raw BioSamples JSON-LD dict, tracks which properties have been consumed (for
  `remaining()`), and exposes helpers: `__getitem__` (value), `as_property` (built `PropertyValue` dict),
  `raw_property` (raw dict), `with_unit`.
- **`build_property`** — lifts a raw property into a `PropertyValue` dict, enriching it with a
  `valueReference: DefinedTerm` when a known terminology term code is found in the value string.
- **`build_thing` / `build_subject_of`** — used exclusively by `_build_object`; produce `Thing` dicts with an optional
  `subjectOf: CreativeWork`. The `subjectOf` is always `@type: "CreativeWork"`. `additionalType` (a list of
  `[url, name, termCode]`) is added only for ENVO terms.
- **`Terminology`** enum — maps term codes (ENVO, NCBI txid, NERC) to URL patterns and `defined_termset` values. Used by
  both `build_property` (→ `DefinedTerm`) and `build_subject_of` (→ `subjectOf`). The `@type` of `subjectOf` is **not**
  determined by `Terminology`.
- **`ProductBuilder` / `ActionBuilder`** — build the `Product` and `Action` dicts. `ActionBuilder._build_object` uses
  `build_thing`; all other property slots use the standard `PropertyValue`/`DefinedTerm` path.
- **`SampleUplifter`** — orchestrates both builders and appends any unconsumed properties to `additionalProperty` on
  both outputs.

### Flat-data workflow (`src/metadata_converter/flat_data/`)

The config's `mapping` dict controls how tabular columns become schema.org properties. Each entry under a sheet name
must have a `type` key (schema.org class name) and then property-to-column mappings:

- **String value** → direct column lookup: `"name": "dataset:title"`
- **Dict value** → nested schema object (must also contain `type`):
  `"author": {"type": "Person", "name": "author:name"}`
- **List of dicts** → list of nested objects: `"creator": [{"type": "Person", ...}]`

When a nested entity has parallel lists of equal length, `build_schema` automatically splits them into one instance per
row.

`preprocess_datahub` is a **hardcoded preprocessing step** for the BIOcean5D datahub input format. It expects exactly
five sheets named `author`, `dataset`, `analysis`, `sample`, and `file`, and wires up cross-sheet relationships (
author → creator, sample → object, file → result) before schema building. Samples are removed from the output because
their JSON-LD is produced by the biosamples workflow instead.

Cleaning plugins implement `CleaningPlugin.run(df) -> df` and are discovered dynamically from a `plugin_dir`. They run
before all other cleaning steps.

### Metadata-collector workflow (`src/metadata_converter/api_fetching/`)

Queries are described with `QueryTerm` (single `field: value`) or `QueryGroup` (AND/OR of terms/groups), serialised to
Elasticsearch query-string syntax for Zenodo/DataCite. Sources that don't support compound queries (SEANOE, Figshare)
raise at runtime if a `QueryGroup` is supplied.

Raw JSON-LD responses are written to `<output_path>/raw/` before Pydantic validation. The validated schema objects are
written to `<output_path>/` directly.

### Serialization

`load_to_jsonld` takes any `SchemaOrgBase` instance, serializes it with `model_dump(by_alias=True, exclude_none=True)`,
prepends `@context`, and writes to `<output_path>/<@id>.jsonld`. The `@id` may contain a path separator, in which case
only the final filename component is used.

### BioSamples test data

Test fixtures live in `tests/biosamples/data/`. For each sample there are three input files (`_original.json`,
`_original.jsonld`, `_with_units.jsonld`) and two expected outputs (`Product_*.jsonld`, `Action_*.jsonld`). The
`_with_units.jsonld` is the fused intermediate used as input to uplifting.