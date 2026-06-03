# metadata_converter — Usage Guide

`metadata_converter` is a CLI tool that converts source-specific metadata into
JSON-LD files conforming to schema.org, driven by a TOML config. It supports
three independent ingest workflows and a separate uplift/linking step.

---

## Installation

```bash
# Editable install during development
uv pip install -e .

# Or from PyPI
pip install metadata_converter
```

## Running the CLI

```bash
converter <phase> <config.toml>
```

`phase` is one of `fetch`, `ingest`, or `uplift`. The `source_type` key in the
config identifies the data source; the phase selects the step to execute:

| Phase | Applicable source types | What it does |
|---|---|---|
| `fetch` | `biosamples`, `api` | Download raw records from external APIs |
| `ingest` | `flat_data`, `biosamples`, `api` | Transform raw/tabular data into schema.org JSON-LD |
| `uplift` | *(uplift config, no source_type)* | Resolve cross-references between ingested JSON-LD files |

Source configs (`flat_data`, `biosamples`, `api`) use `source_type` as their
discriminator. The uplift config has no `source_type` — it is a separate config
kind loaded only when the phase is `uplift`.

Add `--log-level debug` for verbose output.

---

## `@id` and IRIs

In JSON-LD, `@id` is an **IRI** (Internationalized Resource Identifier) — a
globally unique, dereferenceable identifier for an entity in the knowledge graph.
It is not just a filename.

During development, before the files are hosted anywhere, this package uses
local filenames as stand-in IRIs (e.g. `Person_abc123.jsonld`,
`Product_SAMEA001.jsonld`). These are *relative* IRIs that will be resolved
against a base IRI once the data is published. The filename convention is
intentional: as long as the base IRI is stable, every `@id` becomes a valid,
dereferenceable URL without changing the data.

Consequences to keep in mind:

- **Cross-file references** like `{"@type": "Person", "@id": "Person_abc123.jsonld"}`
  are relative IRI references. They resolve correctly when the graph is loaded
  with a consistent base IRI or when all files share the same directory.
- **`ref_id_template`** constructs an `@id` from a domain identifier (e.g.
  `Product_{identifier}.jsonld` → `Product_SAMEA001.jsonld`). The template must
  produce a value that matches the `@id` the canonical file will carry — whether
  that is a relative path now or a full IRI later.
- When the files are eventually hosted, the base IRI must be applied consistently
  across all files and all cross-references so the graph stays internally
  consistent.

---

## Source type: `flat_data`

Reads tabular data (Excel or CSV), cleans it, maps columns to schema.org types,
and writes one JSON-LD file per entity.

### Minimal config

```toml
source_type = "flat_data"

[extractor]
type       = "excel"
file_path  = "data/raw/input.xlsx"
sheet_name = ["author", "dataset"]
header     = 0
skiprows   = [1, 2]          # skip descriptor rows below the header

[cleaning]
strip_header_whitespace = true
strip_cell_whitespace   = true
sentinels_to_na         = false
placeholders_to_na      = false

[output]
ingested = "data/ingested/my_source"

[mapping.author]
type       = "Person"
id         = "@id"
givenName  = "author:first-name"
familyName = "author:last-name"

[mapping.dataset]
type       = "DataCatalog"
id         = "@id"
name       = "dataset:title"
```

### Mapping syntax

Each sheet has a mapping block under `[mapping.<sheet_name>]`. The `type` key
names the schema.org class; every other key is a property name. Values can be:

| Value form | Meaning |
|---|---|
| `"col-name"` | Look up this column in the current row |
| `"Literal:some text"` | Use the literal string `some text` (prefix stripped) |
| `{type = "Person", name = "col"}` | Build a nested schema.org object |
| `[{type = "PropertyValue", …}]` | Build a list of nested objects |

`Literal:` is designed for flag values and controlled vocabulary terms that are
constants, not column lookups — e.g. `name = "Literal:author:is-dataset-author"`.

### Cleaning plugins

Optional Python files that transform a sheet's DataFrame before any other
cleaning step. Each file must define a `CleaningPlugin` subclass with a `run`
method:

```python
# plugins/my_plugin.py
from metadata_converter.flat_data.cleaning_plugin import CleaningPlugin

class MyPlugin(CleaningPlugin):
    def run(self, df):
        df["new_col"] = df["a"] + df["b"]
        return df
```

Activate with:
```toml
[cleaning]
plugin_dir  = "plugins"
plugin_name = "my_plugin.py"
```

### Combining columns

Use `combined_columns` to concatenate several source columns into a new column
before schema building. This is declared separately from the mapping so the
mapping only ever contains plain column references:

```toml
[combined_columns.author]
name = ["author:first-name", "author:last-name"]

[mapping.author]
type   = "Person"
name   = "name"   # references the combined column
```

Source columns are joined with a single space. Multiple target columns per sheet
are supported.

### Splitting multi-value cells

Cells that contain multiple values separated by a delimiter can be exploded into
separate rows before schema building:

```toml
[split_fields]
analysis = ["analysis:author-pid", "analysis:keywords"]
```

### Cross-sheet references (ingest-time)

Inject typed references from one sheet into another when there is no explicit join
key — the implicit link is that the entities belong to the same source file.

```toml
# Persons with author:is-dataset-author == 1 → DataCatalog.creator
[[cross_sheet_refs]]
on_sheet      = "dataset"
property      = "creator"
from_sheet    = "author"
filter_column = "author:is-dataset-author"
filter_value  = 1

# All file Datasets → DataCatalog.dataset
[[cross_sheet_refs]]
on_sheet   = "dataset"
property   = "dataset"
from_sheet = "file"
```

The `@type` of the injected references is taken from
`mapping[from_sheet].type` — no need to repeat it on each rule.

`filter_column` / `filter_value` are optional. When omitted, all entities from
`from_sheet` are injected. Comparison is normalised: `1` (int), `1.0` (float),
and `"1"` (string) all match each other; booleans compare as `"true"` / `"false"`.

---

## Source type: `biosamples`

Fetches `.ldjson` and `.json` metadata from EBI BioSamples, fuses them to add
units, and uplifts each record into a `Product` + `Action` JSON-LD pair.

```toml
source_type = "biosamples"

[input]
input_path  = "data/raw/biosamples/sample_list.xlsx"
sheet_name  = "sample"
header_name = "sample:pid"

[output]
fetched  = "data/fetched/biosamples"
ingested = "data/ingested/biosamples"
```

---

## Source type: `api`

Queries external repositories (currently Zenodo) and fetches JSON-LD records.

```toml
source_type = "api"

[extractor]
api_url              = "https://zenodo.org/api/records"
fetch_strategy       = "export_endpoint"
export_url_template  = "https://zenodo.org/records/{record_id}/export/json-ld"

[extractor.query]
field = "communities"
value = "biocean5d"

[output]
fetched  = "data/fetched/zenodo"
ingested = "data/ingested/zenodo"
```

---

## Uplift phase config

Resolves cross-references in already-ingested JSON-LD. Reads files from each
configured source's output, applies declarative link rules, and writes the
result. This step runs *after* all ingest workflows have completed.

The uplift config has no `source_type` — it is a separate config kind used
exclusively with `converter uplift`.

```toml
[flat_data]
input_path  = "data/ingested/datahub"
output_path = "data/uplifted/datahub"
drop_types  = ["Product"]     # loaded for linking but not written to output

[[flat_data.links]]
on_type         = "Action"
target_property = "agent"
match_value     = "agent.identifier"
in_type         = "Person"
in_property     = "identifier"
```

### Link rule fields

| Field | Required | Description |
|---|---|---|
| `on_type` | yes | `@type` of entities to modify |
| `target_property` | yes | Property name to set on each matched entity |
| `match_value` | one of two | Dot-selector to read from the entity (mutually exclusive with `match_literal`) |
| `match_literal` | one of two | Constant value applied to every entity of `on_type` |
| `in_type` | yes | `@type` of candidates to search |
| `in_property` | one of two | Dot-selector to index candidates by (mutually exclusive with `in_additional_property`) |
| `in_additional_property` | one of two | Name of an `additionalProperty` entry on candidates to index by |
| `ref_id_template` | no | Template for the ref `@id`, e.g. `"Product_{identifier}.jsonld"`. When omitted, the candidate's own `@id` is used. |

### Dot-selectors

Selectors are dot-separated field names walked left-to-right on the model:

- `identifier` — read a single field
- `about.identifier` — descend into a nested object, then read a field
- `agent.identifier` — if `agent` is a list, the selector is applied to each
  element and all results are merged

When the final field holds a `PropertyValue`-like model (one that carries a
`value` attribute — e.g. `Orcid`, `PropertyValue`), the inner value is extracted
automatically. No need to append `.value` to the selector.

### `ref_id_template`

By default the engine links to `{"@type": "X", "@id": <candidate's own @id>}`.
When `ref_id_template` is set, the `@id` is constructed from the candidate's
properties instead:

```toml
ref_id_template = "Product_{identifier}.jsonld"
```

`{identifier}` is replaced with the value returned by
`select_values(candidate, "identifier")`. This is useful when the ingest-time
stub carries a different `@id` than the canonical file produced by another
workflow — `ref_id_template` bridges the naming gap.

If any placeholder cannot be resolved for a particular candidate, that candidate
is skipped and a warning is logged. If no candidates survive, the `target_property`
is left untouched.

### `drop_types`

Types listed under `drop_types` are fully loaded and indexed so link rules can
match against them, but they are **not written** to the output directory. Use this
when stubs for a type are generated at ingest only as reverse-lookup carriers, and
a different workflow (e.g. biosamples) produces the canonical files for those types.

### Value normalisation

All value comparisons in link rules go through the same normalisation:

- `None` → never matches
- `bool` → `"true"` / `"false"` (so `match_literal = "true"` matches a Python `True`)
- `float` with integer value (e.g. `1.0`) → `"1"` (Pydantic sometimes coerces int→float)
- everything else → `str(v).strip().lower()`

This means `1` (int), `1.0` (float), and `"1"` (string) all compare equal. Booleans
do **not** match integers: `True` → `"true"`, not `"1"`.