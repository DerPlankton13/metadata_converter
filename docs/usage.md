# metadata_converter — Usage Guide

`metadata_converter` is a CLI tool that converts source-specific metadata into
JSON-LD files conforming to schema.org, driven by a TOML config. It supports
three independent load workflows and a separate uplift/linking step.

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

`phase` is one of `fetch`, `load`, or `uplift`. The `source_type` key in the
config identifies the data source; the phase selects the step to execute:

| Phase | Applicable source types | What it does |
|---|---|---|
| `fetch` | `biosamples`, `api` | Download raw records from external APIs |
| `load` | `flat_data`, `biosamples`, `api` | Transform raw/tabular data into schema.org JSON-LD (`loaded_base`) |
| `uplift` | *(uplift config, no source_type)* | Resolve cross-references, enrich, add, and remove across loaded JSON-LD |

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

Reads tabular data from Excel, cleans it, maps columns to schema.org types,
and writes one JSON-LD file per entity.

### Minimal config

```toml
source_type = "flat_data"

output_dir = "data/loaded_base/my_source"

[extractor]
input      = "data/raw/input.xlsx"   # an .xlsx file, or a directory of them
sheet_name = ["author", "dataset"]
header     = 0
skiprows   = [1, 2]          # optional: skip descriptor rows below the header

[cleaning]
strip_header_whitespace = true
strip_cell_whitespace   = true
sentinels_to_na         = false
placeholders_to_na      = false

[mapping.author]
type       = "Person"
id         = "@id"
givenName  = "author:first-name"
familyName = "author:last-name"

[mapping.dataset]
type = "DataCatalog"
id   = "@id"
name = "dataset:title"
```

Every entity gets a content-hash `@id` of the form `<Type>_<hash>.jsonld`
automatically; `id = "@id"` in the mapping is the conventional placeholder for it.

### Mapping syntax

Each sheet has a mapping block under `[mapping.<sheet_name>]`. The `type` key
names the schema.org class; every other key is a property name. Values can be:

| Value form | Meaning |
|---|---|
| `"col-name"` | Look up this column in the current row |
| `"Literal:some text"` | Use the literal string `some text` (prefix stripped), not a column |
| `{ type = "Person", name = "col" }` | Build a nested schema.org object (also writable in dotted form, e.g. `author.type = "Person"`) |
| `[{ type = "PropertyValue", … }]` | Build a list of nested objects (each element must be a typed object) |
| `{ type = "Person", id = { from_sheet = "author", … } }` | A **broadcast `@id` reference** — fill this property with refs to entities from another sheet (see below) |

`Literal:` is for flag values and controlled-vocabulary constants that are not
column lookups — e.g. `name = "Literal:author:is-dataset-author"`.

### Cleaning plugins

Optional Python files that transform the **whole dataset** (a `dict[str,
DataFrame]`, keyed by sheet name) before the built-in cleaning steps. Receiving
the whole dataset lets a plugin read one sheet and write another. Each file
defines a `Plugin` subclass implementing `run`:

```python
# plugins/my_plugin.py
import pandas as pd
from metadata_converter.flat_data.transform.cleaning_plugin import Plugin

class MyPlugin(Plugin):
    def run(self, data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        data["author"]["new_col"] = data["author"]["a"] + data["author"]["b"]
        return data
```

Activate with:
```toml
[cleaning]
plugin_dir  = "plugins"
plugin_name = "my_plugin.py"   # a single name or a list of names
```

### Combining columns

Use `combined_columns` to concatenate several source columns into a new column
before schema building, so the mapping only ever contains plain column
references:

```toml
[combined_columns.author]
name = ["author:first-name", "author:last-name"]

[mapping.author]
type = "Person"
name = "name"   # references the combined column
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

### Broadcast `@id` references (load-time)

Inject typed references from one sheet into another when there is no explicit
join key — the implicit link is that the entities belong to the same source
file. Write it **inline** in the mapping as an `id` whose value is a table with
`from_sheet`:

```toml
[mapping.dataset]
type = "DataCatalog"
id   = "@id"
# Persons with author:is-dataset-author == 1 → DataCatalog.creator
creator = { type = "Person", id = { from_sheet = "author", filter_column = "author:is-dataset-author", filter_value = 1 } }
# All Datasets from the "file" sheet → DataCatalog.dataset
dataset = { type = "Dataset", id = { from_sheet = "file" } }
```

`filter_column` / `filter_value` are optional; omit them to inject all entities
from `from_sheet`. Comparison is normalised: `1` (int), `1.0` (float), and `"1"`
(string) all match; booleans compare as `"true"` / `"false"`.

These inline entries are lifted out of the mapping into the config's
`broadcast_id_refs` before schema building; you can also declare them explicitly:

```toml
[[broadcast_id_refs]]
on_sheet      = "dataset"
property      = "creator"
from_sheet    = "author"
filter_column = "author:is-dataset-author"
filter_value  = 1
```

---

## Source type: `biosamples`

Fetches `.ldjson` and `.json` metadata from EBI BioSamples, fuses them to add
units (the `load` phase), and uplifts each record into a `Product` + `Action`
JSON-LD pair (the `uplift` phase).

```toml
source_type = "biosamples"
fetched_dir = "data/fetched/biosamples"   # where `fetch` writes and `load` reads
output_dir  = "data/loaded_base/biosamples"

[extractor]
input            = "data/raw/biosamples"
sheet_name       = "sample"
sample_id_column = "sample:pid"
```

---

## Source type: `api`

Queries external repositories (currently Zenodo) and fetches JSON-LD records.

```toml
source_type = "api"
fetched_dir = "data/fetched/zenodo"     # where `fetch` writes and `load` reads
output_dir  = "data/loaded_base/zenodo"

[extractor]
api_url             = "https://zenodo.org/api/records"
fetch_strategy      = "export_endpoint"
export_url_template = "https://zenodo.org/records/{record_id}/export/json-ld"

[extractor.query]
field = "communities"
value = "biocean5d"
```

---

## Uplift phase config

Post-processes already-loaded JSON-LD: resolves cross-references, enriches
scalars, adds fixed values, and removes scaffolding. It runs *after* all `load`
workflows have completed. The uplift config has no `source_type`; it is used
exclusively with `converter uplift`.

```toml
[flat_data]
# One directory, or several merged into a single store (e.g. one per loaded
# source). A duplicate @id across directories is an error.
input_dir  = ["data/loaded_base/datahub", "data/loaded_base/biosamples"]
output_dir = "data/uplifted/datahub"

[[flat_data.links]]
on_type         = "Action"
target_property = "agent"
match_value     = "agent.identifier"
in_type         = "Person"
in_property     = "identifier"

[[flat_data.enrichments]]
on_type         = "Person"
target_property = "identifier"
enrich_as       = "Orcid"

[[flat_data.additions]]
on_type         = "DataCatalog"
target_property = "funding"
value.type      = "MonetaryGrant"
value.id        = "https://example.org/grant.jsonld"

[[flat_data.removals]]
on_type         = "Dataset"
target_property = "additionalProperty"
where           = { property = "description", contains = "Helper Property for" }
```

Operations are applied in a fixed order: **link → enrich → add → remove → write**.
A config validator rejects two `link`/`enrich`/`add` rules that target the same
`(on_type, target_property)`; `removals` are exempt (they refine other rules'
output). Output shape throughout collapses to the shortest form: 0 → omitted,
1 → scalar, ≥2 → list.

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

### Enrichment / addition / removal rules

| Rule | Key fields | Effect |
|---|---|---|
| `enrichments` | `on_type`, `target_property`, `enrich_as` | Wrap the scalar value in a custom `PropertyValue` subclass (e.g. `Orcid`, `DOI`); its validators fill out `url`/`name`/`propertyID`. At most one value per entity. |
| `additions` | `on_type`, `target_property`, `value` | Set `target_property` to a fixed constant on every entity. `value` is a literal, or a `node` table (`value.type` + `value.id` + fields) built and validated against the schema.org models, or a list. Overwrites any existing value. |
| `removals` | `on_type`, `target_property`, `where` | Filter items out of a list-valued property. `where` is `{ property = "<dot-selector>", equals = "…" }` or `{ …, contains = "…" }` (exactly one; case-sensitive, string-form). |

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
`select_values(candidate, "identifier")`. This is useful when the load-time stub
carries a different `@id` than the canonical file produced by another workflow —
`ref_id_template` bridges the naming gap.

If any placeholder cannot be resolved for a particular candidate, that candidate
is skipped and a warning is logged. If no candidates survive, the
`target_property` is left untouched.

### Value normalisation

All value comparisons in link rules go through the same normalisation:

- `None` → never matches
- `bool` → `"true"` / `"false"` (so `match_literal = "true"` matches a Python `True`)
- `float` with integer value (e.g. `1.0`) → `"1"` (Pydantic sometimes coerces int→float)
- everything else → `str(v).strip().lower()`

This means `1` (int), `1.0` (float), and `"1"` (string) all compare equal. Booleans
do **not** match integers: `True` → `"true"`, not `"1"`.
