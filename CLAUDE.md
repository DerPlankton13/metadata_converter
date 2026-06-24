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

# Run the CLI (phase is: fetch | ingest | uplift)
converter <phase> <config.toml>
```

Dependencies are managed with `uv`. The project uses `hatchling` as the build backend.

## Workflow

For non-trivial work, follow this rhythm by default (unless the user signals otherwise):

1. **Discuss the larger implementation first.** Agree on the overall design and approach before writing
   any code. Surface trade-offs and alternatives; settle naming and semantics up front.
2. **Break it into smaller pieces.** Sequence the work into independently shippable steps and recommend
   an order. Confirm the order before starting.
3. **Implement each piece test-driven.** For every piece, write the tests **first**, pause for the user to
   review and refine them (names, error messages, assertions), confirm they fail, then implement until
   green. One commit per piece.

For the tests-first step, **begin with a compact test-plan table for approval** —
`name | scenario | key data | assertions` — and list the orthogonal coverage axes (with intentional gaps
called out) *before* writing any test bodies. Revising a table row is cheap; revising eight code blocks is
not. Write the test code only for the approved rows, then proceed to implementation.

Do not jump straight to implementation on a multi-part task.

## Code Style

Docstrings use NumPy style. Simple functions get a single-line docstring; only use the full NumPy sections (Parameters,
Returns, etc.) when the function is non-trivial.

For Pydantic models, document fields with `Field(description=...)` instead of a class-level NumPy Parameters section —
the fields already express type and default declaratively, so a class docstring should be at most one line.

A leading underscore on a function, method, or variable name is a strong cultural signal in Python: "do not import or
reference this from outside the module/class." Plain names are the default — even for module-internal helpers. Reserve
`_name` for symbols where there is an affirmative reason they must not be referenced externally (e.g. framework-wired
callbacks like Pydantic validators, symbols that rely on internal invariants and would mislead callers, or APIs you are
deliberately leaving unstable). "I don't currently import this from elsewhere" is **not** such a reason; ordinary
single-purpose helpers should have plain names regardless of whether they have external callers today.

## Testing

Rules of thumb when writing or refactoring tests:

- **One scenario per test.** A scenario is one combination of inputs producing one outcome. That outcome may have
  several observable parts — assert all of them in the same test. Do not split a single scenario across multiple tests
  just to keep each test to one assertion. Conversely, distinct scenarios (different inputs, different expected
  behavior) belong in separate tests.
- **Tests are read more often than they are written.** Each test serves as documentation of one behavior — when it
  fails months later, the person diagnosing it must understand the scenario from the test alone. This shifts the usual
  DRY calculus: extract setup into fixtures or helpers only when it is noise (identical across tests, irrelevant to
  the scenario). When the setup is the scenario — the specific inputs, configuration, or state that the test is
  exercising — keep it inline, even at the cost of repetition.
- **Each test should be self-contained at a glance.** A reader should see what is set up, what is called, and what is
  checked without jumping to a fixture or helper to reconstruct the scenario. Some repetition between tests is
  acceptable — the cost of duplication is lower than the cost of indirection when reading a failing test.
- **Keep setup, call, and assertions clearly separated.** Within a test, the three phases (build inputs, invoke the
  function under test, check the result) should be visually distinct — typically three small blocks of lines, in that
  order. Avoid mutating shared fixtures mid-test or wrapping the call in a helper that also asserts, since both blur
  which lines define the scenario and which lines check it.
- **Test behavior, not implementation.** Assert on what a caller of the function would see — return values, raised
  exceptions, side effects on inputs — not on how the function produces that result. Testing private helpers is
  acceptable when their logic is intricate enough to warrant isolation, but prefer tests against the public interface
  so internal refactors do not cascade into the test suite.
- **Test names describe the scenario and the expected outcome.** Read as a sentence:
  `test_<what is set up>_<what should happen>`. `test_collect_no_filter_returns_all_ids` says exactly what failed when
  it goes red. `test_collect_works` says nothing.
- **Keep names concise and implementation-free.** Trim words that restate the verb (`test_removal_..._is_removed`),
  filler (`..._of_value`, `..._list_items_...`), and redundant prefixes already implied by the file/scenario. Never
  name a test after *how* it works (`..._treats_as_list`) — name it after the observable scenario
  (`..._matching_single_item`). Shorter is better as long as the sentence still reads true.
- **Parametrize over data, not over logic.** When two tests differ only in inputs and expected outputs, collapse them
  with `@pytest.mark.parametrize`. When two tests differ in what they verify (different scenarios, different
  assertions), keep them separate — parametrize is not a tool for merging distinct behaviors into one function.
- **Every part of the return value should be asserted somewhere.** If a function returns three values and the test
  suite only ever asserts on two, the third is either dead code (and should be removed) or untested (and needs a case
  that checks it). When a function returns an enriched object, assert every populated field so a validator that
  silently stops filling one is caught. When an operation filters or mutates a collection, assert the survivors are
  *intact* (their own fields unchanged), not merely present, so a filter that accidentally rebuilds items is caught.
- **No logic in tests.** No `if`, no loops, no arithmetic in the test body. Hardcode the expected value. If you find
  yourself computing it, you are re-implementing the production code inside the test — which means the test cannot
  catch a wrong implementation that makes the same mistake.
- **Tests must be deterministic.** A test should produce the same result on every run. Replace real wall-clock time,
  random number generators, and network calls with controlled substitutes (frozen clocks, seeded RNGs, recorded or
  stubbed responses). Any uncontrolled dependency on the outside world will eventually cause intermittent failures
  that are expensive to diagnose.
- **Prefer real collaborators over mocks.** Use the actual implementation of internal modules in tests. Mock only at
  true system boundaries — network, clock, filesystem outside `tmp_path`. Mocking internal code locks the test to the
  current implementation, so a harmless refactor turns into a red test suite.
- **Assert on values, not on booleans derived from values.** `assert result.id == "expected"` produces a diff that
  pinpoints the failure. `assert is_valid(result)` only tells you something was wrong somewhere. Build the assertion
  around the specific value you expect to see.
- **A single behavior change should break a small, focused set of tests.** If editing one production function causes
  failures across many unrelated test files, the suite is over-coupled — usually through shared fixtures or helpers
  that try to serve too many scenarios at once. Move setup into each test until every failure points clearly at the
  cause.
- **Scope coverage by orthogonal axes, not the cross product.** Identify the independent axes of a behavior (e.g. a
  match-mode axis vs. an outcome-shape axis). When axes are independent, test each axis once rather than every
  combination — exercising the same downstream code through a different upstream choice is over-testing. Spell out the
  axes when proposing the test plan so the intentional gaps are visible, not accidental.
- **Choose example data deliberately.** Use the same literal for the same role across tests, varying it only when the
  variation *is* the point — consistent literals let a reader spot what actually differs. Keep values domain-faithful
  (respect the real constraints of the type) and use generic placeholders rather than values lifted from real input.
- **Pin informative error messages.** When a test expects a raised error, `match=` on the *specific, diagnostic* part
  of the message — the offending field/property name and the reason — not a generic fragment. This both documents the
  message contract and forces the implementation to keep the message useful.

## Architecture

The tool converts metadata from various sources into JSON-LD files conforming to schema.org. Each source has a
`source_type` in its TOML config, and the CLI phase (`fetch`, `ingest`, `uplift`) selects the step to execute.
There are three source types plus a separate uplift config:

- **`flat_data`** — reads tabular data from Excel, cleans it, and maps columns to schema.org types via a `mapping` dict
  in the config. Optional cleaning plugins (Python files in a `plugin_dir`) hook into the cleaning step.
- **`biosamples`** — fetches structured (`.ldjson`) and unstructured (`.json`) metadata from EBI BioSamples, fuses them
  to add units, then optionally "uplifts" the raw records into `Product` + `Action` JSON-LD pairs.
- **`api`** — queries external APIs (currently Zenodo) and fetches JSON-LD records via either an export
  endpoint or HTML scraping.
- **uplift config** — no `source_type`; used exclusively with `converter uplift` to resolve cross-references between
  ingested JSON-LD files via declarative link rules.

### Schema.org models (`src/metadata_converter/schema_org_models/`)

- **`schemaorg_models.py`** — auto-generated Pydantic models for all schema.org types. Do not edit manually; regenerate
  with `schema_org_model_generator.py`. `SchemaOrgBase` is the root; it defines `@context`, `@type`, `@id`, and
  `additionalProperty` (which all our schema objects may carry).
- **`custom_models.py`** — project-specific `PropertyValue` subclasses (e.g. `Orcid`, `DOI`, `ISSN`, `ISBN`,
  `UrlIdentifier`) with validation logic. Also exposes `get_schema(type_name)` for dynamic type lookup by string name.
- **`schemaorg_models.py` (end)** — `make_strict()` creates a strict variant of any model; `rebuild_all_models()` forces
  Pydantic to resolve all forward references.

### BioSamples uplifting (`src/metadata_converter/biosamples/`)

**Project-specific code lives here.** `ProductBuilder` / `ActionBuilder` in
`uplifting.py` carry hardcoded BIOcean5D assumptions (research-project @ids,
funding grant references, MIxS property selection, OBI codes, ContinuousReporting
property names). The fetch step and the lower-level helpers (`SampleRecord`,
`Term`, `build_property`, `build_thing`, …) are project-agnostic. See
`docs/implementation.md#hardcoded-project-specifics` for the full list and
`TODO.md` for cleanup directions.

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

### `@id` and IRIs

In JSON-LD, `@id` is an **IRI** (Internationalized Resource Identifier) — the entity's globally unique, dereferenceable
identity in the knowledge graph. It is not merely a filename. During development, while the files are not yet hosted,
relative filenames (e.g. `Person_abc123.jsonld`, `Product_SAMEA001.jsonld`) are used as stand-in IRIs. These will
resolve to real URLs once a stable base IRI is established at publish time — **without any changes to the data files**,
as long as the base IRI is applied consistently across all files and cross-references.

Consequence: `ref_id_template` on a `LinkRule` must produce the same relative IRI that the target entity's file carries,
so cross-references remain valid when both are resolved against the same base IRI.

### Serialization

`load_to_jsonld` takes any `SchemaOrgBase` instance, serializes it with `model_dump(by_alias=True, exclude_none=True)`,
prepends `@context` (no `@base` — relative IRI resolution is left to the graph-build step), and writes to
`<output_path>/<@id>.jsonld`. The output filename is the last path component of `@id`; this keeps filenames stable and
means the relative `@id` value doubles as the filename.

### BioSamples test data

Test fixtures live in `tests/biosamples/data/`. For each sample there are three input files (`_original.json`,
`_original.jsonld`, `_with_units.jsonld`) and two expected outputs (`Product_*.jsonld`, `Action_*.jsonld`). The
`_with_units.jsonld` is the fused intermediate used as input to uplifting.