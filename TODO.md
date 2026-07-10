# TODO

Tracking improvements identified during a comprehensive code review. Items are
grouped by whether they are concrete refactoring candidates, or first require a
decision before any action is appropriate.

The reviewed state is documented; doing none of these is a valid choice.

---

## Refactoring candidates

Worth doing when time allows. Each entry names the location, the issue, and a
suggested direction.

### Extract project-specific code into a separate package or the `paper` repo
The biosamples uplift (`src/metadata_converter/biosamples/uplifting.py`,
`ProductBuilder`/`ActionBuilder`) is hardcoded for BIOcean5D — see
`docs/implementation.md#hardcoded-project-specifics` for the full list. Two
viable directions:
- Move `ProductBuilder`/`ActionBuilder` to `paper/src/pipeline/` and have
  `paper` depend on the lower-level biosamples fetching from this package.
- Or make the builders config-driven (a TOML/JSON template that mirrors what
  `flat_data/uplifting.py` already does declaratively).

Both reduce coupling between `metadata_converter` and the paper. Deferred for
now since the only consumer is the paper itself; revisit if a second consumer
appears.

### Unify the two schema-building paradigms
`flat_data` builds Pydantic models incrementally via `instantiate()`
(`flat_data/transform/schema_builder.py`).
`biosamples/uplifting.py` builds plain `dict`s and validates once at the end via
`Product(**product_dict)`. `api_fetching/fetch.py` also returns raw `dict`s
(`Record` is a `BaseModel` but the JSON-LD it loads is never validated until
ingest). Pick one — most likely the incremental-Pydantic style from
`flat_data` — and migrate the others. Combine with the project-specific
extraction above.

### Workflow dispatch should be a registry
`src/metadata_converter/main.py` uses a `match (phase, config)` statement with
one case per `(phase, workflow_type)` combination. Adding a new workflow
requires touching `config.py`, the workflow's `run.py`, *and* `main.py`. A
`{(phase, ConfigType): handler}` registry would localize this so each workflow
owns its dispatch in one place.

### Extract the failure-counting loop pattern
Four near-identical loops in `biosamples/run.py:fetch_biosamples`,
`biosamples/run.py:load_biosamples`, `biosamples/run.py:uplift_biosamples`,
and `api_fetching/run.py:load_api_data`. Each does: tqdm-wrap an iterable,
try/except per item, count failures, raise `RuntimeError` if any failed. A
shared helper (`process_with_failures(items, fn, *, desc, unit, log_prefix)`)
would collapse ~80 lines.

### Replace the magic `"Literal:"` prefix in flat_data mappings
`src/metadata_converter/flat_data/transform/schema_builder.py:50` defines
`LITERAL_PREFIX = "Literal:"`. Mapping values that start with this string are treated as constant
text rather than column lookups. An explicit form like
`{"literal": "some text"}` would be self-documenting and remove the magic.
Backwards-incompatible — would need to update every TOML config.

### Tighten the `float.is_integer()` workaround or fix the underlying union typing
`src/metadata_converter/utils/lookup_key.py:to_lookup_key` (already extracted
out of `flat_data`, as this item originally proposed) contains a workaround for
Pydantic coercing `int 1` to `float 1.0` in certain union fields:
```python
if isinstance(value, float) and value.is_integer():
    return str(int(value))
```
The same workaround is now *also* duplicated in
`src/metadata_converter/uplift/remove.py:_stringify` — so the extraction
fixed the module coupling but not the duplication; there are two copies of
this normalization today instead of one. If the underlying union types in the
schema.org models were tightened or ordered differently, both could go away.
Worth investigating once the schema.org generator is touched again, along with
whether `_stringify` and `to_lookup_key` should simply share one function.

### Inline `parse.py` (24 lines) and `http.py` (9 lines)
Both modules are too small to justify their own files. `parse.py` is only
called from `main.py`; `http.py` exposes a single 4-line helper. Folding them
into their respective callers reduces module count without losing clarity.

### Validate API fetch records as Pydantic models early
`src/metadata_converter/api_fetching/fetch.py:fetch_jsonld` returns a raw
`dict`. Validation happens later in `load_api_data` via `get_schema(...)(...)`.
Validating earlier — at fetch time — would surface malformed responses
immediately rather than during the load pass. Combines naturally with the
"unify schema-building paradigms" item above.

---

## Needs clarification before any action

Each of these requires a decision about intent before refactoring would even
be appropriate.

### Builder inheritance vs. composition in `biosamples/uplifting.py`
`BaseBuilder` shares helpers between `ProductBuilder` and `ActionBuilder`, but
has no abstract `build()` and exercises no polymorphism. `unwrap_single` is
`@staticmethod`. `ProductBuilder.build_manufacturer` near-duplicates
`BaseBuilder.build_research_project`. Question: keep the inheritance and fix
the DRY violation, or convert the builders to plain functions taking a
`SampleRecord`? Largely tangled with the "extract project-specific code"
item — defer until that direction is settled.

### Plugin feature — keep or remove?
`CleaningConfig.load_plugins_from_dir` imports arbitrary Python during config
validation. User-leaning toward removing the feature entirely. If kept: move
plugin loading out of the validator into `ingest_flat_data` so config
validation has no side effects. If removed: drop the `plugin_dir` /
`plugin_name` / `plugins` fields, `CleaningPlugin` ABC, and `load_plugins()`.

### Document the implicit coupling in `extract.py`
`src/metadata_converter/flat_data/extract.py:extract_data` uses
`extractor_cfg.model_dump(exclude={"input"})` and passes the result as kwargs
to `pd.read_excel`. Any field added to `ExcelExtractorConfig` that pandas does
not accept will break extraction at runtime. Either document this constraint
clearly (in the docstring on `ExcelExtractorConfig`) or spell out the
supported pandas kwargs explicitly.

### `load_biosamples` skips the repair/validation that `load` does elsewhere
`load_flat_data` (cleaning plugins) and `load_api_data` (`FIXERS` +
`get_schema(...)(**jsonld)`) both build and validate a Pydantic
`SchemaOrgBase` model at load time — malformed or dirty source data is either
corrected or rejected before it reaches `output_dir`. `load_biosamples`
(`biosamples/run.py`) does neither: it only fuses the structured/unstructured
BioSamples JSON and calls `standardise_context`, then writes the raw fused
`dict` straight to `output_dir` — no schema.org validation, no equivalent of a
"fixer" for known source-specific bugs. The first real construction of typed
models (`Product`/`Action`) only happens at uplift, one stage later than every
other source type. Question: is this gap intentional (uplift already
re-derives the shape it needs, so validating the raw fused dict would be
redundant work), or should `load_biosamples` gain fixers/validation
symmetric with the other two source types? Needs a decision before either
adding validation there or documenting the asymmetry as permanent.

### Define a clear public API
`src/metadata_converter/__init__.py` currently exports only `get_schema`.
The CLI is the primary user-facing surface, so this may be intentional. If the
package is meant to be library-usable (importable from other Python code),
decide what belongs on the public API:
- `load_config`?
- The workflow runners (`ingest_flat_data`, `fetch_api_data`, …)?
- `SchemaOrgBase` and friends?

Either commit to "CLI-only, no public Python API" and document it, or pick a
deliberate set of exports and lock them down with `__all__`.

---

## Deliberate choices (no action)

### Hardcoded retry count and timeout in `biosamples/fetch.py`
Not lifted into `BiosamplesConfig` deliberately, to avoid config bloat. Values
(`range(4)`, `timeout=10`) work in practice and are unlikely to need tuning
per run.

### Field descriptions live in `Field(description=...)`, not class docstrings
See `CLAUDE.md` — for Pydantic models, descriptions belong on fields, with at
most a one-line class docstring.
