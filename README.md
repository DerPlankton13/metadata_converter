# metadata_converter

A CLI tool that converts source-specific metadata into schema.org JSON-LD files,
driven by a TOML config. It supports three ingest workflows and a separate
uplift/linking step.

## Installation

```bash
pip install metadata_converter
```

During development (editable install):

```bash
uv pip install -e .
```

## Usage

```bash
converter <phase> <config.toml>
```

`phase` is one of `fetch`, `ingest`, or `uplift`. The `workflow_type` key in the
config selects the workflow. Add `--log-level debug` for verbose output.

## Workflows

| `workflow_type` | Phases | Description |
|---|---|---|
| `flat_data` | `ingest` | Reads Excel/CSV, maps columns to schema.org types |
| `biosamples` | `fetch`, `ingest` | Fetches and uplifts EBI BioSamples records |
| `metadata_collector` | `fetch`, `ingest` | Queries external APIs (Zenodo, etc.) |
| `uplifting` | `uplift` | Resolves cross-references between ingested JSON-LD files |

## Documentation

- [Usage guide](docs/usage.md) — config syntax and workflow examples
- [Implementation reference](docs/implementation.md) — internal architecture

## Real-world usage

The [paper](https://github.com/DerPlankton13/paper) repository uses this package
to build the BIOcean5D metadata graph. Its `configs/` directory contains
production configs for all supported workflow types.
