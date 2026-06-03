# metadata_converter

A CLI tool that converts source-specific metadata into schema.org JSON-LD files,
driven by a TOML config. It supports three ingest workflows and a separate
uplift/linking step.

## Intent and scope

This package was originally designed as a general-purpose schema.org metadata
converter. In its current state it has grown specific to the
[BIOcean5D](https://biocean5d.embl.de/) project, which is its only consumer
([paper](https://github.com/DerPlankton13/paper)). The `flat_data` and
`metadata_collector` workflows remain mostly project-agnostic; the
**biosamples uplift logic** is the part that carries hardcoded BIOcean5D
assumptions — research-project @ids, funding grant references, ContinuousReporting
sheet/property names, OBI codes, and the shape of the produced
`Product` / `Action` schemas.

For other projects, **forking is currently the cleanest path**. The hardcoded
points are listed in
[docs/implementation.md](docs/implementation.md#hardcoded-project-specifics)
with suggested directions for making them config-driven. The intent is to keep
the door open: a future contributor with the resources could lift the
project-specific logic into a template (similar to how `flat_data` uplift is
already declarative via `LinkRule`), at which point the package would become
genuinely reusable. See [TODO.md](TODO.md) for the planned cleanup notes.

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
