# metadata_converter — Implementation Reference

The authoritative description of the internal architecture — package layout,
the schema.org models, the flat-data load pipeline (extract → transform →
schema-build), the uplift stage (link / enrich / add / remove), serialization,
and the `@id`/IRI conventions — lives in the **Architecture** section of
[`CLAUDE.md`](../CLAUDE.md) and is kept in sync with the code. Refer to it for
how the pipeline is structured.

This file covers only the one thing that section deliberately does not spell out
in full: the **project-specific assumptions hardcoded in the biosamples uplift**,
so a fork knows exactly what to change.

---

## Hardcoded project-specifics

This package was intended to be a general-purpose schema.org converter, but the
**biosamples uplift** (`src/metadata_converter/biosamples/uplifting.py`) has
accumulated BIOcean5D-specific assumptions. Forking is the recommended path for
other projects. This section enumerates the coupling.

The `flat_data` and `api` source types, and the generic uplift stage
(`flat_data/uplift/`), are project-agnostic and may be reused as-is. Everything
below lives in `biosamples/uplifting.py`.

### Hardcoded identifiers and URIs

- **`ResearchProject` @id** — `ProductBuilder.build_manufacturer` and
  `BaseBuilder.build_research_project` set the manufacturer/participant to a
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
  used for `Product.keywords` (`ProductBuilder.build_keywords`)
- `"SRA accession"` — used to build the `SRA` identifier when present
- `"project name"` — used by the project-name de-duplication above
- `"sample collection device"`, `"sampling platform"` — used to build
  `Action.instrument` (`ActionBuilder.build_instrument`)
- `"environmental medium"`, `"organism"` — used to build `Action.object`
  (`ActionBuilder.build_object`)
- `"filtration volume"`, `"filtration time"`,
  `"size-fraction lower threshold"`, `"size-fraction upper threshold"` —
  used to build `Action.actionProcess` HowToSteps
  (`ActionBuilder.build_action_process`)
- `"sampling design label"`, `"protocol label"`, `"checklist"`,
  `"ENA-CHECKLIST"` — used for `additionalProperty` enrichment
- `"geographic location (country and/or sea)"`,
  `"geographic location (region and locality)"`,
  `"geographic location (latitude/longitude)"`, `"elevation"`,
  `"broad-scale environmental context"`, `"local environmental context"`,
  `"depth"`, `"depth-max"`, `"depth-min"`, `"collection date"` — used to
  build `Action.location` and `Action.startTime` (`ActionBuilder.build_location`)

These property names come from MIxS and ENA BioSamples checklists, so they are
not entirely arbitrary — but the *selection* of which properties to lift into
which schema.org fields is BIOcean5D-specific.

To generalize: replace the imperative builders with a declarative template
(JSON/TOML) mapping schema.org properties to BioSample property names, similar
to the `mapping` dict already used by `flat_data`.

### Hardcoded MIxS `propertyID` URIs

`ActionBuilder.build_location` injects MIxS-term URIs for selected fields:

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

`ActionBuilder.build_location` embeds a literal description string for the
"sampling design label" `PropertyValue`, including a BIOcean5D-specific FAQ URL
(`https://biocean5d.embl.de/faq.cgi`).

### Builder design

The `BaseBuilder` / `ProductBuilder` / `ActionBuilder` inheritance was chosen
to share helpers but does not express polymorphism (no abstract `build()`
method, `unwrap_single` is a `@staticmethod`). If the BIOcean5D-specific code is
ever lifted out, this inheritance hierarchy should be replaced with plain
functions over a `SampleRecord`.

### What is *not* hardcoded

- The fetch step (`biosamples/fetch.py`) is project-agnostic — it speaks the
  EBI BioSamples API and writes raw `.ldjson` + `.json` files.
- `SampleRecord`, `Term`, `Terminology`, `build_property`,
  `build_defined_term`, `build_thing`, `build_subject_of` are also generic.
  Only the `*Builder.build()` methods carry project-specific assumptions.

So a fork that wants to keep using BioSamples but produce a different schema
shape only needs to replace `ProductBuilder.build()` and `ActionBuilder.build()`
(and their `build_*` helpers).
