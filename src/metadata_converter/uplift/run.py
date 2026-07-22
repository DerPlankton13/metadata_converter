"""Orchestrator for the generic, source-independent uplift stage.

``run_uplift`` is the public entry point: load entities, apply each operation in
order, write entities.
"""

import logging

from metadata_converter.uplift.add import AddApplier
from metadata_converter.uplift.atomize import AtomizeApplier
from metadata_converter.uplift.config import GenericUpliftConfig
from metadata_converter.uplift.enrichment import EnrichmentApplier
from metadata_converter.uplift.entity_store import EntityStore
from metadata_converter.uplift.link import LinkApplier
from metadata_converter.uplift.remove import RemoveApplier
from metadata_converter.uplift.rename import RenameApplier
from metadata_converter.schema_org_models.schemaorg_models import validate_strict
from metadata_converter.utils.provenance_writer import write_provenance_file

logger = logging.getLogger(__name__)


def run_uplift(config: GenericUpliftConfig) -> None:
    """Resolve cross-references in loaded JSON-LD.

    Phases, in order:

    1. **Load** — read every ``*.jsonld`` file from ``config.input_dir`` and
       group entities by ``@type`` into an ``EntityStore``. If ``config.reference_dirs``
       is set, also load it into a separate, read-only ``EntityStore`` that supplies
       extra link candidates but is never written or included in provenance.
    2. **Link** — apply every ``LinkRule`` in ``config.links``.
    3. **Enrich** — apply every ``EnrichmentRule`` in ``config.enrichments``.
    4. **Add** — apply every ``AdditionRule`` in ``config.additions``.
    5. **Rename** — apply every ``RenameRule`` in ``config.renames``.
    6. **Remove** — apply every ``RemovalRule`` in ``config.removals`` (scrubs
       linking scaffolding now that links have been resolved).
    7. **Atomize** — if ``config.atomize``, extract every blank node into its own
       standalone entity (must run last among the transforms above: it reads each
       entity's full nested content, which earlier stages resolve or remove via
       dot-selectors into that same nested content).
    8. **Write** — export every entity to ``config.output_dir``, then log (never raise) a
       warning for every written entity that still carries content outside the modelled
       schema.org vocabulary, via ``validate_strict``.

    Provenance (if ``config.provenance_dir`` is set) is written only for entities
    present before atomize: an atomized entity has no single loaded entity it is
    "based on", so it gets no provenance file of its own.
    """
    logger.info("Starting uplift from %s", config.input_dir)
    store = EntityStore.load(config.input_dir)
    reference_store = (
        EntityStore.load(config.reference_dirs)
        if config.reference_dirs is not None
        else None
    )
    LinkApplier(store, reference_store).apply_all(config.links)
    EnrichmentApplier(store).apply_all(config.enrichments)
    AddApplier(store).apply_all(config.additions)
    RenameApplier(store).apply_all(config.renames)
    RemoveApplier(store).apply_all(config.removals)
    provenance_ids = [entity.id for entity in store.all_entities()]
    if config.atomize:
        AtomizeApplier(store).apply()
    store.write(config.output_dir)
    for entity in store.all_entities():
        try:
            validate_strict(entity)
        except ValueError as e:
            logger.warning("Strict validation failed for %s: %s", entity.id, e)
    if config.provenance_dir is not None:
        for entity_id in provenance_ids:
            # uplift refines an entity in place, so it is based on the loaded
            # entity of the same @id; the stage in the filename distinguishes them.
            write_provenance_file(entity_id, config.provenance_dir, entity_id, "uplift")
    logger.info("Uplift complete. Output: %s", config.output_dir)
