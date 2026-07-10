"""Orchestrator for the generic, source-independent uplift stage.

``run_uplift`` is the public entry point: load entities, apply each operation in
order, write entities.
"""

import logging

from metadata_converter.uplift.add import AddApplier
from metadata_converter.uplift.config import GenericUpliftConfig
from metadata_converter.uplift.enrichment import EnrichmentApplier
from metadata_converter.uplift.entity_store import EntityStore
from metadata_converter.uplift.link import LinkApplier
from metadata_converter.uplift.remove import RemoveApplier
from metadata_converter.uplift.rename import RenameApplier
from metadata_converter.utils.provenance_writer import write_provenance_file

logger = logging.getLogger(__name__)


def run_uplift(config: GenericUpliftConfig) -> None:
    """Resolve cross-references in loaded JSON-LD.

    Phases, in order:

    1. **Load** — read every ``*.jsonld`` file from ``config.input_dir`` and
       group entities by ``@type`` into an ``EntityStore``.
    2. **Link** — apply every ``LinkRule`` in ``config.links``.
    3. **Enrich** — apply every ``EnrichmentRule`` in ``config.enrichments``.
    4. **Add** — apply every ``AdditionRule`` in ``config.additions``.
    5. **Rename** — apply every ``RenameRule`` in ``config.renames``.
    6. **Remove** — apply every ``RemovalRule`` in ``config.removals`` (scrubs
       linking scaffolding now that links have been resolved).
    7. **Write** — export every entity to ``config.output_dir``.
    """
    logger.info("Starting uplift from %s", config.input_dir)
    store = EntityStore.load(config.input_dir)
    LinkApplier(store).apply_all(config.links)
    EnrichmentApplier(store).apply_all(config.enrichments)
    AddApplier(store).apply_all(config.additions)
    RenameApplier(store).apply_all(config.renames)
    RemoveApplier(store).apply_all(config.removals)
    store.write(config.output_dir)
    if config.provenance_dir is not None:
        for models in store.by_type.values():
            for model in models:
                # uplift refines an entity in place, so it is based on the loaded
                # entity of the same @id; the stage in the filename distinguishes them.
                write_provenance_file(
                    model.id, config.provenance_dir, model.id, "uplift"
                )
    logger.info("Uplift complete. Output: %s", config.output_dir)
