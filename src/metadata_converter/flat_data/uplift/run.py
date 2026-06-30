"""Orchestrator for the flat-data uplift stage.

``run_uplift`` is the public entry point: load entities, apply each operation in
order, write entities.
"""

import logging

from metadata_converter.config import FlatDataUpliftConfig
from metadata_converter.flat_data.uplift.add import AddApplier
from metadata_converter.flat_data.uplift.enrichment import EnrichmentApplier
from metadata_converter.flat_data.uplift.entity_store import EntityStore
from metadata_converter.flat_data.uplift.link import LinkApplier
from metadata_converter.flat_data.uplift.remove import RemoveApplier

logger = logging.getLogger(__name__)


def run_uplift(config: FlatDataUpliftConfig) -> None:
    """Resolve cross-references in loaded flat_data JSON-LD.

    Phases, in order:

    1. **Load** — read every ``*.jsonld`` file from ``config.input_dir`` and
       group entities by ``@type`` into an ``EntityStore``.
    2. **Link** — apply every ``LinkRule`` in ``config.links``.
    3. **Enrich** — apply every ``EnrichmentRule`` in ``config.enrichments``.
    4. **Add** — apply every ``AdditionRule`` in ``config.additions``.
    5. **Remove** — apply every ``RemovalRule`` in ``config.removals`` (scrubs
       linking scaffolding now that links have been resolved).
    6. **Write** — export every entity to ``config.output_dir``.
    """
    logger.info("Starting flat-data uplift from %s", config.input_dir)
    store = EntityStore.load(config.input_dir)
    LinkApplier(store).apply_all(config.links)
    EnrichmentApplier(store).apply_all(config.enrichments)
    AddApplier(store).apply_all(config.additions)
    RemoveApplier(store).apply_all(config.removals)
    store.write(config.output_dir)
    logger.info("Flat-data uplift complete. Output: %s", config.output_dir)
