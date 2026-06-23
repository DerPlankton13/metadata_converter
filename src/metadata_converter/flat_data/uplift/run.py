"""Orchestrator for the flat-data uplift stage.

``run_uplift`` is the public entry point: load entities, apply each operation in
order, write entities. Future operations (cast, add, remove) will hook in here
between the load and write phases.
"""

import logging

from metadata_converter.config import FlatDataUpliftConfig
from metadata_converter.flat_data.uplift.enrichment import EnrichmentApplier
from metadata_converter.flat_data.uplift.entity_store import EntityStore
from metadata_converter.flat_data.uplift.link import LinkApplier

logger = logging.getLogger(__name__)


def run_uplift(config: FlatDataUpliftConfig) -> None:
    """Resolve cross-references in ingested flat_data JSON-LD.

    Phases, in order:

    1. **Load** — read every ``*.jsonld`` file from ``config.input_dir`` and
       group entities by ``@type`` into an ``EntityStore``.
    2. **Link** — apply every ``LinkRule`` in ``config.links``.
    3. **Enrich** — apply every ``EnrichmentRule`` in ``config.enrichments``.
    4. **Write** — export every entity to ``config.output_dir``.
    """
    logger.info("Starting flat-data uplift from %s", config.input_dir)
    store = EntityStore.load(config.input_dir)
    LinkApplier(store).apply_all(config.links)
    EnrichmentApplier(store).apply_all(config.enrichments)
    store.write(config.output_dir)
    logger.info("Flat-data uplift complete. Output: %s", config.output_dir)
