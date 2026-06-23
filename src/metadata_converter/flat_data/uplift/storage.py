"""Entity load/write for the flat-data uplift stage.

``EntityStore`` is the single owner of loaded entities during an uplift run.
It indexes by ``@type`` so appliers can iterate the relevant subset cheaply,
and it owns the read-from-disk and write-to-disk responsibilities.
"""

import json
import logging
from pathlib import Path

from pydantic import ValidationError

from metadata_converter.load import load_to_jsonld
from metadata_converter.schema_org_models.custom_models import get_schema
from metadata_converter.schema_org_models.schemaorg_models import SchemaOrgBase
from metadata_converter.utils.log_setup import log_validation_error

logger = logging.getLogger(__name__)


class EntityStore:
    """Loads, holds, and writes the ``@type``-indexed entity collection.

    Constructed via ``EntityStore.load(input_dir)``. Appliers mutate the held
    models in place; ``write`` exports the (post-applier) state to disk.
    """

    def __init__(self) -> None:
        # @type name → list of models of that @type
        self.by_type: dict[str, list[SchemaOrgBase]] = {}

    @classmethod
    def load(cls, input_dir: Path) -> "EntityStore":
        """Read every ``*.jsonld`` file in ``input_dir`` and group models by ``@type``.

        Files that fail to load or validate are logged and skipped — the rest of
        the run continues. A warning is logged when the input directory is empty.
        """
        store = cls()
        files = sorted(input_dir.glob("*.jsonld"))
        if not files:
            logger.warning("No JSON-LD files found in %s", input_dir)
        for path in files:
            with path.open() as f:
                data = json.load(f)
            model = load_as_model(data, path.name)
            if model is not None:
                store.by_type.setdefault(model.type, []).append(model)
        total = sum(len(models) for models in store.by_type.values())
        logger.info("Loaded %d entity file(s)", total)
        return store

    def of_type(self, type_name: str) -> list[SchemaOrgBase]:
        """Return the list of entities for a given ``@type`` (empty when none)."""
        return self.by_type.get(type_name, [])

    def write(self, output_dir: Path, drop_types: list[str] | None = None) -> None:
        """Export every entity through ``load_to_jsonld``.

        Entities whose ``@type`` appears in ``drop_types`` are skipped — they
        were loaded only to be available as link candidates (e.g. sample stubs
        that the biosamples uplift owns canonically).
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        dropped = set(drop_types or [])
        written = 0
        skipped = 0
        for entity_type, models in self.by_type.items():
            if entity_type in dropped:
                skipped += len(models)
                continue
            for model in models:
                load_to_jsonld(model, output_dir)
                written += 1
        logger.info(
            "Wrote %d uplifted file(s) to %s (skipped %d via drop_types)",
            written,
            output_dir,
            skipped,
        )


def load_as_model(data: dict, source: str) -> SchemaOrgBase | None:
    """Instantiate the schema.org Pydantic model for one JSON-LD entity.

    Returns ``None`` (with a warning log) when the entity has no scalar ``@type``,
    an unknown ``@type``, or fails Pydantic validation; callers may skip such
    entities cleanly.
    """
    entity_type = data.get("@type")
    if not isinstance(entity_type, str):
        logger.warning("%s: missing or non-scalar @type; skipping", source)
        return None
    try:
        model_cls = get_schema(entity_type)
    except KeyError:
        logger.warning("%s: unknown schema.org @type %r; skipping", source, entity_type)
        return None
    try:
        return model_cls(**data)
    except ValidationError as e:
        logger.warning(
            "%s: Pydantic validation failed for @type %r", source, entity_type
        )
        log_validation_error(e, logger, level="warning")
        return None
