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
    def load(cls, input_dir: Path | list[Path]) -> "EntityStore":
        """Read every ``*.jsonld`` file from one or more directories, grouping by ``@type``.

        A single ``Path`` or a list of them may be given; the latter merges several
        ingested sources into one store. Files that fail to load or validate are
        logged and skipped, and a warning is logged for any empty directory.

        Raises
        ------
        ValueError
            If the same ``@id`` appears in more than one file across the given
            directories. A collision points to a source-setup problem, so the run
            stops rather than silently keeping one entity over another.
        """
        input_dirs = [input_dir] if isinstance(input_dir, Path) else input_dir
        store = cls()
        seen_ids: dict[str, Path] = {}
        for directory in input_dirs:
            files = sorted(directory.glob("*.jsonld"))
            if not files:
                logger.warning("No JSON-LD files found in %s", directory)
            for path in files:
                with path.open() as f:
                    data = json.load(f)
                model = load_as_model(data, path.name)
                if model is None:
                    continue
                if model.id in seen_ids:
                    raise ValueError(
                        f"Duplicate @id {model.id!r} found in {path} and "
                        f"{seen_ids[model.id]}; each entity must have a unique @id "
                        f"across all input directories."
                    )
                seen_ids[model.id] = path
                store.by_type.setdefault(model.type, []).append(model)
        total = sum(len(models) for models in store.by_type.values())
        logger.info("Loaded %d entity file(s)", total)
        return store

    def of_type(self, type_name: str) -> list[SchemaOrgBase]:
        """Return the list of entities for a given ``@type`` (empty when none)."""
        return self.by_type.get(type_name, [])

    def write(self, output_dir: Path) -> None:
        """Export every held entity to ``output_dir`` via ``load_to_jsonld``."""
        output_dir.mkdir(parents=True, exist_ok=True)
        written = 0
        for models in self.by_type.values():
            for model in models:
                load_to_jsonld(model, output_dir)
                written += 1
        logger.info("Wrote %d uplifted file(s) to %s", written, output_dir)


def load_as_model(data: dict, source: str) -> SchemaOrgBase | None:
    """Instantiate the schema.org Pydantic model for one JSON-LD entity.

    Returns ``None`` (with a warning log) when the entity has no scalar ``@type``,
    an unknown ``@type``, no ``@id``, or fails Pydantic validation; callers may skip
    such entities cleanly.
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
        model = model_cls(**data)
    except ValidationError as e:
        logger.warning(
            "%s: Pydantic validation failed for @type %r", source, entity_type
        )
        log_validation_error(e, logger, level="warning")
        return None
    if model.id is None:
        logger.warning("%s: entity has no @id; skipping", source)
        return None
    return model
