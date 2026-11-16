from datetime import UTC, datetime
from pathlib import Path

from metadata_converter.load import load_to_jsonld
from metadata_converter.schema_org_models.schemaorg_models import (
    CreativeWork,
    DigitalDocument,
    Thing,
)
from metadata_converter.utils.jsonld import SCHEMA_ORG_DEFAULT_CONTEXT


def write_provenance_file(
    about_file_id: str,
    provenance_path: Path,
    based_on: str | list[str],
    stage: str,
) -> None:
    """Write a provenance file linking an entity to the source(s) it was produced from.

    ``about`` and ``isBasedOn`` are built as node references, so they serialize as
    ``@id`` IRIs rather than literal strings. An entity produced from several sources
    (e.g. a biosample fused from two endpoints, or a golden node merged from its
    cluster) records all of them; a single source collapses to one node. The stage is
    part of the provenance file's name, so the provenance files of one entity from
    different stages can coexist in a shared folder.

    Parameters
    ----------
    about_file_id : str
        ``@id`` of the entity or file the provenance describes. Its last ``/``
        segment is used in the provenance file's name.
    provenance_path : Path
        Directory the provenance file is written to.
    based_on : str or list of str
        ``@id`` (entity source) or URL (fetched source) of what the entity was produced
        from; one value or several.
    stage : str
        Pipeline step that produced the entity (e.g. ``"load"``, ``"uplift"``). It is
        stored in the description and in the file's name.
    """
    provenance_id = f"Provenance_{stage}_" + about_file_id.split("/")[-1]
    sources = [based_on] if isinstance(based_on, str) else based_on
    refs = [CreativeWork(id=source) for source in sources]
    provenance = DigitalDocument(
        context=SCHEMA_ORG_DEFAULT_CONTEXT,
        id=provenance_id,
        about=Thing(id=about_file_id),
        isBasedOn=refs[0] if len(refs) == 1 else refs,
        description=f"stage: {stage}",
        dateCreated=datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )
    load_to_jsonld(provenance, provenance_path)
