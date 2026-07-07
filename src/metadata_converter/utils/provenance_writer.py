from datetime import UTC, datetime
from pathlib import Path

from metadata_converter.load import load_to_jsonld
from metadata_converter.schema_org_models.schemaorg_models import (
    CreativeWork,
    DigitalDocument,
    Thing,
)


def write_provenance_file(
    about_file_id: str,
    provenance_path: Path,
    based_on: str | list[str],
    stage: str,
) -> None:
    """Write a per-record provenance file linking a metadata file to its source(s).

    ``about`` (the described file) and ``isBasedOn`` (the source(s) it was produced
    from — an ``@id`` for an entity source, a URL for a fetched one) are built as
    node references so they serialize as ``@id`` IRIs rather than literal strings.
    A file merged from several sources (e.g. a biosample fused from two endpoints)
    records all of them; a single source collapses to one node. ``stage`` records
    which pipeline step produced the file and is part of the provenance file's
    name, so one entity's load- and uplift-stage provenance files can coexist in
    a shared folder.
    """
    provenance_id = f"Provenance_{stage}_" + about_file_id.split("/")[-1]
    sources = [based_on] if isinstance(based_on, str) else based_on
    refs = [CreativeWork(id=source) for source in sources]
    provenance = DigitalDocument(
        id=provenance_id,
        about=Thing(id=about_file_id),
        isBasedOn=refs[0] if len(refs) == 1 else refs,
        description=f"stage: {stage}",
        dateCreated=datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )
    load_to_jsonld(provenance, provenance_path)
