from datetime import UTC, datetime
from pathlib import Path

from metadata_converter.load import load_to_jsonld
from metadata_converter.schema_org_models.schemaorg_models import DigitalDocument


def write_provenance_file(
    about_file_id: str, provenance_path: Path, based_on: str
) -> None:
    """Create a provenance file linking the metadata file to its metadata source."""
    provenance_id = "Provenance_" + about_file_id.split("/")[-1]
    provenance = DigitalDocument(
        id=provenance_id,
        about=about_file_id,
        abstract="Provenance information of " + about_file_id,
        dateCreated=datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        isBasedOn=based_on,
    )
    load_to_jsonld(provenance, provenance_path)
