import re

from pydantic import AnyUrl, Field, HttpUrl, model_validator

from metadata_converter.schema_org_models.schemaorg_models import *

ORCID_EXTRACT_PATTERN = re.compile(
    r"(?:https?://orcid\.org/)?(\d{4}-\d{4}-\d{4}-\d{3}[\dX])"
)


class Orcid(PropertyValue):
    name: str = "Open Researcher and Contributor ID"
    alternateName: str = "ORCID"
    propertyID: AnyUrl = "https://registry.identifiers.org/registry/orcid"

    @model_validator(mode="before")
    @classmethod
    def clean_id(cls, data):
        if "value" in data:
            raw = str(data["value"]).strip()

            match = ORCID_EXTRACT_PATTERN.search(raw)
            if not match:
                raise ValueError(f"Invalid ORCID input: {raw}")

            orcid_id = match.group(1)

            data["value"] = orcid_id
            data["url"] = f"https://orcid.org/{orcid_id}"

        return data


# ---------------------------------------------------------------------------
# Dynamic lookup
# ---------------------------------------------------------------------------


def get_schema(type_name: str) -> type[SchemaOrgBase]:
    """
    Return the Pydantic model class for a schema.org type name.

    Parameters
    ----------
    type_name : str
        Schema.org class name (e.g. "Person").

    Returns
    -------
    type[SchemaOrgBase]

    Raises
    ------
    KeyError
        If the type_name is not available.

    Examples
    --------
    ::

        cls = get_schema("Person")
        instance = cls(**data)
    """
    cls = globals().get(type_name)
    if cls is None or not (isinstance(cls, type) and issubclass(cls, SchemaOrgBase)):
        raise KeyError(
            f"{type_name!r} is not a known schema.org type. Ensure that it is available in schema.org and update the Pydantic models if necessary."
        )
    return cls
