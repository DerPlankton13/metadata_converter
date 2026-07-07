import re

from pydantic import (
    computed_field,
    field_validator,
    model_validator,
)

from metadata_converter.schema_org_models.schemaorg_models import *

ORCID_EXTRACT_PATTERN = re.compile(
    r"(?:https?://orcid\.org/)?(\d{4}-\d{4}-\d{4}-\d{3}[\dX])"
)
ISSN_PATTERN = re.compile(r"^(ISSN)?[ :]?\d{4}[ -]\d{3}[\dX]$")
ISBN_PATTERN = re.compile(
    r"^(ISBN)?(-13|-10)?[ :]?(\d{2,3}[ -]?)?\d{1,5}[ -]?\d{1,7}[ -]?\d{1,6}[ -]?(\d|X)$"
)
DOI_PATTERN = re.compile(r"10\.\d+/\S+")


def check_pattern(value: str, pattern: re.Pattern[str], type: str) -> str:
    if not pattern.fullmatch(value):
        raise ValueError(f"Invalid {type}: {value}")
    return value


def search_pattern(value: str, pattern: re.Pattern[str], type: str) -> str:
    match = pattern.search(value)
    if not match:
        raise ValueError(f"Invalid {type}: {value}")
    return match.group(0)


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


class ISSN(PropertyValue):
    name: str = "International Standard Serial Number"
    alternateName: str = "ISSN"
    propertyID: AnyUrl = "https://registry.identifiers.org/registry/issn"

    @model_validator(mode="before")
    @classmethod
    def clean_issn(cls, data):
        if "value" in data:
            issn = check_pattern(str(data["value"]), ISSN_PATTERN, "ISSN")
            data["url"] = f"https://portal.issn.org/resource/ISSN/{issn}"
        return data


class ISBN(PropertyValue):
    name: str = "International Standard Book Number"
    alternateName: str = "ISBN"
    propertyID: AnyUrl = "https://registry.identifiers.org/registry/isbn"

    @model_validator(mode="before")
    @classmethod
    def clean_isbn(cls, data):
        if "value" in data:
            isbn = check_pattern(str(data["value"]), ISBN_PATTERN, "ISBN")
            data["url"] = f"https://isbnsearch.org/isbn/{isbn}"
        return data


class DOI(PropertyValue):
    name: str = "Digital Object Identifier"
    alternateName: str = "DOI"
    propertyID: AnyUrl = "https://registry.identifiers.org/registry/doi"

    @model_validator(mode="before")
    @classmethod
    def clean_doi(cls, data):
        if "value" in data:
            doi = search_pattern(str(data["value"]), DOI_PATTERN, "DOI")
            data["value"] = doi
            data["url"] = f"https://doi.org/{doi}"
        return data


class UrlIdentifier(PropertyValue):
    name: str = "URL Identifier"
    disambiguatingDescription: str = (
        "The value of this identifier has been provided by the "
        "data submitter, but is not of a known identifier type "
        "to BIOcean5D's task 3.1. Thus the value is provided as "
        "given with no guarantee of persistence or regularity."
    )

    @field_validator("value")
    @classmethod
    def ensure_no_doi(cls, v: str) -> str:
        if DOI_PATTERN.search(v):
            raise ValueError("A valid DOI was given to the arbitrary UrlIdentifier.")
        else:
            return v


# ---------------------------------------------------------------------------
# Dynamic lookup
# ---------------------------------------------------------------------------
SCHEMA_TYPE_REGISTRY: dict[str, type[SchemaOrgBase]] = {
    k.lower(): v
    for k, v in globals().items()
    if isinstance(v, type) and issubclass(v, SchemaOrgBase)
}


def get_schema(type_name: str) -> type[SchemaOrgBase]:
    """
    Return the Pydantic model class for a schema.org type name.

    It works for all naming styles, as the comparison is done on the lowercase names.

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
    cls = SCHEMA_TYPE_REGISTRY.get(type_name.lower())
    if cls is None:
        raise KeyError(
            f"{type_name!r} is not a known schema.org type. Ensure that it is available in schema.org and update the Pydantic models if necessary."
        )
    return cls
