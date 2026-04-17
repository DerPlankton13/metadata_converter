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
DOI_PATTERN = re.compile(r"10\.\d+/.*$")
SRA_PATTERN = re.compile(r"^[SED]R[APRSXZ]\d+$")


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

    @field_validator("value")
    @classmethod
    def check_issn(cls, v: str) -> str:
        return check_pattern(v, ISSN_PATTERN, "ISSN")

    @model_validator(mode="after")
    def set_url(self):
        self.url = f"https://portal.issn.org/resource/ISSN/{self.value}"
        return self


class ISBN(PropertyValue):
    name: str = "International Standard Book Number"
    alternateName: str = "ISBN"
    propertyID: AnyUrl = "https://registry.identifiers.org/registry/isbn"

    @field_validator("value")
    @classmethod
    def check_isbn(cls, v: str) -> str:
        return check_pattern(v, ISBN_PATTERN, "ISBN")

    @model_validator(mode="after")
    def set_url(self):
        self.url = f"https://isbnsearch.org/isbn/{self.value}"
        return self


class DOI(PropertyValue):
    name: str = "Digital Object Identifier"
    alternateName: str = "DOI"
    propertyID: AnyUrl = "https://registry.identifiers.org/registry/doi"

    @field_validator("value")
    @classmethod
    def check_sra(cls, v: str) -> str:
        return check_pattern(v, DOI_PATTERN, "DOI")

    @model_validator(mode="after")
    def set_url(self):
        self.url = f"https://doi.org/{self.value}"
        return self


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


class SRA(PropertyValue):
    name: str = "Short Read Archive Accession"
    alternateName: str = "SRA"
    propertyID: AnyUrl = "https://registry.identifiers.org/registry/insdc.sra"

    def __init__(self, **data):
        data["url"] = f"https://www.ebi.ac.uk/ena/browser/view/{data['value']}?dataType=SAMPLE"
        super().__init__(**data)

    @field_validator("value")
    @classmethod
    def search_doi(cls, v: str) -> str:
        return search_pattern(v, SRA_PATTERN, "SRA")


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
