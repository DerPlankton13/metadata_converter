import re

from pydantic import AnyUrl, field_validator

from metadata_converter.schema_org_models.custom_models import search_pattern
from metadata_converter.schema_org_models.schemaorg_models import PropertyValue


class SRA(PropertyValue):
    name: str = "Short Read Archive Accession"
    alternateName: str = "SRA"
    propertyID: AnyUrl = "https://registry.identifiers.org/registry/insdc.sra"

    def __init__(self, **data):
        data["url"] = (
            f"https://www.ebi.ac.uk/ena/browser/view/{data['value']}?dataType=SAMPLE"
        )
        super().__init__(**data)

    @field_validator("value")
    @classmethod
    def search_sra(cls, v: str) -> str:
        return search_pattern(v, re.compile(r"^[SED]R[APRSXZ]\d+$"), "SRA")


class BioSample(PropertyValue):
    name: str = "BioSamples Accession"
    alternateName: str = "BioSample"
    propertyID: AnyUrl = "https://registry.identifiers.org/registry/biosample"

    def __init__(self, **data):
        data["url"] = [
            f"https://identifiers.org/biosample/{data['value']}",
            f"https://www.ebi.ac.uk/biosamples/samples/{data['value']}",
        ]
        super().__init__(**data)

    @field_validator("value")
    @classmethod
    def search_biosample(cls, v: str) -> str:
        return search_pattern(v, re.compile(r"^SAM[NED](\w)?\d+$"), "BioSample")


class Checklist(PropertyValue):
    name: str = "checklist"
    description: str = "There is a minimum amount of information required during ENA sample registration and all samples must conform to a defined checklist of expected metadata values. The most suitable checklist for sample registration depends on the type of the sample. (https://www.ebi.ac.uk/ena/browser/checklists)"

    def __init__(self, **data):
        data["url"] = f"https://www.ebi.ac.uk/ena/browser/view/{data['value']}"
        super().__init__(**data)
