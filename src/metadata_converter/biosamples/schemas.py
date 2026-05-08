import re

from pydantic import AnyUrl, field_validator

from metadata_converter.schema_org_models.custom_models import search_pattern
from metadata_converter.schema_org_models.schemaorg_models import PropertyValue


class SRA(PropertyValue):
    name: str = "Short Read Archive Accession"
    alternateName: str = "SRA"
    propertyID: AnyUrl = "https://registry.identifiers.org/registry/insdc.sra"

    @field_validator("value")
    @classmethod
    def search_sra(cls, v: str) -> str:
        return search_pattern(v, re.compile(r"^[SED]R[APRSXZ]\d+$"), "SRA")

    def model_post_init(self, __context) -> None:
        object.__setattr__(
            self,
            "url",
            f"https://www.ebi.ac.uk/ena/browser/view/{self.value}?dataType=SAMPLE",
        )


class BioSample(PropertyValue):
    name: str = "BioSamples Accession"
    alternateName: str = "BioSample"
    propertyID: AnyUrl = "https://registry.identifiers.org/registry/biosample"

    def model_post_init(self, __context) -> None:
        object.__setattr__(
            self,
            "url",
            [
                f"https://identifiers.org/biosample/{self.value}",
                f"https://www.ebi.ac.uk/biosamples/samples/{self.value}",
            ],
        )

    @field_validator("value")
    @classmethod
    def search_biosample(cls, v: str) -> str:
        return search_pattern(v, re.compile(r"^SAM[NED](\w)?\d+$"), "BioSample")


class Checklist(PropertyValue):
    name: str = "checklist"
    description: str = "There is a minimum amount of information required during ENA sample registration and all samples must conform to a defined checklist of expected metadata values. The most suitable checklist for sample registration depends on the type of the sample. (https://www.ebi.ac.uk/ena/browser/checklists)"

    @model_validator(mode="before")
    @classmethod
    def set_url(cls, data):
        data["url"] = f"https://www.ebi.ac.uk/ena/browser/view/{data['value']}"
        return data
