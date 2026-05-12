import re
from enum import Enum
from typing import Any, Literal

from metadata_converter.biosamples.schemas import (
    SRA,
    BioSample,
    Checklist,
)


def get_property(sample_record: dict, prop_name: str) -> dict | None:
    props = sample_record["mainEntity"]["additionalProperty"]

    results = [p for p in props if p.get("name") == prop_name]

    if len(results) != 1:
        print(f"No unique match found for {prop_name} within {sample_record}")
        return None

    return results[0].copy()


def get_value(sample_record: dict, prop_name: str) -> str | None:
    """
    Safely extract a property value from the data without raising exceptions.
    Returns the default value if the property is not found.
    """
    try:
        return get_property(sample_record, prop_name)["value"]
    except Exception:
        return None


def get_value_with_unit(
    sample_record: dict, prop_name: str
) -> tuple[str | None, str | Literal["Unit unknown"]]:
    """
    Safely extract a property value and its unit from the data without raising exceptions.
    Returns the default values if the property is not found.

    Returns
    -------
    tuple[Any, Any]
        (value, unit) tuple
    """
    value, unit = None, "Unit unknown"
    try:
        prop = get_property(sample_record, prop_name)
        value = prop["value"]
        unit = prop["unitText"]
    except Exception:
        pass
    return value, unit


class Terminology(Enum):
    ENVO = (
        "https://purl.obolibrary.org/obo/",
        "https://purl.obolibrary.org/obo/envo.owl",
    )
    NCBI = (
        "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=",
        "https://www.ncbi.nlm.nih.gov/Taxonomy",
    )
    NERC = ("https://vocab.nerc.ac.uk/collection/", None)

    def __init__(self, url: str, defined_termset: str | None) -> None:
        self.base_url = url  # renamed to make clear it's a base
        self.defined_termset = defined_termset

    @classmethod
    def from_term_code(cls, term_code: str) -> "Terminology | None":
        if "ENVO" in term_code:
            return cls.ENVO
        elif "txid" in term_code:
            return cls.NCBI
        elif "NERC" in term_code:
            return cls.NERC
        return None

    def normalize_term_code(self, term_code: str) -> str:
        if self == Terminology.NCBI:
            return term_code.split("txid")[-1]
        if self == Terminology.NERC:
            return term_code.split("NERC:")[-1]
        return term_code

    def build_url(self, term_code: str) -> str:
        normalized = self.normalize_term_code(term_code)
        if self == Terminology.ENVO:
            return self.base_url + normalized.replace(":", "_")
        elif self == Terminology.NCBI:
            return self.base_url + normalized
        elif self == Terminology.NERC:
            vocab = normalized.split(":")[1]
            concept = normalized.split("::")[-1]
            return self.base_url + vocab + "/current/" + concept
        return self.base_url


def build_defined_term(value: str) -> dict[str, str] | None:
    matches = re.findall(r"[\[(](.*?)[\])]", value)
    if len(matches) != 1:
        return None
    term_code = matches[0]
    name = re.split(r"[\[(]", value)[0].strip()
    terminology = Terminology.from_term_code(term_code)
    if not terminology:
        print(
            f"Could not identify a known terminology from {value}. Available terminologies are: ",
            ", ".join([t.name for t in Terminology]),
        )
        return None

    term_code = terminology.normalize_term_code(term_code)
    defined_term_dict = {
        "@type": "DefinedTerm",
        "name": name,
        "termCode": term_code,
        "url": terminology.build_url(term_code),
    }
    if terminology.defined_termset:
        defined_term_dict["inDefinedTermSet"] = terminology.defined_termset
    return defined_term_dict


def build_property(
    sample_record: dict, prop_name: str, prop_id: str | None = None
) -> dict | None:
    prop = get_property(sample_record, prop_name)
    if prop is None:
        return None

    if prop_id:
        prop["propertyID"] = prop_id

    # try to build a value reference from the value and overwrite any existing one
    value_reference = build_defined_term(prop.get("value"))
    if value_reference:
        prop["valueReference"] = value_reference
    # otherwise check if there is an existing value reference and clean it up if needed
    elif existing := prop.get("valueReference"):
        if isinstance(existing, list) and len(existing) == 1:
            existing = existing[0]
        # remove any existing valueReference that contain no information
        if not any(v for k, v in existing.items() if k != "@type"):
            prop.pop("valueReference")
        # fix http links
        else:
            prop["valueReference"] = {
                k: convert_to_https(v) for k, v in existing.items()
            }

    return prop


def convert_to_https(link: str) -> str:
    return link.replace("http://", "https://")


class SampleRecord:
    __slots__ = ("_raw", "sample_id", "_used")

    def __init__(self, raw: dict, sample_id: str):
        self._raw = raw
        self.sample_id = sample_id
        self._used: set[str] = set()

    @staticmethod
    def _normalize_prop(prop: dict) -> dict:
        if prop.get("unitText") == "":
            prop.pop("unitText")
        return prop

    def __getitem__(self, prop_name: str) -> str | None:
        self._used.add(prop_name)
        return get_value(self._raw, prop_name)

    def __contains__(self, prop_name: str) -> bool:
        return get_property(self._raw, prop_name) is not None

    def base_value(self, name: str) -> str | None:
        return self._raw["mainEntity"].get(name)

    def with_unit(self, prop_name: str) -> tuple[str | None, str]:
        self._used.add(prop_name)
        return get_value_with_unit(self._raw, prop_name)

    def as_property(self, prop_name: str, prop_id: str | None = None) -> dict | None:
        self._used.add(prop_name)
        prop = build_property(self._raw, prop_name, prop_id)
        return self._normalize_prop(prop) if prop else None

    def raw_property(self, prop_name: str) -> dict | None:
        self._used.add(prop_name)
        prop = get_property(self._raw, prop_name)
        return self._normalize_prop(prop) if prop else None

    def remaining(self) -> list[dict]:
        excluded_values = ["not applicable"]
        remaining_props = []
        for prop in self._raw["mainEntity"]["additionalProperty"]:
            if (
                prop.get("name") not in self._used
                and prop.get("value") not in excluded_values
            ):
                prop = self.as_property(prop.get("name"))
                remaining_props.append(prop)
        return remaining_props


class BaseBuilder:
    def __init__(self, record: SampleRecord):
        self.record = record

    def _build_checklist(self) -> dict | None:
        checklist_prop = self.record.raw_property("checklist")
        ena_checklist_prop = self.record.raw_property("ENA-CHECKLIST")
        checklist = checklist_prop or ena_checklist_prop
        if checklist:
            # check that it is a checklist from ENA
            if "ERC" in checklist["value"]:
                return Checklist(value=checklist["value"]).model_dump(
                    by_alias=True, exclude_none=True
                )
            # otherwise just return it as is
            return checklist
        return None

    @staticmethod
    def _unwrap_single(items: list) -> list | dict:
        return items if len(items) > 1 else items[0]


class ProductBuilder(BaseBuilder):
    def _build_identifiers(self) -> list[dict] | dict:
        identifier_list = [
            BioSample(value=self.record.sample_id).model_dump(
                by_alias=True, exclude_none=True
            ),
        ]
        if sra_accession := self.record["SRA accession"]:
            identifier_list.append(
                SRA(value=sra_accession).model_dump(by_alias=True, exclude_none=True)
            )
        return self._unwrap_single(identifier_list)

    def _build_manufacturer(self) -> list[dict] | dict:
        manufacturer = [
            {
                "@type": "ResearchProject",
                "@id": "https://github.com/DerPlankton13/B5D/blob/main/GeneralSchemas/project_b5d.jsonld",
            }
        ]
        if project_name := self.record["project name"]:
            # do not add the B5D project a second time
            if project_name.lower() not in ["BIOcean5D".lower(), "b5d"]:
                manufacturer.append({"@type": "ResearchProject", "name": project_name})
        return self._unwrap_single(manufacturer)

    def _build_keywords(self) -> list[dict] | None:
        keywords = []
        desired_properties = [
            "organism",
            "target analysis type",
            "local environmental context",
        ]
        for prop_name in desired_properties:
            if prop := self.record[prop_name]:
                if defined_term := build_defined_term(prop):
                    keywords.append(defined_term)
                else:
                    keywords.append(prop)
        return keywords if len(keywords) > 0 else None

    def _build_additional_property(self) -> list[dict] | dict | None:
        additional_property = []
        if checklist := self._build_checklist():
            additional_property.append(checklist)
        if target_analysis := self.record.as_property("target analysis type"):
            additional_property.append(target_analysis)
        if not additional_property:
            return None
        return self._unwrap_single(additional_property)

    def build(self) -> dict:
        return {
            "@context": {"@vocab": "https://schema.org"},
            "@type": "Product",
            "additionalType": [
                "sample",
                "https://purl.obolibrary.org/obo/OBI_0000747",
            ],
            "@id": f"Product_{self.record.sample_id}.jsonld",
            "identifier": self._build_identifiers(),
            "name": self.record.base_value("name"),
            "description": self.record["sample description"],
            "url": convert_to_https(self.record.base_value("sameAs")),
            "productionDate": self.record["collection date"],
            "material": self.record["environmental medium"],
            "countryOfOrigin": self.record["geographic location (country and/or sea)"],
            "funding": {
                "@type": "MonetaryGrant",
                "@id": "https://github.com/DerPlankton13/B5D/blob/main/GeneralSchemas/grant_b5d.jsonld",
            },
            "manufacturer": self._build_manufacturer(),
            "keywords": self._build_keywords(),
            "additionalProperty": self._build_additional_property(),
        }


class ActionBuilder(BaseBuilder):
    def _build_location(self) -> dict:
        """Build the schema.org location Property as type Place"""
        # create name Property if possible
        region = self.record["geographic location (region and locality)"]
        country = self.record["geographic location (country and/or sea)"]
        loc_name = ", ".join(filter(None, [region, country]))

        # adds geo Property to location as type GeoCoordinates if values are provided
        geo_fields = {
            "latitude": self.record.with_unit("geographic location (latitude)"),
            "longitude": self.record.with_unit("geographic location (longitude)"),
            "elevation": self.record.with_unit("elevation"),
        }
        geo = {
            key: f"{value} {unit}" for key, (value, unit) in geo_fields.items() if value
        }

        # We can add additionalProperty to location
        additional_property = []

        # Uplift the properties by adding their MIxS IDs if applicable
        for prop_name, prop_id in {
            "broad-scale environmental context": "https://w3id.org/mixs/0000012",
            "local environmental context": "https://w3id.org/mixs/0000013",
            "depth": "https://w3id.org/mixs/0000018",
            "depth-max": None,
            "depth-min": None,
        }.items():
            prop = self.record.as_property(prop_name, prop_id)
            if prop:
                additional_property.append(prop)

        # Geographic location combined to MIxS term
        if region and country:
            additional_property.append(
                {
                    "@type": "PropertyValue",
                    "propertyID": "https://w3id.org/mixs/0000010",
                    "name": "geographic location (country and/or sea,region)",
                    "value": f"{country}: , {region}",
                }
            )

        # Add an uplifted sampling design label if provided
        if sampling_design_label := self.record["sampling design label"]:
            additional_property.append(
                {
                    "@type": "PropertyValue",
                    "name": "sampling design label",
                    "description": "Sampling Design Label (SDL) is a unique identifier used to track all samples and data originating from the same sampling location. (https://biocean5d.embl.de/faq.cgi)",
                    "propertyID": "sampling design label",
                    "value": sampling_design_label,
                }
            )

        return {
            "@type": "Place",
            "name": loc_name if loc_name else None,
            "geo": {"@type": "GeoCoordinates"} | geo if geo else None,
            "additionalProperty": additional_property if additional_property else None,
        }

    def _build_instrument(self) -> list[dict] | None:
        instrument = []
        for prop_name in ["sample collection device", "sampling platform"]:
            if prop := self.record.raw_property(prop_name):
                category = None
                if value_reference := prop.get("valueReference"):
                    if isinstance(value_reference, list) and len(value_reference) == 1:
                        value_reference = value_reference[0]
                    category = value_reference.get("@id")
                print(category)
                instrument.append(
                    {
                        "@type": "Product",
                        "description": prop_name,
                        "name": prop.get("value"),
                        "category": category,
                    }
                )
        return instrument if instrument else None

    def _build_object(self) -> list[dict] | None:
        object = []
        for prop_name, prop_id in {
            "environmental medium": "https://w3id.org/mixs/0000014",
            "organism": None,
        }.items():
            prop = self.record.as_property(prop_name, prop_id)
            if prop:
                object.append(prop)
        # Todo clarify organism
        # weird, unclear if this is to be understood as the object or the result - intuition is to use object, if this was a penguin, I'd assume that the penguin was the object of sampling and not the result
        return object if object else None

    def _build_action_process(self) -> dict | None:
        step = []

        # Filtration step - extract actual values and units from data
        filtration_param = {
            "filtration volume": self.record.with_unit("filtration volume"),
            "filtration time": self.record.with_unit("filtration time"),
        }
        text_parts = [
            f"{label}: {value} {unit}"
            for label, (value, unit) in filtration_param.items()
            if value
        ]
        if text_parts:
            step.append(
                {
                    "@type": "HowToStep",
                    "name": "filtration",
                    "text": ", ".join(text_parts),
                }
            )

        # Size fractionation step
        size_frac_param = {
            "lower threshold": self.record.with_unit("size-fraction lower threshold"),
            "upper threshold": self.record.with_unit("size-fraction upper threshold"),
        }
        text_parts = [
            f"{label} of {value} {unit}"
            for label, (value, unit) in size_frac_param.items()
            if value
        ]
        if text_parts:
            step.append(
                {
                    "@type": "HowToStep",
                    "name": "size fractionation",
                    "text": f"size fractionation was performed with: {', '.join(text_parts)}",
                }
            )

        if step:
            return {
                "@type": "HowTo",
                "name": f"Submitter-declared sampling steps for sample {self.record.sample_id}.",
                "description": "The steps in this object are those that have been provided by the submitter of this metadata. They are not ordered and may be incomplete. Please refer to the associated publication and or documentation for more authoritative information.",
                "step": step,
            }
        return None

    def _build_participant(self) -> list[dict] | dict:
        participant = [
            {
                "@type": "ResearchProject",
                "@id": "https://github.com/DerPlankton13/B5D/blob/main/GeneralSchemas/project_b5d.jsonld",
            }
        ]
        if project_name := self.record["project name"]:
            participant.append({"@type": "ResearchProject", "name": project_name})
        return self._unwrap_single(participant)

    def _build_additional_property(self) -> list[dict] | dict | None:
        additional_property = []
        if checklist := self._build_checklist():
            additional_property.append(checklist)
        if protocol_label := self.record.as_property("protocol label"):
            additional_property.append(protocol_label)
        if not additional_property:
            return None
        return self._unwrap_single(additional_property)

    def build(self) -> dict:
        return {
            "@context": {"@vocab": "https://schema.org"},
            "@type": "Action",
            "additionalType": [
                "sampling process",
                "https://purl.obolibrary.org/obo/OBI_0000744",
            ],
            "@id": f"Action_{self.record.sample_id}.jsonld",
            "name": f"Sampling process for sample {self.record.sample_id}",
            "result": {
                "@type": "Product",
                "@id": f"Product_{self.record.sample_id}.jsonld",
            },
            "startTime": self.record["collection date"],
            "location": self._build_location(),
            "instrument": self._build_instrument(),
            "object": self._build_object(),
            "actionProcess": self._build_action_process(),
            "participant": self._build_participant(),
            "additionalProperty": self._build_additional_property(),
        }


class SampleExtractor:
    def __init__(self, raw: dict, sample_id: str):
        self.record = SampleRecord(raw, sample_id)

    @staticmethod
    def _append_remaining_props(
        schema_dict: dict[str, Any], remaining_props: list[dict[str, Any]]
    ) -> dict[str, Any]:
        additional_property = schema_dict.get("additionalProperty")
        if additional_property is not None:
            if isinstance(additional_property, list):
                combined = additional_property + remaining_props
            else:
                combined = [additional_property] + remaining_props
        else:
            combined = remaining_props
        schema_dict["additionalProperty"] = combined
        return schema_dict

    def build_dicts(self) -> tuple[dict[str, Any], dict[str, Any]]:
        product_dict = ProductBuilder(self.record).build()
        action_dict = ActionBuilder(self.record).build()
        remaining_props = self.record.remaining()
        if remaining_props:
            product_dict = self._append_remaining_props(product_dict, remaining_props)
            action_dict = self._append_remaining_props(action_dict, remaining_props)
        return product_dict, action_dict
