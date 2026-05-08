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
    except (Exception, KeyError, IndexError):
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
    except (Exception, KeyError, IndexError):
        pass
    return value, unit


class Terminology(Enum):
    NERC = ("https://vocab.nerc.ac.uk/collection/L22/current/", None)
    ENVO = (
        "https://purl.obolibrary.org/obo/",
        "https://purl.obolibrary.org/obo/envo.owl",
    )
    NCBI = (
        "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=",
        "https://www.ncbi.nlm.nih.gov/Taxonomy",
    )

    def __init__(self, url: str, defined_termset: str | None) -> None:
        self.url = url
        self.defined_termset = defined_termset

    @classmethod
    def from_term_code(cls, term_code: str) -> "Terminology | None":
        if "NERC" in term_code:
            return cls.NERC
        elif "ENVO" in term_code:
            return cls.ENVO
        elif "txid" in term_code:
            return cls.NCBI
        return None

    def normalize_term_code(self, term_code: str) -> str:
        if self == Terminology.NERC:
            return term_code.replace("NERC:", "")
        if self == Terminology.NCBI:
            return term_code.split("txid")[-1]
        return term_code


def build_defined_term(value: str) -> dict[str, str] | None:
    try:
        term_code = re.search(r"\[([^\[\]]*)\]", value).group(1)
    except AttributeError:
        return None
    name = value.split("[")[0].strip()
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
        "url": terminology.url,
    }
    if terminology.defined_termset:
        defined_term_dict["inDefinedTermSet"] = terminology.defined_termset
    return defined_term_dict


def build_property(
    sample_record: dict, prop_name: str, prop_id: str | None = None
) -> dict | None:
    prop = get_property(sample_record, prop_name)
    if not prop:
        return None

    if prop_id:
        prop["propertyID"] = prop_id
    # see if wee can create a value reference from the value
    value_reference = build_defined_term(prop.get("value"))
    # if so, overwrite any possibly existing, otherwise any existing will be kept
    if value_reference:
        prop["valueReference"] = value_reference

    return prop


class SampleExtractor:
    def __init__(self, sample_record: dict, sample_id: str):
        self.sample_record = sample_record
        self.sample_id = sample_id
        self._used_props: set[str] = set()

    def _get_prop(self, prop_name: str) -> dict:
        self._used_props.add(prop_name)
        return get_property(self.sample_record, prop_name)

    def _get_prop_value(self, prop_name: str) -> str | None:
        self._used_props.add(prop_name)
        return get_value(self.sample_record, prop_name)

    def _get_prop_value_with_unit(
        self, prop_name: str
    ) -> tuple[str | None, str | Literal["Unit unknown"]]:
        self._used_props.add(prop_name)
        return get_value_with_unit(self.sample_record, prop_name)

    def _build_prop(
        self, prop_name: str, prop_id: str | None = None
    ) -> dict[str, str] | None:
        self._used_props.add(prop_name)
        return build_property(self.sample_record, prop_name, prop_id)

    def _get_base_value(self, name: str) -> str:
        return self.sample_record["mainEntity"].get(name)

    def _build_product_dict(self) -> dict:

        # prebuild more complex entries
        def build_identifiers() -> list[dict] | dict:
            identifier_list = [
                BioSample(value=self.sample_id).model_dump(
                    by_alias=True, exclude_none=True
                ),
            ]
            if sra_accession := self._get_prop_value("SRA accession"):
                identifier_list.append(
                    SRA(value=sra_accession).model_dump(
                        by_alias=True, exclude_none=True
                    )
                )
            return identifier_list if len(identifier_list) > 1 else identifier_list[0]

        def build_manufacturer() -> list[dict] | dict:
            manufacturer = [
                {
                    "@type": "ResearchProject",
                    "@id": "https://github.com/DerPlankton13/B5D/blob/main/GeneralSchemas/project_b5d.jsonld",
                }
            ]
            if project_name := self._get_prop_value("project name"):
                manufacturer.append({"@type": "ResearchProject", "name": project_name})
            return manufacturer if len(manufacturer) > 1 else manufacturer[0]

        def build_keywords() -> list[dict] | None:
            keywords = []
            organism = self._get_prop_value("organism")
            if organism:
                if defined_term := build_defined_term(organism):
                    keywords.append(defined_term)
                else:
                    keywords.append(organism)
            if target := self._get_prop_value("target analysis type"):
                keywords.append(target)
            if local := self._get_prop_value("local environmental context"):
                keywords.append(local)
            return keywords if len(keywords) > 0 else None

        def build_additional_property() -> list[dict[str, Any]] | dict[str, Any] | None:
            additional_property = []
            if checklist := self._get_prop_value("checklist"):
                additional_property.append(
                    Checklist(value=checklist).model_dump(
                        by_alias=True, exclude_none=True
                    )
                )
            if target_analysis := self._build_prop("target analysis type"):
                additional_property.append(target_analysis)
            if not additional_property:
                return None
            return (
                additional_property
                if len(additional_property) > 1
                else additional_property[0]
            )

        return {
            "@context": {"@vocab": "https://schema.org"},
            "@type": "Product",
            "additionalType": [
                "sample",
                "https://purl.obolibrary.org/obo/OBI_0000747",
            ],
            "@id": f"Product_{self.sample_id}.jsonld",
            "identifier": build_identifiers(),
            "name": self._get_base_value("name"),
            "description": self._get_prop_value("sample description"),
            "url": self._get_base_value("url"),
            "productionDate": self._get_prop_value("collection date"),
            "material": self._get_prop_value("environmental medium"),
            "countryOfOrigin": self._get_prop_value(
                "geographic location (country and/or sea)"
            ),
            "funding": {
                "@type": "MonetaryGrant",
                "@id": "https://github.com/DerPlankton13/B5D/blob/main/GeneralSchemas/grant_b5d.jsonld",
            },
            "manufacturer": build_manufacturer(),
            "keywords": build_keywords(),
            "additionalProperty": build_additional_property(),
        }

    def _build_action_dict(self) -> dict:
        """
        Extract action data from biosamples metadata and convert to Action.jsonld format.

        Returns
        -------
        dict
            A dictionary representing the Action in JSON-LD format
        """

        def build_location() -> dict[str, Any]:
            """Build the schema.org location Property as type Place"""

            # create name Property if possible
            region = self._get_prop_value("geographic location (region and locality)")
            country = self._get_prop_value("geographic location (country and/or sea)")
            loc_name = ", ".join(filter(None, [region, country]))

            # adds geo Property to location as type GeoCoordinates if values are provided
            geo_fields = {
                "latitude": self._get_prop_value_with_unit(
                    "geographic location (latitude)"
                ),
                "longitude": self._get_prop_value_with_unit(
                    "geographic location (longitude)"
                ),
                "elevation": self._get_prop_value_with_unit("elevation"),
            }
            geo = {
                key: f"{value} {unit}"
                for key, (value, unit) in geo_fields.items()
                if value
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
                prop = self._build_prop(prop_name, prop_id)
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
            if sampling_design_label := self._get_prop_value("sampling design label"):
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
                "additionalProperty": additional_property
                if additional_property
                else None,
            }

        def build_instrument() -> dict[str, Any] | None:
            # Build instrument array
            instrument = []
            for prop_name in ["sample collection device", "sampling platform"]:
                prop = self._build_prop(prop_name)
                if prop:
                    instrument.append(prop)
            return instrument if instrument else None

        def build_object() -> dict[str, Any] | None:
            # Build object array
            object = []
            for prop_name, prop_id in {
                "environmental medium": "https://w3id.org/mixs/0000014",
                "organism": None,
            }.items():
                prop = self._build_prop(prop_name, prop_id)
                if prop:
                    object.append(prop)
            # Todo clarify organism
            # weird, unclear if this is to be understood as the object or the result - intuition is to use object, if this was a penguin, I'd assume that the penguin was the object of sampling and not the result

            return object if object else None

        def build_action_process() -> dict[str, Any] | None:
            step = []

            # Filtration step - extract actual values and units from data
            filtration_param = {
                "filtration volume": self._get_prop_value_with_unit(
                    "filtration volume"
                ),
                "filtration time": self._get_prop_value_with_unit("filtration time"),
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
                "lower threshold": self._get_prop_value_with_unit(
                    "size-fraction lower threshold"
                ),
                "upper threshold": self._get_prop_value_with_unit(
                    "size-fraction upper threshold"
                ),
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
                    "name": f"Submitter-declared sampling steps for sample {self.sample_id}.",
                    "description": "The steps in this object are those that have been provided by the submitter of this metadata. They are not ordered and may be incomplete. Please refer to the associated publication and or documentation for more authoritative information.",
                    "step": step,
                }
            return None

        def build_participant() -> list[dict[str, Any]] | dict[str, Any]:
            participant = [
                {
                    "@type": "ResearchProject",
                    "@id": "https://github.com/DerPlankton13/B5D/blob/main/GeneralSchemas/project_b5d.jsonld",
                }
            ]
            if project_name := self._get_prop_value("project name"):
                participant.append({"@type": "ResearchProject", "name": project_name})
            return participant if len(participant) > 1 else participant[0]

        def build_additional_property() -> list[dict[str, Any]] | dict[str, Any] | None:
            additional_property = []
            if checklist := self._get_prop_value("checklist"):
                additional_property.append(
                    Checklist(value=checklist).model_dump(
                        by_alias=True, exclude_none=True
                    )
                )
            if protocol_label := self._build_prop("protocol label"):
                additional_property.append(protocol_label)

            if not additional_property:
                return None

            return (
                additional_property
                if len(additional_property) > 1
                else additional_property[0]
            )

        return {
            "@context": {"@vocab": "https://schema.org"},
            "@type": "Action",
            "additionalType": [
                "sampling process",
                "https://purl.obolibrary.org/obo/OBI_0000744",
            ],
            "@id": f"Action_{self.sample_id}.jsonld",
            "name": f"Sampling process for sample {self.sample_id}",
            "result": {"@type": "Product", "@id": f"Product_{self.sample_id}.jsonld"},
            "startTime": self._get_prop_value("collection date"),
            "location": build_location(),
            "instrument": build_instrument(),
            "object": build_object(),
            "actionProcess": build_action_process(),
            "participant": build_participant(),
            "additionalProperty": build_additional_property(),
        }

    def _append_remaining_props(self, schema_dict: dict[str, str]):
        used_props = set(self._used_props)
        remaining_props = [
            p
            for p in self.sample_record["mainEntity"]["additionalProperty"]
            if p.get("name") not in used_props
        ]

        if remaining_props:
            additional_property = schema_dict.get("additionalProperty")
            if additional_property is not None:
                if isinstance(additional_property, list):
                    remaining_props = additional_property + remaining_props
                else:
                    remaining_props.insert(0, additional_property)

            schema_dict["additionalProperty"] = remaining_props

    def build_dicts(self) -> tuple[dict[str, Any], dict[str, Any]]:
        product_dict = self._build_product_dict()
        action_dict = self._build_action_dict()
        self._append_remaining_props(product_dict)
        self._append_remaining_props(action_dict)
        return product_dict, action_dict
