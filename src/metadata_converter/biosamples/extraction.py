import re
from enum import Enum
from typing import Any, Literal

from metadata_converter.biosamples.schemas import SRA, BioSample


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
    finally:
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
            ",".join([t.name for t in Terminology]),
        )
        return None

    term_code = terminology.normalize_term_code(term_code)
    defined_term_dict = {
        "@type": "DefinedTerm",
        "name": name,
        "termCode": term_code,
        "url": terminology.value[0],
    }
    if terminology.value[1]:
        defined_term_dict["inDefinedTermSet"] = terminology.value[1]
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
        self._used_props: list[str] = []
        self.product_dict: dict[str, str] = {}
        self.action_dict: dict[str, str] = {}

    def _get_prop(self, prop_name: str) -> dict:
        self._used_props.append(prop_name)
        return get_property(self.sample_record, prop_name)

    def _get_prop_value(self, prop_name: str) -> str | None:
        return get_value(self.sample_record, prop_name)

    def _get_prop_value_with_unit(
        self, prop_name: str
    ) -> tuple[str | None, str | Literal["Unit unknown"]]:
        return get_value_with_unit(self.sample_record, prop_name)

    def _append_built_prop(self, prop_list: list, prop_name: str, prop_id=None) -> None:
        prop = build_property(self.sample_record, prop_name, prop_id)
        if prop:
            prop_list.append(prop)

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
            sra_accession = self._get_prop_value("SRA accession")
            if sra_accession:
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
            project_name = self._get_prop_value("project name")
            if project_name:
                manufacturer.append({"@type": "ResearchProject", "name": project_name})
            return manufacturer if len(manufacturer) > 1 else manufacturer[0]

        def build_keywords() -> list[dict] | None:
            keywords = []
            organism = self._get_prop_value("organism")
            if organism:
                defined_term = build_defined_term(organism)
                if defined_term:
                    keywords.append(defined_term)
                else:
                    keywords.append(organism)
            target = self._get_prop_value("target analysis type")
            if target:
                keywords.append(target)
            local = self._get_prop_value("local environmental context")
            if local:
                keywords.append(local)
            return keywords if len(keywords) > 0 else None

        def build_additional_property() -> list[dict] | None:
            additional_property = []
            checklist = self._get_prop_value("checklist")
            target_analysis = self._get_prop("target analysis type")
            if checklist or target_analysis:
                if checklist:
                    additional_property.append(
                        {
                            "@type": "PropertyValue",
                            "name": "checklist",
                            "value": checklist,
                            "description": "There is a minimum amount of information required during ENA sample registration and all samples must conform to a defined checklist of expected metadata values. The most suitable checklist for sample registration depends on the type of the sample. (https://www.ebi.ac.uk/ena/browser/checklists)",
                            "url": f"https://www.ebi.ac.uk/ena/browser/view/{checklist}",
                        }
                    )
                if target_analysis:
                    additional_property.append(target_analysis)
            return additional_property if len(additional_property) > 0 else None

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
            "description": self._get_prop_value("description"),
            "url": self._get_base_value("ur"),
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
        action_dict = {
            "@context": {"@vocab": "https://schema.org"},
            "@type": "Action",
            "additionalType": [
                "sampling process",
                "https://purl.obolibrary.org/obo/OBI_0000744",
            ],
            "@id": f"Action_{self.sample_id}.jsonld",
            "name": f"Sampling process for sample {self.sample_id}",
            "result": {"@type": "Product", "@id": f"Product_{self.sample_id}.jsonld"},
        }

        # Add startTime from collection date
        collection_date = self._get_prop_value("collection date")
        if collection_date:
            action_dict["startTime"] = collection_date

        # Build the schema.org location Property as type Place
        location = {
            "@type": "Place",
        }

        # create name Property if possible
        region = get_value(sample_record, "geographic location (region and locality)")
        country = get_value(sample_record, "geographic location (country and/or sea)")
        if region and country:
            location["name"] = f"{region}, {country}"
        elif region:
            location["name"] = region
        elif country:
            location["name"] = country

        # adds geo Property to location as type GeoCoordinates if values are provided
        latitude, lat_unit = get_value_with_unit(
            sample_record, "geographic location (latitude)"
        )
        longitude, lon_unit = get_value_with_unit(
            sample_record, "geographic location (longitude)"
        )
        elevation, elev_unit = get_value_with_unit(sample_record, "elevation")

        if latitude or longitude or elevation:
            geo = {"@type": "GeoCoordinates"}
            if latitude:
                geo["latitude"] = f"{latitude} {lat_unit}"
            if longitude:
                geo["longitude"] = f"{longitude} {lon_unit}"
            if elevation:
                geo["elevation"] = f"{elevation} {elev_unit}"
            location["geo"] = geo

        # We can add additionalProperty to location
        location_props = []

        # Uplift the properties by adding their MIxS IDs if applicable
        for name, prop_id in {
            "broad-scale environmental context": "https://w3id.org/mixs/0000012",
            "local environmental context": "https://w3id.org/mixs/0000013",
            "depth": "https://w3id.org/mixs/0000018",
            "depth-max": None,
            "depth-min": None,
        }.items():
            build_property(location_props, sample_record, name, prop_id)

        # Geographic location combined
        if region and country:
            location_props.append(
                {
                    "@type": "PropertyValue",
                    "propertyID": "https://w3id.org/mixs/0000010",
                    "name": "geographic location (country and/or sea,region)",
                    "value": f"{country}: , {region}",
                }
            )

        # Sampling design label
        build_property(location_props, sample_record, "sampling design label", None)

        if location_props:
            location["additionalProperty"] = location_props

        action_dict["location"] = location

        # Build instrument array
        instruments = []

        # Sample collection device
        collection_device = self._get_prop_value("sample collection device")
        if collection_device:
            prop = get_property(sample_record, "sample collection device")
            instrument = {
                "@type": "Product",
                "description": "sample collection device",
                "name": collection_device,
            }
            if prop.get("valueReference"):
                instrument["category"] = prop["valueReference"][0].get("@id")
            instruments.append(instrument)

        # Sampling platform
        sampling_platform = self._get_prop_value("sampling platform")
        if sampling_platform:
            instruments.append(
                {
                    "@type": "Product",
                    "name": "sampling platform",
                    "description": sampling_platform,
                }
            )

        if instruments:
            action_dict["instrument"] = instruments

        # Build object array
        objects = []

        for name, prop_id in {
            "environmental medium": "https://w3id.org/mixs/0000014",
            "organism": None,
        }.items():
            build_property(objects, sample_record, name, prop_id)

        # Todo clarify organism
        # weird, unclear if this is to be understood as the object or the result - intuition is to use object, if this was a penguin, I'd assume that the penguin was the object of sampling and not the result

        if objects:
            action_dict["object"] = objects

        # Build actionProcess
        steps = []

        # Filtration step - extract actual values and units from data
        filtration_volume, filt_vol_unit = get_value_with_unit(
            sample_record, "filtration volume"
        )
        filtration_time, filt_time_unit = get_value_with_unit(
            sample_record, "filtration time"
        )

        if filtration_volume or filtration_time:
            text_parts = []
            if filtration_volume:
                text_parts.append(
                    f"filtration volume: {filtration_volume} {filt_vol_unit}"
                )
            if filtration_time:
                text_parts.append(
                    f"filtration time: {filtration_time} {filt_time_unit}"
                )
            if text_parts:
                steps.append(
                    {
                        "@type": "HowToStep",
                        "name": "filtration",
                        "text": ", ".join(text_parts),
                    }
                )

        # Size fractionation step
        lower_threshold, lower_unit = get_value_with_unit(
            sample_record, "size-fraction lower threshold"
        )
        upper_threshold, upper_unit = get_value_with_unit(
            sample_record, "size-fraction upper threshold"
        )
        if lower_threshold and upper_threshold:
            steps.append(
                {
                    "@type": "HowToStep",
                    "name": "size fractionation",
                    "text": f"size fractionation was performed with a lower threshold of {lower_threshold} {lower_unit} and an upper threshold of {upper_threshold} {upper_unit}",
                }
            )

        if steps:
            action_dict["actionProcess"] = {
                "@type": "HowTo",
                "name": f"Submitter-declared sampling steps for sample {self.sample_id}.",
                "description": "The steps in this object are those that have been provided by the submitter of this metadata. They are not ordered and may be incomplete. Please refer to the associated publication and or documentation for more authoritative information.",
                "step": steps,
            }

        # Build participant array
        participants = [
            {
                "@type": "ResearchProject",
                "@id": "https://github.com/DerPlankton13/B5D/blob/main/GeneralSchemas/project_b5d.jsonld",
            }
        ]

        project_name = self._get_prop_value("project name")
        if project_name:
            participants.append({"@type": "ResearchProject", "name": project_name})

        action_dict["participant"] = participants

        # Build additionalProperty array
        additional_props = []

        for name in [
            "checklist",
            "protocol label",
        ]:
            build_property(additional_props, sample_record, name)

        if additional_props:
            action_dict["additionalProperty"] = additional_props

        return action_dict

    def _append_remaining_props(self, schema_dict: dict[str, str]):
        used_props = set(self._used_props)
        remaining_props = [
            p
            for p in self.sample_record["mainEntity"]["additionalProperty"]
            if p.get("name") not in used_props
        ]

        additional_properties = schema_dict.get("additionalProperty", [])
        additional_properties.extend(remaining_props)
        schema_dict["additionalProperty"] = additional_properties

    def build_dicts(self) -> tuple[dict[str, Any], dict[str, Any]]:
        product_dict = self._build_product_dict()
        action_dict = self._build_action_dict()
        self._append_remaining_props(product_dict)
        self._append_remaining_props(action_dict)
        return product_dict, action_dict
