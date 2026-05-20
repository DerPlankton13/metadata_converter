import logging
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Literal

from metadata_converter.biosamples.schemas import (
    SRA,
    BioSample,
    Checklist,
)

logger = logging.getLogger(__name__)


def get_property(sample_record: dict, prop_name: str) -> dict | None:
    props = sample_record["mainEntity"]["additionalProperty"]

    results = [p for p in props if p.get("name") == prop_name]

    if len(results) == 0:
        logger.debug("Property '%s' not found in sample record", prop_name)
        return None
    if len(results) > 1:
        logger.warning(
            "Ambiguous property '%s': found %d matches, expected 1",
            prop_name,
            len(results),
        )
        logger.debug("The searched sample_record was: %s", sample_record)
        return None

    return results[0].copy()


def get_value(sample_record: dict, prop_name: str) -> str | None:
    """
    Safely extract a property value from the data without raising exceptions.
    Returns None if the property is not found.
    """
    if prop := get_property(sample_record, prop_name):
        try:
            return prop["value"]
        except KeyError:
            logger.debug("Property '%s' has no 'value' key: %s", prop_name, prop)
        except Exception as e:
            logger.error(e)
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
    if prop := get_property(sample_record, prop_name):
        try:
            value = prop["value"]
            unit = prop["unitText"]
        except KeyError:
            logger.debug(
                "Property '%s' is missing 'value' or 'unitText': %s", prop_name, prop
            )
        except Exception as e:
            logger.error(e)
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
        elif "txid" in term_code or "NCBITaxon" in term_code:
            return cls.NCBI
        elif "NERC" in term_code:
            return cls.NERC
        return None

    def normalize_term_code(self, term_code: str) -> str:
        if self == Terminology.NCBI:
            if "txid" in term_code:
                return term_code.split("txid")[-1]
            if "NCBITaxon_" in term_code:
                return term_code.split("NCBITaxon_")[-1]
            return term_code
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
            collection = normalized.split(":")[1]
            concept = normalized.split("::")[-1]
            return self.base_url + collection + "/current/" + concept
        return self.base_url


@dataclass(frozen=True)
class Term:
    name: str
    identifier: str  # normalised term code
    terminology: Terminology

    @property
    def url(self) -> str:
        return self.terminology.build_url(self.identifier)

    @property
    def defined_termset(self) -> str | None:
        return self.terminology.defined_termset

    @classmethod
    def from_value(cls, value: str) -> "Term | None":
        """Parse 'name (TERM:code)' into a Term, or None."""
        matches = re.findall(r"[\[(](.*?)[\])]", value)
        if len(matches) != 1:
            return None
        raw_code = matches[0]
        name = re.split(r"[\[(]", value)[0].strip()
        terminology = Terminology.from_term_code(raw_code)
        if not terminology:
            logger.debug(
                "Could not identify a known terminology from '%s'. Available terminologies: %s",
                value,
                ", ".join(t.name for t in Terminology),
            )
            return None
        if terminology == Terminology.NERC and "::" not in raw_code:
            logger.warning(
                "Invalid NERC term code '%s': expected format 'NERC:<namespace>:<collection>::<concept>' (e.g. 'NERC:SDN:L22::TOOL0412')",
                raw_code,
            )
            return None
        return cls(name=name, identifier=terminology.normalize_term_code(raw_code), terminology=terminology)

    @classmethod
    def from_obo_url(cls, url: str, name: str) -> "Term | None":
        """Parse an OBO purl URL into a Term using the given name, or None."""
        if "/obo/" not in url:
            return None
        obo_name = url.split("/obo/")[-1]
        terminology = Terminology.from_term_code(obo_name)
        if not terminology:
            return None
        return cls(name=name, identifier=terminology.normalize_term_code(obo_name), terminology=terminology)


def build_subject_of(term: Term) -> dict:
    """Build a subjectOf CreativeWork dict from a Term."""
    subject_of: dict = {
        "@type": "CreativeWork",
        "url": term.url,
        "identifier": term.identifier,
        "name": term.name,
    }
    if term.defined_termset:
        subject_of["partOf"] = term.defined_termset
    return subject_of


def build_thing(value: str, reference_url: str | None = None) -> dict:
    """Build a Thing dict with an optional subjectOf from a value string or OBO reference URL."""
    subject_of = None
    additional_type = None
    name = value.strip()

    term = Term.from_value(value)
    if term is None and reference_url:
        term = Term.from_obo_url(reference_url, name)

    if term:
        name = term.name
        subject_of = build_subject_of(term)
        if term.terminology == Terminology.ENVO:
            additional_type = [term.url, name, term.identifier]
    elif reference_url:
        subject_of = {"@type": "CreativeWork", "url": reference_url}

    return {
        "@type": "Thing",
        "additionalType": additional_type,
        "name": name,
        "subjectOf": subject_of,
    }


def build_defined_term(value: str) -> dict[str, str] | None:
    """Build a DefinedTerm dict from a value string containing a bracketed term code, or None."""
    if not (term := Term.from_value(value)):
        return None
    defined_term_dict: dict = {
        "@type": "DefinedTerm",
        "name": term.name,
        "termCode": term.identifier,
        "url": term.url,
    }
    if term.defined_termset:
        defined_term_dict["inDefinedTermSet"] = term.defined_termset
    return defined_term_dict


def build_property(
    sample_record: dict, prop_name: str, prop_id: str | None = None
) -> dict | None:
    prop = get_property(sample_record, prop_name)
    if prop is None:
        return None

    if prop_id:
        prop["propertyID"] = prop_id

    parts = [p.strip() for p in str(prop.get("value", "")).split("|")]
    multi = len(parts) > 1
    if multi:
        prop["value"] = parts

    # try to build value references from the value parts and overwrite any existing ones
    defined_terms = [dt for p in parts if (dt := build_defined_term(p)) is not None]
    if defined_terms:
        prop["valueReference"] = defined_terms if multi else defined_terms[0]
    # otherwise check if there is an existing value reference and clean it up if needed
    elif existing := prop.get("valueReference"):
        entries = existing if isinstance(existing, list) else [existing]
        if not multi and len(entries) > 1:
            raise ValueError(
                f"Expected valueReference to be a dict or single-element list for a single value, "
                f"got list of length {len(entries)} ({entries}) for property '{prop_name}'"
            )
        # remove entries that contain no information beyond @type
        entries = [e for e in entries if any(v for k, v in e.items() if k != "@type")]
        # fix http links
        entries = [{k: convert_to_https(v) for k, v in e.items()} for e in entries]
        if entries:
            prop["valueReference"] = entries if multi else entries[0]
        else:
            prop.pop("valueReference")

    return prop


def convert_to_https(link: str) -> str:
    return link.replace("http://", "https://")


class SampleRecord:
    __slots__ = ("_raw", "sample_id", "_used")

    def __init__(self, raw: dict):
        self._raw = raw
        try:
            self.sample_id = raw["@id"].split(":")[-1]
        except KeyError:
            raise ValueError("The provided input does not contain an '@id' key.")
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

    def _build_additional_property(
        self, extra_prop_names: list[str]
    ) -> list[dict] | dict | None:
        additional_property = []
        if checklist := self._build_checklist():
            additional_property.append(checklist)
        for prop_name in extra_prop_names:
            if prop := self.record.as_property(prop_name):
                additional_property.append(prop)
        if not additional_property:
            return None
        return self._unwrap_single(additional_property)

    def _build_research_project(self) -> list[dict] | dict:
        projects = [
            {
                "@type": "ResearchProject",
                "@id": "https://github.com/DerPlankton13/B5D/blob/main/GeneralSchemas/project_b5d.jsonld",
            }
        ]
        if project_name := self.record["project name"]:
            # do not add the B5D project a second time
            if project_name.lower() not in ["biocean5d", "b5d"]:
                projects.append({"@type": "ResearchProject", "name": project_name})
        return self._unwrap_single(projects)

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

    def build(self) -> dict:
        return {
            "@context": {"@vocab": "https://schema.org/"},
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
            "manufacturer": self._build_research_project(),
            "keywords": self._build_keywords(),
            "additionalProperty": self._build_additional_property(
                ["target analysis type"]
            ),
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
                # also handle multiple values
                parts = [p.strip() for p in str(prop.get("value", "")).split("|")]
                value = parts if len(parts) > 1 else parts[0] if parts else None

                category = None
                if value_reference := prop.get("valueReference"):
                    value_reference = (
                        value_reference
                        if isinstance(value_reference, list)
                        else [value_reference]
                    )
                    ids = [
                        convert_to_https(e.get("@id"))
                        for e in value_reference
                        if e.get("@id")
                    ]
                    category = ids if len(ids) > 1 else ids[0] if ids else None

                instrument.append(
                    {
                        "@type": "Product",
                        "description": prop_name,
                        "name": value,
                        "category": category,
                    }
                )
        return instrument if instrument else None

    def _build_object(self) -> list[dict] | None:
        objects = []
        for prop_name in ["environmental medium", "organism"]:
            prop = self.record.raw_property(prop_name)
            if prop is None:
                continue
            value = prop.get("value", "")
            reference_url = None
            if vr := prop.get("valueReference"):
                entry = vr[0] if isinstance(vr, list) else vr
                reference_url = entry.get("@id")
            objects.append(build_thing(value, reference_url))
        return objects if objects else None

    def _build_action_process(self) -> dict | None:
        step = []

        # Filtration step - extract actual values and units from data
        filtration_params = {
            "filtration volume": self.record.with_unit("filtration volume"),
            "filtration time": self.record.with_unit("filtration time"),
        }
        text_parts = [
            f"{label}: {value} {unit}"
            for label, (value, unit) in filtration_params.items()
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
        size_frac_params = {
            "lower threshold": self.record.with_unit("size-fraction lower threshold"),
            "upper threshold": self.record.with_unit("size-fraction upper threshold"),
        }
        text_parts = [
            f"{label} of {value} {unit}"
            for label, (value, unit) in size_frac_params.items()
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

    def build(self) -> dict:
        return {
            "@context": {"@vocab": "https://schema.org/"},
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
            "participant": self._build_research_project(),
            "additionalProperty": self._build_additional_property(["protocol label"]),
        }


class SampleUplifter:
    def __init__(self, raw: dict):
        self.record = SampleRecord(raw)

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
