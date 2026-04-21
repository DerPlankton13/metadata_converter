from typing import Any

import requests

from metadata_converter.schema_org_models.custom_models import SRA, BioSample
from metadata_converter.schema_org_models.schemaorg_models import PropertyValue


def fetch_metadata(url: str) -> dict:
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # raises an exception for 4xx/5xx status codes
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error: {e}")
    except requests.exceptions.ConnectionError:
        print("Could not connect")
    except requests.exceptions.Timeout:
        print("Request timed out")


def fuse_metadata(structured_metadata: dict, unstructured_metadata: dict) -> dict:
    characteristics = unstructured_metadata.get("characteristics", {})

    for prop in structured_metadata["mainEntity"]["additionalProperty"]:
        name = prop["name"]
        if name in characteristics and "unit" in characteristics[name][0]:
            if len(characteristics[name]) > 1:
                raise RuntimeError(
                    "Could not fuse the data, several entries were found for {name} in the unstructured metadata."
                )
            prop["unitText"] = characteristics[name][0]["unit"]

    return structured_metadata


def get_metadata(sample_id: str) -> dict:

    base_url = f"https://www.ebi.ac.uk/biosamples/samples/{sample_id}"
    structured_metadata = fetch_metadata(base_url + ".ldjson")
    unstructured_metadata = fetch_metadata(base_url + ".json")

    return fuse_metadata(structured_metadata, unstructured_metadata)


def find_property_value(data: dict, query: str) -> Any:

    props = data["mainEntity"]["additionalProperty"]

    results = [p for p in props if p.get("name") == query]

    if len(results) != 1:
        raise Exception()  # f"No unique match found for {query} within {data}")

    return results[0]


def safe_extract(data: dict, query: str, default=None) -> Any:
    """
    Safely extract a property value from the data without raising exceptions.
    Returns the default value if the property is not found.
    """
    try:
        return find_property_value(data, query)["value"]
    except (Exception, KeyError, IndexError):
        return default


def safe_extract_with_unit(
    data: dict, query: str, default_value=None, default_unit="Unit unknown"
) -> tuple[Any, Any]:
    """
    Safely extract a property value and its unit from the data without raising exceptions.
    Returns the default values if the property is not found.

    Returns
    -------
    tuple[Any, Any]
        (value, unit) tuple
    """
    try:
        prop = find_property_value(data, query)
        return prop["value"], prop.get("unitText", default_unit)
    except (Exception, KeyError, IndexError):
        return default_value, default_unit


def extract_sample(data: dict, sample_id: str) -> dict:
    identifier_list = [
        BioSample(value=sample_id).model_dump(by_alias=True, exclude_none=True),
    ]

    sra_accession = safe_extract(data, "SRA accession")
    if sra_accession:
        identifier_list.append(
            SRA(value=sra_accession).model_dump(by_alias=True, exclude_none=True)
        )

    sampling_design = safe_extract(data, "sampling design label")
    if sampling_design:
        identifier_list.append(
            PropertyValue(
                name="sampling design label",
                propertyID="sampling design label",
                value=sampling_design,
            ).model_dump(by_alias=True, exclude_none=True)
        )

    sample_dict = {
        "@context": {"@vocab": "http://schema.org"},
        "@type": "Product",
        "additionalType": [
            "sample",
            "http://purl.obolibrary.org/obo/OBI_0000747",
        ],
        "@id": f"Product_{sample_id}.jsonld",
        "identifier": identifier_list,
    }

    # Add optional fields only if data exists
    if data["mainEntity"].get("name"):
        sample_dict["name"] = data["mainEntity"]["name"]

    description = safe_extract(data, "sample description")
    if description:
        sample_dict["description"] = description

    if data["mainEntity"].get("sameAs"):
        sample_dict["subjectOf"] = data["mainEntity"]["sameAs"]

    if data["mainEntity"].get("url"):
        sample_dict["url"] = data["mainEntity"]["url"]

    production_date = safe_extract(data, "collection date")
    if production_date:
        sample_dict["productionDate"] = production_date

    material = safe_extract(data, "environmental medium")
    if material:
        sample_dict["material"] = material

    country = safe_extract(data, "geographic location (country and/or sea)")
    if country:
        sample_dict["countryOfOrigin"] = country

    sample_dict["funding"] = {
        "@type": "MonetaryGrant",
        "@id": "https://github.com/DerPlankton13/B5D/blob/main/GeneralSchemas/grant_b5d.jsonld",
    }

    sample_dict["manufacturer"] = [
        {
            "@type": "ResearchProject",
            "@id": "https://github.com/DerPlankton13/B5D/blob/main/GeneralSchemas/project_b5d.jsonld",
        }
    ]

    project_name = safe_extract(data, "project name")
    if project_name:
        sample_dict["manufacturer"].append(
            {"@type": "ResearchProject", "name": project_name}
        )

    keywords = [
        k
        for k in [
            safe_extract(data, "organism"),
            safe_extract(data, "target analysis type"),
            safe_extract(data, "local environmental context"),
        ]
        if k is not None
    ]
    if keywords:
        sample_dict["keywords"] = keywords

    checklist = safe_extract(data, "checklist")
    if checklist:
        sample_dict["additionalProperty"] = {
            "@type": "PropertyValue",
            "name": "checklist",
            "value": checklist,
        }

    return sample_dict


def extract_sampling_action(data: dict, sample_id: str) -> dict:
    """
    Extract action data from biosamples metadata and convert to Action.jsonld format.

    Parameters
    ----------
    data : dict
        The metadata from BioSamples API
    sample_id : str
        The sample ID (e.g., SAMEA112489011)

    Returns
    -------
    dict
        A dictionary representing the Action in JSON-LD format
    """
    action_dict = {
        "@context": {"@vocab": "http://schema.org"},
        "@type": "Action",
        "additionalType": [
            "sampling process",
            "http://purl.obolibrary.org/obo/OBI_0000744",
        ],
        "@id": f"Action_{sample_id}.jsonld",
        "name": f"Sampling process for sample {sample_id}",
        "result": f"Product_{sample_id}.jsonld",
    }

    # Add startTime from collection date
    collection_date = safe_extract(data, "collection date")
    if collection_date:
        action_dict["startTime"] = collection_date

    # Build location object
    location = {
        "@type": "Place",
    }

    # Location name from region and country
    region = safe_extract(data, "geographic location (region and locality)")
    country = safe_extract(data, "geographic location (country and/or sea)")
    if region and country:
        location["name"] = f"{region}, {country}"
    elif region:
        location["name"] = region
    elif country:
        location["name"] = country

    # Geo coordinates
    latitude, lat_unit = safe_extract_with_unit(data, "geographic location (latitude)")
    longitude, lon_unit = safe_extract_with_unit(
        data, "geographic location (longitude)"
    )
    elevation, elev_unit = safe_extract_with_unit(data, "elevation")

    if latitude or longitude or elevation:
        geo = {"@type": "GeoCoordinates"}
        if latitude:
            geo["latitude"] = f"{latitude} {lat_unit}"
        if longitude:
            geo["longitude"] = f"{longitude} {lon_unit}"
        if elevation:
            geo["elevation"] = f"{elevation} {elev_unit}"
        location["geo"] = geo

    # Location additional properties
    location_props = []

    # Broad-scale environmental context
    broad_context = safe_extract(data, "broad-scale environmental context")
    if broad_context:
        prop = find_property_value(data, "broad-scale environmental context")
        value_ref = (
            prop.get("valueReference", [{}])[0] if prop.get("valueReference") else {}
        )
        location_props.append(
            {
                "@type": "PropertyValue",
                "name": "broad-scale environmental context",
                "value": broad_context,
                "propertyID": "https://w3id.org/mixs/0000012",
                "valueReference": {
                    "@type": "DefinedTerm",
                    "identifier": value_ref.get(
                        "@id", "http://purl.obolibrary.org/obo/ENVO_00000304"
                    ),
                    "name": "shore",
                    "inDefinedTermSet": "http://purl.obolibrary.org/obo/envo.owl",
                    "termCode": "ENVO:00000304",
                }
                if value_ref
                else None,
            }
        )

    # Local environmental context
    local_context = safe_extract(data, "local environmental context")
    if local_context:
        location_props.append(
            {
                "@type": "PropertyValue",
                "name": "local environmental context",
                "value": local_context,
                "propertyID": "https://w3id.org/mixs/0000013",
            }
        )

    # Depth information
    depth, depth_unit = safe_extract_with_unit(data, "depth")
    if depth:
        location_props.append(
            {
                "@type": "PropertyValue",
                "name": "depth",
                "value": depth,
                "unitText": depth_unit,
                "propertyID": "https://w3id.org/mixs/0000018",
            }
        )

    depth_max, depth_max_unit = safe_extract_with_unit(data, "depth-max")
    if depth_max:
        location_props.append(
            {
                "@type": "PropertyValue",
                "name": "depth-max",
                "value": depth_max,
                "unitText": depth_max_unit,
            }
        )

    depth_min, depth_min_unit = safe_extract_with_unit(data, "depth-min")
    if depth_min:
        location_props.append(
            {
                "@type": "PropertyValue",
                "name": "depth-min",
                "value": depth_min,
                "unitText": depth_min_unit,
            }
        )

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

    if location_props:
        location["additionalProperty"] = location_props

    if (
        location.get("name")
        or location.get("geo")
        or location.get("additionalProperty")
    ):
        action_dict["location"] = location

    # Build instrument array
    instruments = []

    # Sample collection device
    collection_device = safe_extract(data, "sample collection device")
    if collection_device:
        prop = find_property_value(data, "sample collection device")
        instrument = {
            "@type": "Product",
            "description": "sample collection device",
            "name": collection_device,
        }
        if prop.get("valueReference"):
            instrument["category"] = prop["valueReference"][0].get("@id")
        instruments.append(instrument)

    # Sampling platform
    sampling_platform = safe_extract(data, "sampling platform")
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

    # Environmental medium
    env_medium = safe_extract(data, "environmental medium")
    if env_medium:
        prop = find_property_value(data, "environmental medium")
        obj = {
            "@type": "PropertyValue",
            "name": "environmental medium",
            "value": env_medium,
            "propertyID": "https://w3id.org/mixs/0000014",
        }
        if prop.get("valueReference"):
            obj["valueReference"] = {
                "@type": "DefinedTerm",
                "identifier": prop["valueReference"][0].get(
                    "@id", "http://purl.obolibrary.org/obo/ENVO_01001964"
                ),
                "inDefinedTermSet": "http://purl.obolibrary.org/obo/envo.owl",
                "termCode": "ENVO:01001964",
                "name": "seawater",
            }
        objects.append(obj)

    # Organism
    organism = safe_extract(data, "organism")
    if organism:
        prop = find_property_value(data, "organism")
        obj = {
            "@type": "PropertyValue",
            "name": "organism",
            "value": organism,
            "ambiguityDescription": "weird, unclear if this is to be understood as the object or the result - intuition is to use object, if this was a penguin, I'd assume that the penguin was the object of sampling and not the result",
        }
        if prop.get("valueReference"):
            obj["valueReference"] = {
                "@type": "DefinedTerm",
                "identifier": prop["valueReference"][0].get(
                    "@id",
                    "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=408172",
                ),
                "name": "marine metagenome",
            }
        objects.append(obj)

    if objects:
        action_dict["object"] = objects

    # Build actionProcess
    steps = []

    # Filtration step - extract actual values and units from data
    filtration_volume, filt_vol_unit = safe_extract_with_unit(data, "filtration volume")
    filtration_time, filt_time_unit = safe_extract_with_unit(data, "filtration time")

    if filtration_volume or filtration_time:
        text_parts = []
        if filtration_volume:
            text_parts.append(f"filtration volume: {filtration_volume} {filt_vol_unit}")
        if filtration_time:
            text_parts.append(f"filtration time: {filtration_time} {filt_time_unit}")
        if text_parts:
            steps.append(
                {
                    "@type": "HowToStep",
                    "name": "filtration",
                    "text": ", ".join(text_parts),
                }
            )

    # Size fractionation step
    lower_threshold, lower_unit = safe_extract_with_unit(
        data, "size-fraction lower threshold"
    )
    upper_threshold, upper_unit = safe_extract_with_unit(
        data, "size-fraction upper threshold"
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
            "name": "Sampling ...",
            "step": steps,
        }

    # Build participant array
    participants = [
        {
            "@type": "ResearchProject",
            "@id": "https://github.com/DerPlankton13/B5D/blob/main/GeneralSchemas/project_b5d.jsonld",
        }
    ]

    project_name = safe_extract(data, "project name")
    if project_name:
        participants.append({"@type": "ResearchProject", "name": project_name})

    action_dict["participant"] = participants

    # Build additionalProperty array
    additional_props = []

    checklist = safe_extract(data, "checklist")
    if checklist:
        additional_props.append(
            {"@type": "PropertyValue", "name": "checklist", "value": checklist}
        )

    protocol_label = safe_extract(data, "protocol label")
    if protocol_label:
        additional_props.append(
            {
                "@type": "PropertyValue",
                "name": "protocol label",
                "value": protocol_label,
            }
        )

    sampling_design = safe_extract(data, "sampling design label")
    if sampling_design:
        additional_props.append(
            {
                "@type": "PropertyValue",
                "name": "sampling design label",
                "value": sampling_design,
            }
        )

    target_analysis = safe_extract(data, "target analysis type")
    if target_analysis:
        additional_props.append(
            {
                "@type": "PropertyValue",
                "name": "target analysis type",
                "value": target_analysis,
            }
        )

    if additional_props:
        action_dict["additionalProperty"] = additional_props

    return action_dict


if __name__ == "__main__":
    sample_id = "SAMEA112489011"
    data = get_metadata(sample_id)
    extract_sample(data, sample_id)
