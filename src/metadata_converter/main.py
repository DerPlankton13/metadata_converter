from pathlib import Path
from typing import Any

from metadata_converter.config import (
    FlatDataConfig,
    MetadataCollectorConfig,
)
from metadata_converter.extract import extract_data
from metadata_converter.flat_data.preprocess_datahub import preprocess_datahub
from metadata_converter.flat_data.transform import (
    add_id,
    clean_dataframe,
    convert_to_long,
    extract_schemas,
)
from metadata_converter.linked_data.metadata_collector import fetch_jsonld, query_source
from metadata_converter.load import load_to_jsonld
from metadata_converter.parse import parse_cli
from metadata_converter.schema_org_models.custom_models import get_schema
from metadata_converter.schema_org_models.schemaorg_models import SchemaOrgBase


def main():
    config = parse_cli()

    if isinstance(config, FlatDataConfig):
        # Extract Step
        data_dict = extract_data(config)

        # Transform Step
        for name, data in data_dict.items():
            data = clean_dataframe(data, config.cleaning)
            data = add_id(data, config.mapping[name]["type"])
            data = convert_to_long(data)
            data_dict[name] = data

        data_dict = preprocess_datahub(data_dict)

        # create the schemata
        results = {}
        for name, data in data_dict.items():
            results[name] = extract_schemas(data, config.mapping[name])

        # Load Step
        for schema in [s for schemas in results.values() for s in schemas]:
            load_to_jsonld(schema, output_path=config.output.output_path)

    elif isinstance(config, MetadataCollectorConfig):
        # Extract Step
        records = query_source(config.extractor)
        results: dict[str, SchemaOrgBase] = {}
        for record in records:
            jsonld = fetch_jsonld(record, config.extractor)

            # Transform Step
            # currently just check that matching schem.org model can be generated
            try:
                results[record.doi] = get_schema(jsonld["@type"])(**jsonld)
            except Exception as e:
                print("Failed to extract schema for DOI:", record.doi)
                print(e)

        # Todo: Standardise files
        # Todo: uplift the data

        # Load Step
        for key, schema in results.items():
            load_to_jsonld(schema, output_path=config.output.output_path)


if __name__ == "__main__":
    main()
