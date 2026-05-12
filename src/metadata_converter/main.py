import json

from metadata_converter.biosamples.run import run_biosamples_extraction
from metadata_converter.config import (
    BiosamplesConfig,
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
        from pydantic import ValidationError

        def print_validation_error(error: ValidationError):
            # calculate the depth of the error
            error_list = list(
                map(lambda e: {**e, "depth": len(e["loc"])}, error.errors())
            )

            # use the deepest errors, as they contain the most essential information
            max_depth = max(e["depth"] for e in error_list)
            deepest_errors = [e for e in error_list if e["depth"] == max_depth]

            # assumes that the deepest errors all correspond to the same input
            # from the loc structure, the first entry should be the parent structure, where it failed
            # and the second to entry the failed property
            print(
                f"Validation error in '{deepest_errors[0]['loc'][0]}' for property: {deepest_errors[0]['loc'][-2]}"
            )
            print("The following errors occurred:")
            for e in deepest_errors:
                print(" - ", e["msg"])
            print(f"The input was: {deepest_errors[0]['input']}")

        results: dict[str, SchemaOrgBase] = {}
        raw_output_path = config.output.output_path / "raw"
        raw_output_path.mkdir(parents=True, exist_ok=True)
        # Extract Step
        records = query_source(config.extractor)
        print(raw_output_path)
        for record in records:
            jsonld = fetch_jsonld(record, config.extractor)

            # write raw jsonfiles
            output_path = raw_output_path / f"{record.source_id}.jsonld"
            print(output_path)
            output_path.write_text(
                json.dumps(jsonld, indent=2, ensure_ascii=False, default=str),
                encoding="utf-8",
            )

            # Transform Step
            # currently just check that matching schema.org model can be generated
            try:
                schema_type = jsonld["@type"].split("/")[-1]
                results[record.doi] = get_schema(schema_type)(**jsonld)
            except Exception as e:
                print("Failed to extract schema for DOI:", record.doi)
                print_validation_error(e)

        # Todo: Standardise files
        # Todo: uplift the data

        # Load Step
        for key, schema in results.items():
            load_to_jsonld(schema, output_path=config.output.output_path)

    elif isinstance(config, BiosamplesConfig):
        run_biosamples_extraction(config)

if __name__ == "__main__":
    main()
