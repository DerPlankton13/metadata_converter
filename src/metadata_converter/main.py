from pathlib import Path

import pandas as pd

from metadata_converter.extract import extract_data
from metadata_converter.load import load_to_jsonld
from metadata_converter.parse import parse_cli
from metadata_converter.schema_org_models.schemaorg_models import Person
from metadata_converter.transform import (
    add_id,
    clean_dataframe,
    convert_to_long,
    extract_schemas,
)

SPLIT_AUTHORS = False


def main():
    config = parse_cli()

    # Extract Step
    data_dict = extract_data(config)

    # Transform Step
    for schema_type, data in data_dict.items():
        data = clean_dataframe(data, config.input.cleaning)
        data = add_id(data, schema_type)
        data = convert_to_long(data)

        # split the authors
        if SPLIT_AUTHORS:
            fields_to_split = ["Authors"]
            for header in fields_to_split:
                field_df = data[data.header == header].copy()
                non_field_df = data[data.header != header]

                field_df.value = field_df.value.str.split(r"\s*[,;&]\s*|\s+and\s+")
                exploded_df = field_df.explode("value")
                data = pd.concat([non_field_df, exploded_df], ignore_index=True)

        data_dict[schema_type] = data

    # combine data for the specific types
    # handle Person
    def combine_names(group):
        first = group.loc[group.header == "author:first-name", "value"].values[0]
        last = group.loc[group.header == "author:last-name", "value"].values[0]
        return pd.Series({"header": "name", "value": f"{first} {last}"})

    full_names = data_dict["Person"].groupby("id").apply(combine_names).reset_index()
    data_dict["Person"] = pd.concat(
        [data_dict["Person"], full_names], ignore_index=True
    )

    # add the people as creators
    p = data_dict["Person"]
    creator_ids = p[(p.header == "author:is-dataset-author") & (p.value == 1)].id
    creators = pd.DataFrame(
        [
            p.loc[(p.id == id) & (p.header == "@id")].value.values[0]
            for id in creator_ids
        ],
        columns=["value"],
    )
    creators["header"] = "creator"
    creators["id"] = "0"
    data_dict["Dataset"] = pd.concat(
        [data_dict["Dataset"], creators], ignore_index=True
    )

    # create the schema
    results = {}
    for schema_type, data in data_dict.items():
        results[schema_type] = extract_schemas(
            data, schema_type, config.mapping[schema_type]
        )

    # Load Step
    for schema in [s for schemas in results.values() for s in schemas]:
        load_to_jsonld(schema, output_path=Path("output"))


if __name__ == "__main__":
    main()
