from pathlib import Path
from typing import Any

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
from transform_helpers import create_full_names, split_field


def main():
    config = parse_cli()

    # Extract Step
    data_dict = extract_data(config)

    # Transform Step
    for schema_type, data in data_dict.items():
        data = clean_dataframe(data, config.input.cleaning)
        data = add_id(data, schema_type)
        data = convert_to_long(data)
        data_dict[schema_type] = data

    # combine data for the specific types
    # ====== handle Person ======
    data_dict["Person"] = create_full_names(data_dict["Person"])

    # ====== handle main Dataset ======
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
    creators["header"] = "creator_id"
    creators["id"] = "0"
    data_dict["Dataset"] = pd.concat(
        [data_dict["Dataset"], creators], ignore_index=True
    )
    data_dict["Dataset"] = split_field(data_dict["Dataset"], "dataset:keywords")

    # add agents to analysis
    data_dict["Action"] = split_field(data_dict["Action"], "analysis:author-pid")
    p_wide = p.pivot(index="id", columns="header", values="value")

    a = data_dict["Action"]
    agents = a.loc[a.header == "analysis:author-pid"]
    agents["header"] = "agent_id"
    agents["value"] = agents["value"].apply(
        lambda v: p_wide.loc[p_wide["author:pid"] == v, "@id"].values[0]
    )

    # add samples as objects
    s_wide = data_dict["Thing"].pivot(index="id", columns="header", values="value")
    samples = (
        a.loc[a.header == "analysis:pid", ["id", "value"]]
        .merge(
            s_wide[["sample:analysis-pid", "@id"]],
            left_on="value",
            right_on="sample:analysis-pid",
        )
        .drop(columns="value")
        .rename(columns={"@id": "value"})
        .assign(header="sample_id")[["id", "header", "value"]]
    )

    data_dict["Action"] = pd.concat(
        [data_dict["Action"], agents, samples], ignore_index=True
    )

    # create the schemata
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
