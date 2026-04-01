import json

import pytest
from pydantic import ValidationError

from metadata_converter.schema_org_models.schemaorg_models import *

INPUT_FILES = [f"Example{i}.jsonld" for i in range(1, 10)]
OUT_OF_SCOPE = ["Example3.jsonld", "Example7.jsonld"]  # mark these as xfail


@pytest.mark.parametrize("input_file", INPUT_FILES)
def test_creation(input_file):

    # If file is out of scope, mark as expected failure
    if input_file in OUT_OF_SCOPE:
        pytest.xfail(f"{input_file} is out of scope for current implementation")

    with open(f"tests/schema/data/{input_file}", "r") as f:
        definition_dict = json.load(f)

    definition_dict.pop("@context", None)
    model_type = definition_dict["@type"]
    cls = get_schema(model_type)

    try:
        cls(**definition_dict)
    except ValidationError as e:
        print(f"Error in input file {input_file}")
        for error in e.errors():
            print(f"Field: {error['loc']}")
            print(f"Got input: {error.get(input)}")
            print(f"Error: {error['msg']}")
            print("---")
        raise
