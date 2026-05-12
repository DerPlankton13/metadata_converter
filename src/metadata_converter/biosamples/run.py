import json

import pandas as pd

from metadata_converter.biosamples.fetch import get_metadata
from metadata_converter.config import BiosamplesConfig


def run_biosamples_extraction(config: BiosamplesConfig):
    input_cfg = config.input
    excel_files = sorted(input_cfg.input_path.glob("*.xlsx")) + sorted(input_cfg.input_path.glob("*.xls"))
    for excel_file in excel_files:
        df = pd.read_excel(
            excel_file,
            sheet_name=input_cfg.sheet_name,
            header=input_cfg.header,
            skiprows=input_cfg.skiprows,
        )
        df = df.dropna(how="all")

        if input_cfg.header_name not in df.columns:
            raise KeyError(
                f"Column '{input_cfg.header_name}' not found in sheet '{input_cfg.sheet_name}' of '{excel_file.name}'"
            )
        sample_ids = set(df[input_cfg.header_name].dropna().tolist())

        for sample_id in sample_ids:
            try:
                metadata = get_metadata(sample_id)
            except Exception as e:
                print(
                    f"Could not extract metadata for sample '{sample_id}' read from '{excel_file.name}'."
                )
                print(e)
                continue

            # fix the schema.org context to be in @vocab and use https to avoid issues with rdflib downloading stuff
            context = metadata.get("@context")
            if not context:
                print(f"Warning: No '@context' found for sample {sample_id}.")
            else:
                try:
                    terms = context[1]
                    context = {"@vocab": "https://schema.org", **terms}
                    metadata["@context"] = context
                except (IndexError, TypeError):
                    print("Got unexpected context: ", context)

            output_path = config.output.output_path / f"raw/{sample_id}.jsonld"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            jsonld_str = json.dumps(metadata, indent=2, ensure_ascii=False, default=str)
            output_path.write_text(jsonld_str, encoding="utf-8")
