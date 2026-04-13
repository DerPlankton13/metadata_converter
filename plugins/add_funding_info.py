import pandas as pd

from metadata_converter.cleaning_plugin import CleaningPlugin


class AddFundingInfo(CleaningPlugin):
    # add funding and link to project
    def run(self, data: pd.DataFrame):
        data["Grant ID"] = (
            "https://github.com/DerPlankton13/B5D/blob/main/GeneralSchemas/grant_b5d.jsonld"
        )
        data["Project ID"] = (
            "https://github.com/DerPlankton13/B5D/blob/main/GeneralSchemas/project_b5d.jsonld"
        )

        return data
