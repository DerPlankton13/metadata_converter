import pandas as pd

from metadata_converter.flat_data.transform.cleaning_plugin import Plugin


class FillAnalysisSamples(Plugin):
    """Populate ``analysis:sample`` on the analysis sheet from the sample sheet.

    The source data carries the (sample, analysis) relation only as co-located
    rows in the sample sheet — each row pairs one ``sample:pid`` with one
    ``sample:analysis-pid``. The analysis sheet has no direct column for the
    samples it covers. This plugin materializes that column so downstream
    mapping can populate ``Action.object`` directly without needing a stub
    Product entity to mediate the link.

    For each analysis row, the cell is filled with the comma-delimited,
    de-duplicated list of sample PIDs whose ``sample:analysis-pid`` matches the
    analysis's ``analysis:pid``. The downstream ``split_fields`` step then
    splits this into one value per row in long format.
    """

    def run(self, data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        sample = data["sample"]
        analysis = data["analysis"].copy()
        by_analysis = (
            sample.dropna(subset=["sample:analysis-pid", "sample:pid"])
            .groupby("sample:analysis-pid")["sample:pid"]
            .apply(lambda s: ",".join(sorted(s.unique())))
        )
        analysis["analysis:sample"] = analysis["analysis:pid"].map(by_analysis)
        data["analysis"] = analysis
        return data
