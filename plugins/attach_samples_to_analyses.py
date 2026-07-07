import pandas as pd

from metadata_converter.flat_data.transform.cleaning_plugin import Plugin


class AttachSamplesToAnalyses(Plugin):
    """Attach the corresponding samples to each row of the analysis sheet.

    The source data carries the (sample, analysis) relation only as co-located
    rows in the sample sheet — each row pairs one ``sample:pid`` with one
    ``sample:analysis-pid``. The analysis sheet has no direct column for the
    samples it covers. This plugin materializes that column so downstream
    mapping can populate ``Action.object`` directly without needing a stub
    Product entity to mediate the link.

    For each analysis row, the new ``analysis:sample`` cell holds the comma-
    delimited, de-duplicated list of sample PIDs whose ``sample:analysis-pid``
    matches the analysis's ``analysis:pid``. The downstream ``split_fields``
    step then splits this into one value per row in long format.

    The sample sheet itself is not consumed here — ``clean`` drops it later
    because it has no mapping entry, leaving the analysis sheet as the only
    surviving carrier of the (sample, analysis) relation.
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
