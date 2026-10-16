import logging

import pandas as pd
from metadata_converter.flat_data.transform.cleaning_plugin import Plugin
from metadata_converter.flat_data.transform.reshape import split_values
from metadata_converter.utils.hashing import content_hash

logger = logging.getLogger(__name__)

ID_PREFIX = "B5D_file_"


def row_identifier(row: pd.Series) -> str:
    """Create an identifier for a file row: a content hash of the whole row.

    Pandas NA is filtered out first because ``content_hash`` only drops Python
    ``None`` — otherwise the sentinel lands in the hash input.
    """
    values = {k: v for k, v in row.items() if pd.notna(v)}
    return f"{ID_PREFIX}{content_hash(values)}"


def normalize_join_key(values: pd.Series) -> pd.Series:
    """Render a pid column to the form both sides of the join are matched on.

    ``astype(str)`` runs first because ``.str`` yields NA for any non-string
    element, so a numerically-typed pid would drop out of the join with nothing
    logged. ``strip`` then trims padding at the ends of the cell, which the
    delimiter split leaves alone.
    """
    return values.astype(str).str.strip()


class AttachFilesToAnalyses(Plugin):
    """Attach the files each analysis produced to the analysis sheet.

    The source states the relation in the one direction schema.org cannot
    express. Each file row's ``file:analysis`` names the ``analysis:pid`` that
    produced it — but no property on ``Dataset`` says "was produced by this
    Action". The relation is only sayable as ``Action.result``, so it has to
    be turned around before mapping.

    Turning it around means grouping the file sheet by analysis, which the
    mapping grammar cannot express, so the column is built here. From there the
    config treats it as an ordinary multi-value column: the cell is split on
    delimiters, and each piece becomes one ``Dataset`` reference under
    ``Action.result``.

    Those references need something to point at, and the file sheet carries no
    identifier of its own. So each file row gets a content hash in
    ``file:identifier`` — distinct for any two rows that differ — and
    ``analysis:result`` carries the same value. Matching the two sides later is
    then exact, rather than a guess at names or URLs.
    """

    def run(self, data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        file = data["file"].copy()
        analysis = data["analysis"].copy()

        # A blank file:analysis does not allow linking to Action. If the sheet holds
        # exactly one analysis row, we assume it generated all files.
        blank = file["file:analysis"].isna()
        if blank.any():
            if len(analysis) == 1:
                # an all-blank column arrives as float64; assigning a string into
                # it without this cast is deprecated and will raise in later pandas
                file["file:analysis"] = file["file:analysis"].astype(object)
                file.loc[blank, "file:analysis"] = analysis["analysis:pid"].iloc[0]
            else:
                logger.warning(
                    "%d of %d file rows have no file:analysis in a sheet declaring "
                    "%d analyses; leaving them unlinked",
                    int(blank.sum()),
                    len(file),
                    len(analysis),
                )

        file["file:identifier"] = [row_identifier(row) for _, row in file.iterrows()]

        # a file may name several analyses; but we need to invert that relation
        # we need to know which analysis produced which files
        links = file.dropna(subset=["file:analysis"])[
            ["file:analysis", "file:identifier"]
        ].copy()
        # converts a string with separators into a list of parts
        links["file:analysis"] = split_values(links["file:analysis"])
        # puts each list element into a row, so that groupby allows collecting
        # all file identifiers per analysis
        links = links.explode("file:analysis")
        links["file:analysis"] = normalize_join_key(links["file:analysis"])
        # removes possible left-overs from the splitting, e.g. originating from ",,"
        links = links[links["file:analysis"] != ""]
        # generate the desired file identifier per analysis mapping
        by_analysis = links.groupby("file:analysis")["file:identifier"].apply(
            lambda s: ",".join(sorted(s.unique()))
        )
        # generate the new column by mapping via the pid
        analysis["analysis:result"] = normalize_join_key(analysis["analysis:pid"]).map(
            by_analysis
        )

        # the map above reads from the declared pids, so a file naming an analysis
        # this sheet does not declare is already excluded -- it would just vanish
        known = set(normalize_join_key(analysis["analysis:pid"].dropna()))
        orphan = ~links["file:analysis"].isin(known)
        if orphan.any():
            logger.warning(
                "%d file reference(s) name an analysis that this sheet does not "
                "declare and stay unlinked: %s (declared: %s)",
                int(orphan.sum()),
                sorted(links.loc[orphan, "file:analysis"].unique()),
                sorted(known),
            )

        data["file"] = file
        data["analysis"] = analysis
        return data
