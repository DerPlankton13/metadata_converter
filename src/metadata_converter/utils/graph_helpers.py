import pandas as pd
from rdflib import Graph


def load_graph(path: str) -> Graph:
    """Parse a turtle file into a rdflib Graph backed by the Oxigraph store."""
    g = Graph(store="Oxigraph")
    g.parse(path, format="ox-ttl")
    return g


def convert_result_to_pd(result) -> pd.DataFrame:
    """Converts query results to a pandas dataframe."""
    column_names = [str(v) for v in result.vars]
    return pd.DataFrame(data=result, columns=column_names)