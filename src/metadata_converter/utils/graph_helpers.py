import pandas as pd
import pyoxigraph as o
from rdflib import Graph


def load_graph(path: str) -> Graph:
    """Parse a turtle file into a rdflib Graph backed by the Oxigraph store.

    Since currently using Oxigraph as a backend does not read the prefixes,
    these are manually read and bound to the resulting graph.
    """

    parser = o.parse(path=path, format=o.RdfFormat.TURTLE)
    # we need to go to the first triple to actually load the prefixes, as the parser
    # is lazy; return None on empty iterator instead of failing
    next(parser, None)

    g = Graph(store="Oxigraph")
    g.parse(path, format="ox-ttl")

    # if the parser found any prefixes, we bind them now
    # we need to override, otherwise rdflib defaults are used
    for prefix, namespace in getattr(parser, "prefixes", {}).items():
        g.bind(prefix, namespace, override=True)

    return g


def convert_result_to_pd(result) -> pd.DataFrame:
    """Converts query results to a pandas dataframe."""
    column_names = [str(v) for v in result.vars]
    return pd.DataFrame(data=result, columns=column_names)