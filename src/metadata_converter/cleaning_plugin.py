import importlib.util
import inspect
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd


class CleaningPlugin(ABC):
    """
    Abstract base class for dataframe cleaning plugins.

    Subclass this to implement a custom cleaning step that can be
    loaded dynamically from a plugin directory. Each plugin file
    may contain one or more subclasses of this class.

    Examples
    --------
    >>> class MyPlugin(CleaningPlugin):
    ...     def run(self, df: pd.DataFrame) -> pd.DataFrame:
    ...         return df.dropna(subset=["my_col"])
    """

    @abstractmethod
    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Execute the cleaning step.

        Parameters
        ----------
        df : pd.DataFrame
            The dataframe to clean.

        Returns
        -------
        pd.DataFrame
            The cleaned dataframe.
        """
        ...


def load_plugins(plugin_dir: str | Path) -> list[CleaningPlugin]:
    """
    Discover and instantiate all cleaning plugins in a directory.

    Scans all Python files in ``plugin_dir`` for subclasses of
    ``CleaningPlugin`` and returns one instance of each. Multiple
    plugin classes may be defined in a single file. Files are
    processed in alphabetical order; within a file, classes are
    instantiated in alphabetical order by class name.

    Parameters
    ----------
    plugin_dir : str or Path
        Path to the directory containing plugin files.

    Returns
    -------
    list of CleaningPlugin
        Instantiated plugin objects in discovery order.

    Raises
    ------
    NotADirectoryError
        If ``plugin_dir`` does not exist or is not a directory.
    ImportError
        If a plugin file fails to load, or if a Python file in
        ``plugin_dir`` contains no ``CleaningPlugin`` subclasses.

    Examples
    --------
    >>> plugins = load_plugins("plugins/")
    >>> config = CleaningConfig(plugins=plugins)
    """
    plugin_dir = Path(plugin_dir)
    if not plugin_dir.is_dir():
        raise NotADirectoryError(f"Plugin directory not found: {plugin_dir}")

    plugins = []
    for path in sorted(plugin_dir.glob("*.py")):
        spec = importlib.util.spec_from_file_location(path.stem, path)
        module = importlib.util.module_from_spec(spec)

        try:
            spec.loader.exec_module(module)
        except Exception as e:
            raise ImportError(f"Failed to load plugin file {path.name}: {e}") from e

        found = [
            cls
            for _, cls in inspect.getmembers(module, inspect.isclass)
            if issubclass(cls, CleaningPlugin) and cls is not CleaningPlugin
        ]

        if not found:
            raise ImportError(
                f"{path.name} does not contain any `CleaningPlugin` subclasses"
            )

        plugins.extend(cls() for cls in found)

    return plugins
