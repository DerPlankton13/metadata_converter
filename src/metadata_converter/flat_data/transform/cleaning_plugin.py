import importlib.util
import inspect
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd


class Plugin(ABC):
    """User-supplied step that operates on the full dataset before built-in cleaning.

    Subclass this to implement a fix that the built-in cleaning steps can't
    express — e.g. a structural repair that reads one sheet to populate another,
    or a per-sheet transform that needs to know its sheet name. Plugins receive
    the whole ``{sheet_name: dataframe}`` dict and may modify any sheet.

    Examples
    --------
    >>> class FillX(Plugin):
    ...     def run(self, data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    ...         data["target"]["x"] = data["source"]["x"]
    ...         return data
    """

    @abstractmethod
    def run(self, data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        """Apply the plugin to the whole dataset."""
        ...


def load_plugins(
    plugin_dir: str | Path, plugin_name: str | list[str] | None = None
) -> list[Plugin]:
    """Discover and instantiate all ``Plugin`` subclasses in a directory.

    Scans Python files in ``plugin_dir`` for subclasses of ``Plugin`` and
    returns one instance of each. Files are processed in alphabetical order;
    within a file, classes are instantiated in alphabetical order by class name.

    Raises
    ------
    NotADirectoryError
        If ``plugin_dir`` does not exist or is not a directory.
    ImportError
        If a plugin file fails to load, or contains no ``Plugin`` subclasses.
    """
    plugin_dir = Path(plugin_dir)
    if not plugin_dir.is_dir():
        raise NotADirectoryError(f"Plugin directory not found: {plugin_dir}")

    if not isinstance(plugin_name, list):
        plugin_name = [plugin_name]

    plugins = []
    for path in sorted(plugin_dir.glob("*.py")):
        if path.name not in plugin_name:
            continue
        spec = importlib.util.spec_from_file_location(path.stem, path)
        module = importlib.util.module_from_spec(spec)

        try:
            spec.loader.exec_module(module)
        except Exception as e:
            raise ImportError(f"Failed to load plugin file {path.name}: {e}") from e

        found = [
            cls
            for _, cls in inspect.getmembers(module, inspect.isclass)
            if issubclass(cls, Plugin) and cls is not Plugin
        ]

        if not found:
            raise ImportError(
                f"{path.name} does not contain any `Plugin` subclasses"
            )

        plugins.extend(cls() for cls in found)

    return plugins
