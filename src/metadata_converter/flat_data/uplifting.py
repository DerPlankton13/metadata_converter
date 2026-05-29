"""Datahub cross-reference linker.

Reads ingested flat_data JSON-LD (one file per entity row, no cross-refs resolved),
resolves the links between Persons, the main Dataset, Actions, sample stubs and file
Datasets, and writes linked JSON-LD to the uplift output directory.

Linking model (see plan / preprocess_datahub for the original cross-sheet semantics):

- ``Dataset.creator``  ← Persons whose ``additionalProperty[name="author:is-dataset-author"]``
  is truthy (1 / "1" / True / "true").
- ``Action.agent``     ← Person whose ``identifier.value`` matches the Action's
  ``agent.identifier`` (forward).
- ``Action.object``    ← Product stubs whose ``additionalProperty[name="sample:analysis-pid"]``
  matches the Action's ``identifier`` (reverse).
- ``Action.result``    ← file Datasets whose ``additionalProperty[name="file:analysis"]``
  matches the Action's ``identifier`` (reverse).
- file ``Dataset.about`` ← biosamples Product ``Product_<sample:pid>.jsonld`` reference
  derived from the file's ``about.identifier`` (forward, deterministic — no scan needed).

Sample stubs are NOT copied to the uplifted directory; they are placeholders whose
``@id`` namespace is owned by the biosamples uplift output.
"""
import json
import logging
from pathlib import Path
from typing import Any, Iterable

from metadata_converter.config import SourceConfig
from metadata_converter.io import write_json

logger = logging.getLogger(__name__)


_IS_DATASET_AUTHOR = "author:is-dataset-author"
_SAMPLE_ANALYSIS_PID = "sample:analysis-pid"
_FILE_ANALYSIS = "file:analysis"


def _as_list(value: Any) -> list:
    """Wrap a scalar in a list; pass lists through; treat None as empty."""
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _truthy(value: Any) -> bool:
    """Loosely interpret 1 / "1" / True / "true" as truthy."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return False


def _find_additional_property(entity: dict, name: str) -> dict | None:
    """Return the first additionalProperty entry with the given name, or None."""
    for ap in _as_list(entity.get("additionalProperty")):
        if isinstance(ap, dict) and ap.get("name") == name:
            return ap
    return None


def _has_additional_property(entity: dict, name: str) -> bool:
    return _find_additional_property(entity, name) is not None


def _identifier_value(identifier: Any) -> str | None:
    """Extract a PID from an identifier that may be a string, a PropertyValue dict, or a list."""
    if identifier is None:
        return None
    if isinstance(identifier, str):
        return identifier
    if isinstance(identifier, dict):
        val = identifier.get("value")
        return str(val) if val is not None else None
    if isinstance(identifier, list):
        for item in identifier:
            v = _identifier_value(item)
            if v is not None:
                return v
    return None


class DatahubLinker:
    def __init__(self, config: SourceConfig):
        self.input_path = Path(config.input_path)
        self.output_path = Path(config.output_path)
        # Loaded entities, keyed by source filename.
        self._entities: dict[str, dict] = {}
        # Buckets by @type / role.
        self._persons: list[dict] = []
        self._main_datasets: list[dict] = []
        self._file_datasets: list[dict] = []
        self._actions: list[dict] = []
        self._sample_stubs: list[dict] = []
        # Lookups built in _build_lookups.
        self._person_by_pid: dict[str, str] = {}
        self._samples_per_analysis: dict[str, list[str]] = {}  # analysis_pid → [sample_pid]
        self._files_per_analysis: dict[str, list[str]] = {}    # analysis_pid → [file @id]
        # Dataset creator @ids (resolved Person @ids).
        self._dataset_creator_ids: list[str] = []

    def run(self) -> None:
        logger.info("Starting flat-data uplift from %s", self.input_path)
        self._load()
        self._classify()
        self._build_lookups()
        self._resolve_links()
        self._write()
        logger.info("Flat-data uplift complete. Output: %s", self.output_path)

    # --- Phase 1 — load -----------------------------------------------------

    def _load(self) -> None:
        files = sorted(self.input_path.glob("*.jsonld"))
        if not files:
            logger.warning("No JSON-LD files found in %s", self.input_path)
        for path in files:
            with path.open() as f:
                self._entities[path.name] = json.load(f)
        logger.info("Loaded %d entity file(s)", len(self._entities))

    # --- Phase 2 — classify -------------------------------------------------

    def _classify(self) -> None:
        for entity in self._entities.values():
            etype = entity.get("@type")
            if etype == "Person":
                self._persons.append(entity)
            elif etype == "Action":
                self._actions.append(entity)
            elif etype == "Product":
                self._sample_stubs.append(entity)
            elif etype == "Dataset":
                if _has_additional_property(entity, _FILE_ANALYSIS):
                    self._file_datasets.append(entity)
                else:
                    self._main_datasets.append(entity)
            else:
                logger.debug("Skipping entity with unrecognized @type: %r", etype)

        if len(self._main_datasets) != 1:
            logger.warning(
                "Expected exactly 1 main Dataset, found %d", len(self._main_datasets)
            )
        logger.info(
            "Classified: %d Person, %d main Dataset, %d file Dataset, "
            "%d Action, %d Product stub",
            len(self._persons), len(self._main_datasets), len(self._file_datasets),
            len(self._actions), len(self._sample_stubs),
        )

    # --- Phase 3 — build lookups -------------------------------------------

    def _build_lookups(self) -> None:
        # Person ORCID PID → Person @id
        for person in self._persons:
            pid = _identifier_value(person.get("identifier"))
            pid_str = str(pid) if pid is not None else None
            pid_at = person.get("@id")
            if pid_str and pid_at:
                if pid_str in self._person_by_pid:
                    logger.warning(
                        "Multiple Persons share ORCID %s; using first match", pid_str
                    )
                else:
                    self._person_by_pid[pid_str] = pid_at

        # Dataset creators — Persons flagged is-dataset-author
        for person in self._persons:
            ap = _find_additional_property(person, _IS_DATASET_AUTHOR)
            if ap and _truthy(ap.get("value")) and (pid_at := person.get("@id")):
                self._dataset_creator_ids.append(pid_at)

        # samples_per_analysis (from Product stubs' additionalProperty)
        for stub in self._sample_stubs:
            ap = _find_additional_property(stub, _SAMPLE_ANALYSIS_PID)
            if not ap:
                continue
            analysis_pid = ap.get("value")
            sample_pid = _identifier_value(stub.get("identifier"))
            if analysis_pid is None or sample_pid is None:
                continue
            self._samples_per_analysis.setdefault(str(analysis_pid), []).append(
                str(sample_pid)
            )

        # files_per_analysis (from file Datasets' additionalProperty)
        for file_ds in self._file_datasets:
            ap = _find_additional_property(file_ds, _FILE_ANALYSIS)
            if not ap:
                continue
            analysis_pid = ap.get("value")
            file_at = file_ds.get("@id")
            if analysis_pid is None or file_at is None:
                continue
            self._files_per_analysis.setdefault(str(analysis_pid), []).append(file_at)

    # --- Phase 4 — resolve --------------------------------------------------

    def _resolve_links(self) -> None:
        # Main Dataset(s): add creator
        for ds in self._main_datasets:
            if self._dataset_creator_ids:
                ds["creator"] = _unwrap_single(
                    [{"@type": "Person", "@id": pid} for pid in self._dataset_creator_ids]
                )

        # Actions: resolve agent.identifier → @id; inject object + result
        for action in self._actions:
            self._resolve_agent(action)
            self._inject_action_object(action)
            self._inject_action_result(action)

        # file Datasets: resolve about.identifier → deterministic Product_<pid>.jsonld
        for file_ds in self._file_datasets:
            self._resolve_file_about(file_ds)

    def _resolve_agent(self, action: dict) -> None:
        agent = action.get("agent")
        agents = _as_list(agent)
        resolved: list[dict] = []
        for a in agents:
            if not isinstance(a, dict):
                continue
            pid = _identifier_value(a.get("identifier"))
            if pid is None:
                resolved.append(a)
                continue
            pid_at = self._person_by_pid.get(str(pid))
            if pid_at is None:
                logger.warning(
                    "Action %s: no Person found for agent ORCID %s; keeping stub",
                    action.get("@id"), pid,
                )
                resolved.append(a)
                continue
            resolved.append({"@type": "Person", "@id": pid_at})
        if resolved:
            action["agent"] = _unwrap_single(resolved)

    def _inject_action_object(self, action: dict) -> None:
        analysis_pid = _identifier_value(action.get("identifier"))
        if analysis_pid is None:
            return
        sample_pids = self._samples_per_analysis.get(str(analysis_pid), [])
        if not sample_pids:
            return
        action["object"] = _unwrap_single(
            [
                {"@type": "Product", "@id": f"Product_{pid}.jsonld"}
                for pid in sample_pids
            ]
        )

    def _inject_action_result(self, action: dict) -> None:
        analysis_pid = _identifier_value(action.get("identifier"))
        if analysis_pid is None:
            return
        file_ids = self._files_per_analysis.get(str(analysis_pid), [])
        if not file_ids:
            return
        action["result"] = _unwrap_single(
            [{"@type": "Dataset", "@id": fid} for fid in file_ids]
        )

    def _resolve_file_about(self, file_ds: dict) -> None:
        about = file_ds.get("about")
        if not isinstance(about, dict):
            return
        sample_pid = _identifier_value(about.get("identifier"))
        if sample_pid is None:
            return
        file_ds["about"] = {
            "@type": "Product",
            "@id": f"Product_{sample_pid}.jsonld",
        }

    # --- Phase 5 — write ----------------------------------------------------

    def _write(self) -> None:
        self.output_path.mkdir(parents=True, exist_ok=True)
        kept: Iterable[tuple[str, dict]] = (
            (name, entity)
            for name, entity in self._entities.items()
            if entity.get("@type") != "Product"  # drop sample stubs
        )
        count = 0
        for name, entity in kept:
            write_json(entity, self.output_path / name)
            count += 1
        logger.info(
            "Wrote %d uplifted file(s) to %s (dropped %d sample stub(s))",
            count, self.output_path, len(self._sample_stubs),
        )


def _unwrap_single(items: list) -> list | dict:
    """Return a single dict when the list has one item, else the list itself."""
    return items[0] if len(items) == 1 else items
