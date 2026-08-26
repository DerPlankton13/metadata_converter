"""Tests for phase-based config dispatch.

``uplift`` and ``uplift_record`` are separate CLI phases, and the phase — not a
union — decides which model a config file is read as. These tests pin the two
guardrails that dispatch relies on.
"""

import pytest

from metadata_converter.biosamples.config import BiosamplesUpliftRecordConfig
from metadata_converter.config import load_uplift_config, load_uplift_record_config
from metadata_converter.uplift.config import GenericUpliftConfig


def write_toml(path, body: str) -> str:
    path.write_text(body)
    return str(path)


def test_load_uplift_config_reads_a_generic_config(tmp_path):
    path = write_toml(
        tmp_path / "uplift.toml",
        'input_dir = "in"\noutput_dir = "out"\n',
    )

    cfg = load_uplift_config(path)

    assert isinstance(cfg, GenericUpliftConfig)
    # atomize is on unless a config turns it off
    assert cfg.atomize is True


def test_load_uplift_config_rejects_an_uplift_record_config(tmp_path):
    """A config carrying source_type belongs to uplift_record, not uplift.

    GenericUpliftConfig forbids extra keys, so source_type fails validation — that
    rejection is what keeps a misrouted config from being silently accepted here.
    """
    path = write_toml(
        tmp_path / "uplift.toml",
        'source_type = "biosamples"\ninput_dir = "in"\noutput_dir = "out"\n',
    )

    with pytest.raises(SystemExit):
        load_uplift_config(path)


def test_load_uplift_record_config_reads_a_biosamples_config(tmp_path):
    path = write_toml(
        tmp_path / "uplift_record.toml",
        'source_type = "biosamples"\ninput_dir = "in"\noutput_dir = "out"\n',
    )

    cfg = load_uplift_record_config(path)

    assert isinstance(cfg, BiosamplesUpliftRecordConfig)


def test_generic_uplift_config_rejects_a_list_input_dir(tmp_path):
    """The writable input is a single directory, deliberately.

    Naming a sibling source here would copy it into this source's output_dir and
    record it as based on itself; reference_dirs is the read-only way to reach one.
    """
    path = write_toml(
        tmp_path / "uplift.toml",
        'input_dir = ["a", "b"]\noutput_dir = "out"\n',
    )

    with pytest.raises(SystemExit):
        load_uplift_config(path)
