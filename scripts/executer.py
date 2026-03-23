# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "tomlkit",
# ]
# ///

import subprocess
from pathlib import Path

import tomlkit

# --- Configuration ---
BASE_CONFIG_PATH = "base_configs"
INPUT_DIR = "input"
OUTPUT_DIR = "configs"
CONFIG_PREFIX_STRIP = 5  # Number of characters to strip from base config filename
PROJECT_PATH = "metadata_converter/"
CONVERTER_CMD = "converter"
INPUT_FILES = [f"ReportingContinuous_WP{i}.xlsx" for i in range(1, 8)]


def generate_configs(input_files: list[str]) -> list[Path]:
    """Generate one config file per (base_config, input_file) combination.

    Parameters
    ----------
    input_files : list[str]
        List of input filenames to embed in the configs.

    Returns
    -------
    list[Path]
        List of paths to the generated config files.
    """
    Path(OUTPUT_DIR).mkdir(exist_ok=True)
    output_paths = []

    for config_file in Path(BASE_CONFIG_PATH).glob("*.toml"):
        print(f"Creating modifications of {config_file}")
        with open(config_file, "r") as f:
            config = tomlkit.parse(f.read())

        for input_file in input_files:
            config["input"]["file_path"] = f"{INPUT_DIR}/{input_file}"
            output_path = (
                Path(OUTPUT_DIR)
                / f"{config_file.stem[CONFIG_PREFIX_STRIP:]}_{Path(input_file).stem}.toml"
            )
            with open(output_path, "w") as f:
                tomlkit.dump(config, f)
            output_paths.append(output_path)

    return output_paths


def run_converter(config_path: Path) -> bool:
    """Run the converter for a single config file.

    Parameters
    ----------
    config_path : Path
        Path to the config file to process.

    Returns
    -------
    bool
        True if the converter succeeded, False otherwise.
    """
    try:
        subprocess.run(
            ["uv", "run", "--project", PROJECT_PATH, CONVERTER_CMD, str(config_path)],
            check=True,
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"  Failed: {e}")
        return False


def run_all(config_paths: list[Path]) -> None:
    """Run the converter for all config files and print a summary.

    Parameters
    ----------
    config_paths : list[Path]
        List of config file paths to process.
    """
    succeeded, failed = [], []

    for config_path in config_paths:
        print(f"Running converter for {config_path}")
        if run_converter(config_path):
            succeeded.append(config_path)
        else:
            failed.append(config_path)

    print("\n--- Summary ---")
    print(f"Succeeded ({len(succeeded)}/{len(config_paths)}):")
    for p in succeeded:
        print(f"  ✓ {p}")
    if failed:
        print(f"Failed ({len(failed)}/{len(config_paths)}):")
        for p in failed:
            print(f"  ✗ {p}")


if __name__ == "__main__":
    config_paths = generate_configs(INPUT_FILES)
    run_all(config_paths)
