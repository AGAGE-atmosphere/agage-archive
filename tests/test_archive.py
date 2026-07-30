"""Golden manifest test for the full agage_test archive.

This is the safety net for refactoring: it runs the whole processing chain and compares
the resulting archive against a checked-in reference manifest describing every file's
path, variables, dtypes, encoding and attributes. Anything that changes the output
format or the archive structure will fail here.

The manifest deliberately excludes the three attributes that are expected to vary
between runs (file_created, file_created_by, processing_code_version).

If you have changed the output *on purpose*, regenerate the reference with:

    AGAGE_UPDATE_MANIFEST=1 python -m pytest tests/test_archive.py

and check the diff carefully before committing it. See AGENTS.md: the output format is
frozen, and a manifest diff is exactly the thing that needs an explicit decision.
"""

import hashlib
import json
import os
import tempfile
from pathlib import Path
from zipfile import ZipFile

import numpy as np
import pytest
import xarray as xr

import agage_archive.config
from agage_archive.config import data_file_path
from agage_archive.run import run_all


NETWORK = "agage_test"

# Attributes that legitimately change between runs and so cannot be pinned
VOLATILE_ATTRS = {"file_created", "file_created_by", "processing_code_version"}

REFERENCE = Path(__file__).parent / "reference" / "archive_manifest.json"


def _data_checksum(ds):
    """Checksum of the data in a dataset, robust to last-bit floating point noise.

    Values are rounded to 6 significant figures before hashing. float32 carries roughly
    7 significant figures, so this absorbs harmless differences between numpy/pandas
    versions while still catching any real change in the numbers.

    Args:
        ds (xr.Dataset): Dataset to summarise.

    Returns:
        str: Hex digest.
    """

    h = hashlib.sha256()

    for var in sorted(ds.variables):
        values = ds[var].values
        if np.issubdtype(values.dtype, np.floating):
            with np.errstate(divide="ignore", invalid="ignore"):
                magnitude = np.floor(np.log10(np.abs(values)))
                decimals = np.where(np.isfinite(magnitude), 5 - magnitude, 0)
                values = np.array([round(float(v), int(d)) if np.isfinite(v) else v
                                   for v, d in zip(values.ravel(), decimals.ravel())])
        h.update(var.encode("utf-8"))
        h.update(np.asarray(values).tobytes())

    return h.hexdigest()


def archive_manifest(root):
    """Describe every file in an output archive.

    Args:
        root (pathlib.Path): Archive root directory.

    Returns:
        dict: Manifest keyed by archive-relative path.
    """

    root = Path(root)
    manifest = {}

    for path in sorted(root.rglob("*")):
        if path.is_dir() or path.name.startswith("."):
            continue

        rel = path.relative_to(root).as_posix()

        if path.suffix != ".nc":
            # Non-netCDF files (README, CHANGELOG) are part of the archive structure,
            # but their content is not this repository's concern
            manifest[rel] = {"type": "file"}
            continue

        with xr.open_dataset(path) as ds:
            manifest[rel] = {
                "type": "netcdf",
                "n_time": int(ds.sizes.get("time", 0)),
                "variables": {
                    var: {
                        "dtype": str(ds[var].dtype),
                        "encoding": {k: str(v) for k, v in sorted(ds[var].encoding.items())
                                     if k in ("dtype", "_FillValue", "units", "calendar")},
                        "attrs": {k: str(v) for k, v in sorted(ds[var].attrs.items())},
                    }
                    for var in sorted(ds.variables)
                },
                "attrs": {k: str(v) for k, v in sorted(ds.attrs.items())
                          if k not in VOLATILE_ATTRS},
                "data_checksum": _data_checksum(ds),
            }

    return manifest


def _patch_config_output(monkeypatch, output_path_value):
    """Redirect the agage_test output_path for the duration of a run.

    Paths re-reads config.yaml (via yaml.safe_load) on every construction, so patching
    the loader is the least invasive way to point the output somewhere else without
    editing the user's config file. The guard leaves any non-config YAML untouched, so
    only the network's output_path is affected.

    Args:
        monkeypatch (pytest.MonkeyPatch): Patcher to register the override on.
        output_path_value (str): Value to substitute for the network's output_path.
    """

    real_safe_load = agage_archive.config.yaml.safe_load

    def patched(stream):
        loaded = real_safe_load(stream)
        try:
            network_paths = loaded["paths"][NETWORK]
        except (TypeError, KeyError):
            return loaded
        loaded["paths"][NETWORK] = {**network_paths, "output_path": output_path_value}
        return loaded

    monkeypatch.setattr(agage_archive.config.yaml, "safe_load", patched)


@pytest.fixture(scope="module", params=["directory", "zip"])
def archive(request):
    """Run the full archive once per output backend and return its manifest + error logs.

    The directory and zip backends take different paths through config.py and io.py
    (data_file_path, output_write, create_empty_archive, delete_archive); only the
    directory path was covered before. Running the same golden manifest against zip output
    pins the zip write path end to end — the path used by the downstream dvc-tracked
    release repositories. Zip members are stored under their archive path with no stem
    prefix, so extracting the archive reproduces the exact directory tree and the existing
    reader compares against the same reference manifest.

    Returns:
        tuple[dict, list[str]]: Manifest, and the contents of any error log written.
    """

    out_dir = data_file_path("", network=NETWORK, sub_path="output", errors="ignore")
    out_dir.mkdir(parents=True, exist_ok=True)
    zip_path = out_dir.parent / "output.zip"

    with pytest.MonkeyPatch.context() as monkeypatch:
        if request.param == "zip":
            _patch_config_output(monkeypatch, "output.zip")

        run_all(NETWORK,
                delete=True,
                combined=True,
                baseline=True,
                monthly=True)

        error_logs = []
        for name in ("error_log_individual.txt", "error_log_combined.txt"):
            log = data_file_path(name, network=NETWORK, errors="ignore")
            if log.exists():
                error_logs.append(f"{name}:\n{log.read_text()}")

        if request.param == "zip":
            with tempfile.TemporaryDirectory() as extract_dir:
                with ZipFile(zip_path, "r") as archive_zip:
                    archive_zip.extractall(extract_dir)
                manifest = archive_manifest(extract_dir)
        else:
            manifest = archive_manifest(out_dir)

    if request.param == "zip" and zip_path.exists():
        zip_path.unlink()

    return manifest, error_logs


@pytest.mark.slow
def test_archive_has_no_errors(archive):
    """A full run must not swallow any exceptions into the error logs.

    run_individual_site and run_combined_site catch every exception and log it, so a
    failure shows up as a silently missing file rather than a crash. Every live cell in
    the agage_test release schedules has input data behind it, so any error here is a
    real regression.
    """

    _, error_logs = archive

    assert error_logs == [], "run_all wrote error logs:\n\n" + "\n\n".join(error_logs)


@pytest.mark.slow
def test_archive_matches_reference(archive):
    """The archive structure, format and contents must match the reference manifest."""

    manifest, _ = archive

    if os.environ.get("AGAGE_UPDATE_MANIFEST"):
        REFERENCE.parent.mkdir(parents=True, exist_ok=True)
        REFERENCE.write_text(json.dumps(manifest, indent=1, sort_keys=True) + "\n")
        pytest.skip(f"Reference manifest regenerated ({len(manifest)} files). "
                    "Review the diff before committing.")

    assert REFERENCE.exists(), (
        f"No reference manifest at {REFERENCE}. Generate one with "
        "AGAGE_UPDATE_MANIFEST=1 python -m pytest tests/test_archive.py")

    reference = json.loads(REFERENCE.read_text())

    # Structure first: which files exist
    missing = sorted(set(reference) - set(manifest))
    unexpected = sorted(set(manifest) - set(reference))
    assert not missing, f"{len(missing)} file(s) missing from archive: {missing[:10]}"
    assert not unexpected, f"{len(unexpected)} unexpected file(s): {unexpected[:10]}"

    # Then the content of each file, reported one file at a time so the failure is
    # readable rather than a 125-entry dict diff
    differences = []
    for path in sorted(reference):
        if reference[path] == manifest[path]:
            continue
        for key in sorted(set(reference[path]) | set(manifest[path])):
            if reference[path].get(key) != manifest[path].get(key):
                differences.append(
                    f"{path} [{key}]\n"
                    f"    reference: {str(reference[path].get(key))[:300]}\n"
                    f"    actual   : {str(manifest[path].get(key))[:300]}")

    assert not differences, (
        f"{len(differences)} difference(s) from the reference manifest:\n\n"
        + "\n".join(differences[:15]))
