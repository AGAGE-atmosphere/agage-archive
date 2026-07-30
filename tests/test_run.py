"""Targeted tests for the processing orchestration in agage_archive.run.

These pin behaviours that the golden manifest test only covers indirectly. The manifest
records what the archive looks like; if one of these bugs came back, the manifest would
simply be regenerated to match. These tests assert the rule rather than the result.
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

import agage_archive.run
from agage_archive.data_selection import data_combination_species, read_data_combination
from agage_archive.run import run_all, run_combined_instruments, \
    run_individual_instrument, run_timestamp_checks


NETWORK = "agage_test"


def _dataset(times, instrument_type=0):
    """Build a minimal dataset with the variables run_timestamp_checks needs.

    Args:
        times (list): Timestamps.
        instrument_type (int, optional): Instrument type value. Defaults to 0.

    Returns:
        xr.Dataset: Dataset with time and instrument_type.
    """

    time = pd.to_datetime(times)
    return xr.Dataset(
        {"instrument_type": ("time", np.full(len(time), instrument_type, dtype=np.int8))},
        coords={"time": time},
        attrs={"network": NETWORK},
    )


# run_timestamp_checks
# ====================

def test_timestamp_checks_pass_when_baseline_matches():
    """The happy path: identical timestamps in data and baseline."""

    ds = _dataset(["2000-01-01", "2000-01-02", "2000-01-03"])
    ds_baseline = _dataset(["2000-01-01", "2000-01-02", "2000-01-03"])

    run_timestamp_checks(ds, ds_baseline, "ch3ccl3", "CGO")


def test_timestamp_checks_allow_no_baseline():
    """Passing no baseline dataset is legitimate and must not raise."""

    run_timestamp_checks(_dataset(["2000-01-01", "2000-01-02"]), None, "ch3ccl3", "CGO")


def test_timestamp_checks_reject_duplicate_timestamps():
    """Duplicate timestamps in the data are an error, and the message names them."""

    ds = _dataset(["2000-01-01", "2000-01-01", "2000-01-02"])

    with pytest.raises(ValueError, match="Duplicate timestamps"):
        run_timestamp_checks(ds, None, "ch3ccl3", "CGO")


def test_timestamp_checks_reject_mismatched_timestamps():
    """Same number of points, different times: the data and baseline disagree."""

    ds = _dataset(["2000-01-01", "2000-01-02"])
    ds_baseline = _dataset(["2000-01-01", "2000-01-03"])

    with pytest.raises(ValueError, match="different timestamps"):
        run_timestamp_checks(ds, ds_baseline, "ch3ccl3", "CGO")


def test_timestamp_checks_do_not_skip_on_falsy_baseline():
    """B3: an empty Dataset is falsy, and used to skip every check silently.

    run_timestamp_checks tested `if ds_baseline:`, so an empty baseline dataset was
    indistinguishable from "no baseline requested" and the checks were skipped rather
    than failed.
    """

    ds = _dataset(["2000-01-01", "2000-01-02"])
    empty = xr.Dataset()

    # The falsiness that caused the bug
    assert not empty

    with pytest.raises((KeyError, ValueError)):
        run_timestamp_checks(ds, empty, "ch3ccl3", "CGO")


def test_timestamp_checks_reject_different_length_baseline():
    """B2: a baseline dataset covering fewer points than the data must be an error."""

    ds = _dataset(["2000-01-01", "2000-01-02", "2000-01-03"])
    ds_baseline = _dataset(["2000-01-01", "2000-01-02"])

    with pytest.raises(ValueError, match="different timestamps"):
        run_timestamp_checks(ds, ds_baseline, "ch3ccl3", "CGO")


def test_timestamp_checks_reject_disjoint_baseline():
    """B2: the comparison must not be an xarray one, which aligns the operands first.

    `ds_baseline.time != ds.time` aligns both sides on the time coordinate before
    comparing, so each timestamp is compared with itself and the result is always False.
    The check could never fire, for any input — not even two datasets with no timestamps
    in common.
    """

    ds = _dataset(["2000-01-01", "2000-01-02"])
    ds_baseline = _dataset(["1990-01-01", "1990-01-02"])

    with pytest.raises(ValueError, match="different timestamps"):
        run_timestamp_checks(ds, ds_baseline, "ch3ccl3", "CGO")


# Deterministic instrument ordering
# =================================

def test_data_combination_order_follows_the_file():
    """Instrument order must come from the data_combination columns, not a set.

    read_data_combination used to iterate set(instruments), so the order varied with
    Python's hash seed. That order reaches xr.concat(combine_attrs="override") in
    combine_datasets, which takes global attributes from whichever dataset is first, so
    combined files were not reproducible.
    """

    instrument_dates = read_data_combination(NETWORK, "ch3ccl3", "CGO")

    # The column order of data_combination_CGO.csv, minus the instruments marked x
    assert list(instrument_dates) == ["ALE", "GAGE", "GCMD", "GCMS-Medusa"]


def test_data_combination_order_is_stable_across_calls():
    """Repeated calls must agree, whatever the hash seed of this interpreter."""

    orders = [list(read_data_combination(NETWORK, "ch3ccl3", "CGO")) for _ in range(5)]

    assert all(order == orders[0] for order in orders)


# data_combination_species
# ========================

def test_data_combination_species_lists_explicit_entries():
    """Species named in the site's data_combination file are reported."""

    species = data_combination_species(NETWORK, "CGO")

    assert {"ch3ccl3", "ccl4", "cfc-11", "cfc-12", "n2o"} <= species


def test_data_combination_species_empty_without_file():
    """A site with no data_combination file has no explicit entries.

    This is what distinguishes "one instrument, chosen deliberately" from "no entry, so
    any instrument may claim the top-level file".
    """

    assert data_combination_species(NETWORK, "MHD") == set()


# Contested top-level files
# =========================

def test_contested_top_level_file_raises(monkeypatch, clean_output, error_log_text):
    """Two instruments eligible for the top-level file must fail, not silently drop one.

    ccl4 at CGO is measured by both ALE and GAGE. With no data_combination entry, both
    are eligible to write the recommended file at ccl4/, and whichever ran first used to
    win while the other was discarded without comment.
    """

    # Pretend ccl4 has no data_combination entry at all. Both functions have to be
    # patched: read_data_combination decides whether the top-level folder is written to,
    # and data_combination_species decides whether doing so is legitimate.
    monkeypatch.setattr(agage_archive.run, "data_combination_species",
                        lambda network, site: set())
    monkeypatch.setattr(agage_archive.run, "read_data_combination",
                        lambda network, species, site, verbose=True: {"GCMS-Medusa": [None, None]})

    for instrument in ("ALE", "GAGE"):
        run_individual_instrument(network=NETWORK, instrument=instrument,
                                  species=["ccl4"], sites=["CGO"],
                                  baseline="", monthly=False)

    errors = error_log_text()

    assert "More than one instrument is eligible" in errors
    assert "ccl4" in errors and "CGO" in errors
    # The message must say how to fix it
    assert "data_combination_CGO.csv" in errors


def test_uncontested_top_level_file_is_written(clean_output, error_log_text):
    """The real fixture has a data_combination entry for ccl4, so this must succeed."""

    for instrument in ("ALE", "GAGE"):
        run_individual_instrument(network=NETWORK, instrument=instrument,
                                  species=["ccl4"], sites=["CGO"],
                                  baseline="", monthly=False)

    assert error_log_text() == ""


# Instruments without baseline flags
# ==================================

def test_flask_with_baseline_produces_mole_fractions(clean_output, error_log_text):
    """B9: flask data has no baseline flags, and used to produce nothing at all.

    read_baseline_function is None for GCMS-Medusa-flask, but it was called regardless
    when baseline was requested, raising TypeError. The per-species handler swallowed it
    into the error log, so the flask mole fraction files were never written either.
    """

    run_individual_instrument(network=NETWORK, instrument="GCMS-Medusa-flask",
                              species=["cf4"], sites=["CBW"],
                              baseline="git_pollution_flag", monthly=True)

    assert error_log_text() == ""

    written = [f.name for f in clean_output.rglob("*.nc")]
    assert any("cf4" in name for name in written), f"no flask output written: {written}"

    # Baseline products are skipped rather than half-written
    assert not list(clean_output.rglob("baseline-flags/*.nc"))
    assert not list(clean_output.rglob("monthly-baseline/*.nc"))


# T1: run.py control flow
# =======================
#
# These pin the branches of the individual/combined orchestration that the golden manifest
# only exercises as a side effect: the release-schedule "x" skip, single- vs multi-instrument
# folder choice, the top_level_only guard, and the error-log path. They exist so that the
# Phase 4 (S6) rewrite of this control flow — which removes the leftover per-site/per-species
# split and the (site, species, error) tuple threading — can be checked against the intended
# behaviour rather than only against the byte-level manifest.


def test_release_schedule_x_is_skipped(clean_output, error_log_text):
    """A cell marked "x" means "process no part of this record": no file, no error.

    ALE measures cfc-11 only at CGO in the fixture; every other site, including SMO, is
    marked x. Processing SMO must produce nothing at all and must not be treated as a
    failure.
    """

    run_individual_instrument(NETWORK, "ALE", species=["cfc-11"], sites=["SMO"],
                              baseline="", monthly=False)

    assert error_log_text() == ""
    assert not list(clean_output.rglob("*.nc"))


def test_unknown_species_for_instrument_is_skipped(clean_output, error_log_text):
    """A species absent from the instrument's release schedule is skipped cleanly.

    hfc-134a is a GCMS-Magnum species and has no row in the ALE schedule, so
    run_individual_instrument has nothing to process and must return without writing a
    file or an error.
    """

    run_individual_instrument(NETWORK, "ALE", species=["hfc-134a"], sites=["CGO"],
                              baseline="", monthly=False)

    assert error_log_text() == ""
    assert not list(clean_output.rglob("*.nc"))


def test_single_instrument_is_promoted_to_top_level(clean_output, error_log_text):
    """One instrument and no data_combination entry: the file is also the recommended file.

    nf3 at MHD is measured only by GCMS-Medusa, and MHD has no data_combination file, so
    read_data_combination returns a single default instrument. The individual file is then
    written both to individual-instruments/ and, as the recommended record, to the
    top-level nf3/ directory (with no instrument in its name).
    """

    run_individual_instrument(NETWORK, "GCMS-Medusa", species=["nf3"], sites=["MHD"],
                              baseline="", monthly=False)

    assert error_log_text() == ""

    top_level = list((clean_output / "nf3").glob("agage_test_mhd_nf3_*.nc"))
    individual = list((clean_output / "nf3" / "individual-instruments").glob(
        "agage_test-gcms-medusa_mhd_nf3_*.nc"))

    assert top_level, "single-instrument file was not promoted to the top-level directory"
    assert individual, "individual-instruments file was not written"


def test_top_level_only_conflicts_with_multiple_instruments(clean_output, error_log_text):
    """top_level_only is incompatible with a species that has a combined record.

    ch3ccl3 at CGO is measured by four instruments and has a data_combination entry, so a
    combined file owns the top-level slot. Asking an individual instrument to write only
    the top-level file for it is a contradiction and must be reported, naming the species
    and site.
    """

    run_individual_instrument(NETWORK, "GCMD", species=["ch3ccl3"], sites=["CGO"],
                              baseline="", monthly=False, top_level_only=True)

    errors = error_log_text()

    assert "top_level_only is set to True" in errors
    assert "ch3ccl3" in errors and "CGO" in errors


def test_individual_instrument_defers_to_combined_top_level(monkeypatch, clean_output,
                                                            error_log_text):
    """When a combined file owns the top-level slot, the individual run must not rewrite it.

    cfc-113 at CGO is a single-instrument (GAGE) species that nonetheless has a
    data_combination entry, which marks it as the recommended record. Once the combined
    workflow has written the top-level file, run_individual_site takes the early-return
    path and writes only its individual-instruments file, leaving the top-level file to
    the combined product.
    """

    run_combined_instruments(NETWORK, species=["cfc-113"], sites=["CGO"],
                             baseline=False, monthly=False)
    assert error_log_text() == ""
    assert list((clean_output / "cfc-113").glob("agage_test_cgo_cfc-113_*.nc"))

    real_output_dataset = agage_archive.run.output_dataset
    written_subpaths = []

    def spy(ds, network, output_subpath="", **kwargs):
        written_subpaths.append(output_subpath)
        return real_output_dataset(ds, network, output_subpath=output_subpath, **kwargs)

    monkeypatch.setattr(agage_archive.run, "output_dataset", spy)

    run_individual_instrument(NETWORK, "GAGE", species=["cfc-113"], sites=["CGO"],
                              baseline="", monthly=False)

    assert error_log_text() == ""
    # Its own file is written...
    assert "cfc-113/individual-instruments" in written_subpaths
    # ...but the top-level file, owned by the combined product, is left untouched.
    assert "cfc-113" not in written_subpaths


def test_read_failure_is_logged_for_that_species_only(monkeypatch, clean_output,
                                                       error_log_text):
    """A failure in one species lands in the error log, naming it, and spares the others.

    run_individual_site wraps each species in a broad except that appends to the error
    log, so a failure is a missing file rather than a crash. Break the read for cfc-11 and
    confirm it is logged with its site, species and message, while cfc-12 — read through
    the same instrument in the same call — succeeds and is absent from the log.
    """

    real_get = agage_archive.run.get_data_read_function

    def failing_get(network, instrument):
        reader = real_get(network, instrument)

        def wrapper(network, species, site, instrument, **kwargs):
            if species == "cfc-11":
                raise ValueError("synthetic read failure")
            return reader(network, species, site, instrument, **kwargs)

        wrapper.__name__ = reader.__name__
        return wrapper

    monkeypatch.setattr(agage_archive.run, "get_data_read_function", failing_get)

    run_individual_instrument(NETWORK, "ALE", species=["cfc-11", "cfc-12"], sites=["CGO"],
                              baseline="", monthly=False)

    errors = error_log_text()

    assert "synthetic read failure" in errors
    assert "cfc-11" in errors and "CGO" in errors
    # cfc-12 was read through the same instrument and succeeded
    assert "cfc-12" not in errors


# run_all argument validation
# ===========================
#
# The hand-written isinstance wall in run_all (Phase 4, S3). Cheap to pin because each
# check raises before any processing or filesystem side effect.


def test_run_all_requires_a_network():
    with pytest.raises(ValueError, match="Must specify network"):
        run_all("")


def test_run_all_rejects_non_string_network():
    with pytest.raises(TypeError, match="network must be a string"):
        run_all(123)


def test_run_all_rejects_non_list_species():
    with pytest.raises(TypeError, match="species must be a list"):
        run_all(NETWORK, species="ch3ccl3")


def test_run_all_rejects_non_bool_baseline():
    with pytest.raises(TypeError, match="baseline must be a boolean"):
        run_all(NETWORK, baseline="yes")
