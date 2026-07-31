"""Tests for the input-file consistency checker (agage_archive.checks, issue #97)."""

from unittest.mock import Mock

import pandas as pd
import pytest

import agage_archive.checks as checks
import agage_archive.run as run_module
from agage_archive.checks import (
    check_species_known,
    check_release_schedule_df,
    check_data_combination_df,
    collect_input_file_problems,
    check_input_files,
)
from agage_archive.run import run_all


# The real fixture must be clean
# ==============================

def test_agage_test_config_passes():
    """The committed agage_test configuration must have no consistency problems."""

    assert collect_input_file_problems("agage_test") == []
    # Should not raise
    check_input_files("agage_test", verbose=False)


# Species-name consistency
# ========================

def test_check_species_known_flags_unknown_species():
    known = {"cfc-11", "ch3ccl3"}

    problems = check_species_known(["cfc-11", "madeupgas"], known, "schedule.csv")

    assert len(problems) == 1
    assert "madeupgas" in problems[0]
    assert "schedule.csv" in problems[0]


def test_check_species_known_is_case_insensitive():
    """Names differing only in case are consistent once canonicalised."""

    assert check_species_known(["CFC-11", "ch3ccl3"], {"cfc-11", "ch3ccl3"}, "f.csv") == []


def test_check_species_known_ignores_blank_and_excluded():
    assert check_species_known(["", "x", None], {"cfc-11"}, "f.csv") == []


# Release-schedule dates
# ======================

def test_check_release_schedule_flags_unparseable_date():
    df = pd.DataFrame({
        "Species": ["cfc-11", "ch4"],
        "MHD": ["2020-01-01 00:00", "x"],
        "CGO": ["", "not-a-date"],
    })

    problems = check_release_schedule_df(df, "2023-01-01 00:00", "rs.csv")

    assert len(problems) == 1
    assert "ch4" in problems[0] and "CGO" in problems[0] and "not-a-date" in problems[0]


def test_check_release_schedule_flags_bad_general_release_date():
    df = pd.DataFrame({"Species": ["cfc-11"], "MHD": ["x"]})

    problems = check_release_schedule_df(df, "garbage", "rs.csv")

    assert len(problems) == 1
    assert "general release date" in problems[0]


def test_check_release_schedule_accepts_blank_and_excluded_cells():
    df = pd.DataFrame({"Species": ["cfc-11"], "MHD": [""], "CGO": ["x"]})

    assert check_release_schedule_df(df, "2023-01-01 00:00", "rs.csv") == []


# data_combination dates and ordering
# ===================================

def test_check_data_combination_flags_end_before_start():
    df = pd.DataFrame({
        "Species": ["ch3ccl3", "ccl4"],
        "ALE start": ["", "2000-01-01 00:00"],
        "ALE end": ["1984-01-01 00:00", "1990-01-01 00:00"],
    })

    problems = check_data_combination_df(df, "dc.csv")

    # ch3ccl3 has an unbounded (blank) start, so only ccl4's reversed range is flagged
    assert len(problems) == 1
    assert "ccl4" in problems[0] and "before" in problems[0]


def test_check_data_combination_flags_unparseable_date():
    df = pd.DataFrame({
        "Species": ["ch3ccl3"],
        "GAGE start": ["notadate"],
        "GAGE end": ["x"],
    })

    problems = check_data_combination_df(df, "dc.csv")

    assert len(problems) == 1
    assert "GAGE start" in problems[0] and "notadate" in problems[0]


def test_check_data_combination_accepts_valid_and_excluded():
    df = pd.DataFrame({
        "Species": ["ch3ccl3"],
        "ALE start": [""],
        "ALE end": ["1984-01-01 00:00"],
        "GAGE start": ["1984-01-01 00:00"],
        "GAGE end": ["1994-03-15 00:00"],
        "Picarro start": ["x"],
        "Picarro end": ["x"],
    })

    assert check_data_combination_df(df, "dc.csv") == []


# The raising entry point
# =======================

def test_check_input_files_raises_and_lists_all_problems(monkeypatch):
    monkeypatch.setattr(checks, "collect_input_file_problems",
                        lambda network: ["problem one", "problem two"])

    with pytest.raises(ValueError) as excinfo:
        check_input_files("agage_test")

    message = str(excinfo.value)
    assert "2 problem" in message
    assert "problem one" in message and "problem two" in message


# Integration with run_all
# ========================

def test_run_all_check_inputs_aborts_before_processing(monkeypatch):
    """check_inputs=True runs the checker and aborts before any archive is touched."""

    checker = Mock(side_effect=ValueError("bad config"))
    deleter = Mock(side_effect=AssertionError("processing started despite a failed check"))
    monkeypatch.setattr(run_module, "check_input_files", checker)
    monkeypatch.setattr(run_module, "delete_archive", deleter)

    with pytest.raises(ValueError, match="bad config"):
        run_all("agage_test", check_inputs=True)

    checker.assert_called_once()
    deleter.assert_not_called()


def test_run_all_runs_checker_by_default(monkeypatch):
    """check_inputs defaults to True: the checker runs before processing."""

    checker = Mock()
    monkeypatch.setattr(run_module, "check_input_files", checker)
    # Abort as soon as real processing begins, so the full pipeline does not run.
    monkeypatch.setattr(run_module, "delete_archive",
                        Mock(side_effect=RuntimeError("reached processing")))

    with pytest.raises(RuntimeError, match="reached processing"):
        run_all("agage_test")

    checker.assert_called_once()


def test_run_all_check_inputs_false_skips_checker(monkeypatch):
    """check_inputs=False opts out of the checker."""

    checker = Mock()
    monkeypatch.setattr(run_module, "check_input_files", checker)
    monkeypatch.setattr(run_module, "delete_archive",
                        Mock(side_effect=RuntimeError("reached processing")))

    with pytest.raises(RuntimeError, match="reached processing"):
        run_all("agage_test", check_inputs=False)

    checker.assert_not_called()


def test_run_all_rejects_non_bool_check_inputs():
    with pytest.raises(TypeError, match="check_inputs must be a boolean"):
        run_all("agage_test", check_inputs="yes")
