import logging
import shutil

import pytest

from agage_archive.config import data_file_path
from agage_archive.logging_config import PACKAGE_LOGGER


ERROR_LOGS = ("error_log_individual.txt", "error_log_combined.txt")


@pytest.fixture
def clean_output():
    """Give a test a clean agage_test output directory and error logs.

    Several tests write into data/agage_test/output. Without this fixture they leave
    files behind for each other to trip over, which makes the suite order-dependent.
    The error logs matter too: run_individual_instrument appends to them and, unlike
    run_all, does not clear them first, so a stale log would be read as this test's
    failures. Anything a test needs to assert on should be read before it returns.

    Yields:
        pathlib.Path: Path to the (empty) output directory.
    """

    pth = data_file_path("", network="agage_test", sub_path="output", errors="ignore")

    def empty():
        for name in ERROR_LOGS:
            log = data_file_path(name, network="agage_test", errors="ignore")
            if log.exists():
                log.unlink()

        if not pth.exists():
            pth.mkdir(parents=True, exist_ok=True)
            return
        for f in sorted(pth.iterdir()):
            if f.name.startswith("."):
                # Keep .gitignore and friends
                continue
            if f.is_dir():
                shutil.rmtree(f)
            else:
                f.unlink()

    empty()
    yield pth
    empty()


@pytest.fixture
def error_log_text():
    """Read the agage_test error logs.

    Returns:
        Callable[[], str]: Function returning the concatenated error logs, or "" if none
            were written.
    """

    def read():
        text = ""
        for name in ERROR_LOGS:
            log = data_file_path(name, network="agage_test", errors="ignore")
            if log.exists():
                text += log.read_text()
        return text

    return read


@pytest.fixture
def caught_logs():
    """Capture records logged through the agage_archive package logger.

    The package logger sets propagate=False (see logging_config.py), so pytest's
    built-in caplog fixture -- which attaches to the root logger -- never sees these
    records. This attaches a handler directly to the agage_archive logger instead.

    Yields:
        list[logging.LogRecord]: Records logged during the test, appended live.
    """

    records = []

    class _ListHandler(logging.Handler):
        def emit(self, record):
            records.append(record)

    handler = _ListHandler()
    PACKAGE_LOGGER.addHandler(handler)
    try:
        yield records
    finally:
        PACKAGE_LOGGER.removeHandler(handler)
