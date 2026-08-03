import logging

from agage_archive.logging_config import (
    PACKAGE_LOGGER,
    _DeduplicateFilter,
    _LevelFormatter,
    configure_logging,
)


def make_record(level, msg, name="agage_archive.test"):
    return logging.LogRecord(name, level, __file__, 1, msg, None, None)


def test_level_formatter_leaves_info_bare():
    formatter = _LevelFormatter()
    record = make_record(logging.INFO, "... doing something")
    assert formatter.format(record) == "... doing something"


def test_level_formatter_prefixes_warning_and_error():
    formatter = _LevelFormatter()

    warning = make_record(logging.WARNING, "something looks wrong")
    assert formatter.format(warning) == "WARNING: something looks wrong"

    error = make_record(logging.ERROR, "something failed")
    assert formatter.format(error) == "ERROR: something failed"


def test_deduplicate_filter_drops_consecutive_repeats():
    dedup = _DeduplicateFilter()

    first = make_record(logging.WARNING, "Site not set for X... skipping")
    second = make_record(logging.WARNING, "Site not set for X... skipping")
    different = make_record(logging.WARNING, "Site not set for Y... skipping")
    repeat_after_different = make_record(logging.WARNING, "Site not set for X... skipping")

    assert dedup.filter(first) is True
    # Identical to the immediately preceding record: suppressed
    assert dedup.filter(second) is False
    # A different message always passes
    assert dedup.filter(different) is True
    # No longer "immediately preceding", so it passes again
    assert dedup.filter(repeat_after_different) is True


def test_deduplicate_filter_does_not_conflate_different_loggers_or_levels():
    dedup = _DeduplicateFilter()

    warning = make_record(logging.WARNING, "same text", name="agage_archive.a")
    error = make_record(logging.ERROR, "same text", name="agage_archive.a")
    other_logger = make_record(logging.WARNING, "same text", name="agage_archive.b")

    assert dedup.filter(warning) is True
    # Same message text, but a different level: not a duplicate
    assert dedup.filter(error) is True
    # Same message text and level, but a different logger: not a duplicate
    assert dedup.filter(other_logger) is True


def test_configure_logging_is_idempotent():
    configure_logging()
    configure_logging()
    configure_logging()

    assert len(PACKAGE_LOGGER.handlers) == 1
    assert PACKAGE_LOGGER.propagate is False
    assert PACKAGE_LOGGER.level == logging.INFO
