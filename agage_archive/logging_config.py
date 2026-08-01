"""Console logging for the archive-processing pipeline.

Every module gets its own ``logging.getLogger(__name__)``, which propagates up to the
single ``agage_archive`` logger configured here by :func:`configure_logging`.

Which routine progress messages appear is still controlled by each function's own
``verbose`` argument, exactly as when these were plain ``print()`` calls: the logger
level is fixed at INFO, so an explicit ``if verbose: logger.info(...)`` call site is
shown if and only if that specific ``verbose`` flag is set, matching the previous
print-based behaviour. What logging adds:

- Warnings and errors (``logger.warning``/``logger.error``) are never gated behind
  ``verbose`` and are always visible, with a "WARNING: "/"ERROR: " prefix that routine
  progress output does not get, so they can be told apart at a glance.
- The console handler (``_TqdmSafeHandler``) always writes via ``tqdm.write()``, which
  coordinates with any currently-active ``tqdm`` progress bar so a log message appears
  above it instead of corrupting its rendering. This works whether or not a bar happens
  to be active, so no special handling is needed around the loops in ``run.py`` that use
  ``tqdm``.
- A message identical to the immediately preceding one is suppressed (see
  ``_DeduplicateFilter``). Some warnings recur once per item in a large loop -- e.g. a
  site missing a piece of per-site config warns once per species processed for that
  site -- and without this, hundreds of consecutive duplicates would themselves bury
  everything else, defeating the point of making warnings always visible.

Both the formatter and the deduplicate filter are attached to the handler, not to
``PACKAGE_LOGGER`` itself: log calls are made on per-module child loggers (e.g.
``agage_archive.io``), and a filter attached to a logger only applies to records logged
directly on that exact logger object, not to records from children that merely
propagate through it on their way to this handler.
"""

import logging
import sys

from tqdm import tqdm

PACKAGE_LOGGER = logging.getLogger("agage_archive")


class _LevelFormatter(logging.Formatter):
    """Prefix WARNING/ERROR/CRITICAL with their level name; leave INFO/DEBUG bare.

    Keeps routine progress messages in the existing "... doing X" style, while making
    warnings and errors visually distinct without having to change every call site.
    """

    def format(self, record):
        self._style._fmt = "%(levelname)s: %(message)s" if record.levelno >= logging.WARNING \
            else "%(message)s"
        return super().format(record)


class _DeduplicateFilter(logging.Filter):
    """Drop a record if it is identical (logger, level, message) to the previous one."""

    def __init__(self):
        super().__init__()
        self._last = None

    def filter(self, record):
        key = (record.name, record.levelno, record.getMessage())
        if key == self._last:
            return False
        self._last = key
        return True


class _TqdmSafeHandler(logging.StreamHandler):
    """A StreamHandler that writes through tqdm.write() instead of the raw stream.

    Safe to use whether or not a tqdm progress bar is currently active: with no bar
    active this behaves like a plain stream write.
    """

    def emit(self, record):
        try:
            tqdm.write(self.format(record), file=self.stream)
        except Exception:
            self.handleError(record)


def configure_logging():
    """Attach a console handler to the package logger, once.

    Called at package import time, so console output works the same whether or not the
    caller goes through :func:`agage_archive.run.run_all`. Safe to call again (e.g. once
    per ``run_all`` call) -- does nothing after the first call.
    """

    PACKAGE_LOGGER.setLevel(logging.INFO)
    if not PACKAGE_LOGGER.handlers:
        handler = _TqdmSafeHandler(sys.stderr)
        handler.setFormatter(_LevelFormatter())
        handler.addFilter(_DeduplicateFilter())
        PACKAGE_LOGGER.addHandler(handler)
    PACKAGE_LOGGER.propagate = False
