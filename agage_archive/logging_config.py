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
- A message identical to the immediately preceding one is suppressed (see
  ``_DeduplicateFilter``). Some warnings recur once per item in a large loop -- e.g. a
  site missing a piece of per-site config warns once per species processed for that
  site -- and without this, hundreds of consecutive duplicates would themselves bury
  everything else, defeating the point of making warnings always visible.

The handler writes to ``sys.stderr``, resolved *at emit time* rather than captured when
the handler is built (see ``_ConsoleHandler``). ``configure_logging`` runs at package
import, before an IDE, debugger or notebook has swapped in its own ``sys.stderr``
wrapper; a handler that had bound the original stream would keep writing to it, and if
that original stream is a pipe the host has stopped reading (common under debuggers),
the write blocks forever with no traceback. Resolving ``sys.stderr`` each time keeps
output going to the stream the host is actually reading -- the same one ``tqdm`` draws
its progress bars on.

The handler deliberately does *not* route records through ``tqdm.write``. Coupling every
log call to tqdm's locking was a second way to hang in some environments, and buys
little: in the default quiet mode the only messages emitted while a bar is active are
occasional (de-duplicated) warnings, so the cost is at most a redrawn bar, not garbled
output.

Both the formatter and the deduplicate filter are attached to the handler, not to
``PACKAGE_LOGGER`` itself: log calls are made on per-module child loggers (e.g.
``agage_archive.io``), and a filter attached to a logger only applies to records logged
directly on that exact logger object, not to records from children that merely
propagate through it on their way to this handler.
"""

import logging
import sys

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


class _ConsoleHandler(logging.Handler):
    """Write records to the current ``sys.stderr``, resolved at emit time.

    Unlike ``logging.StreamHandler``, this does not bind the stream when the handler is
    created, so it follows any later reassignment of ``sys.stderr`` (by an IDE, debugger
    or notebook) instead of writing to a possibly-abandoned original stream.
    """

    def emit(self, record):
        try:
            stream = sys.stderr
            stream.write(self.format(record) + "\n")
            stream.flush()
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
        handler = _ConsoleHandler()
        handler.setFormatter(_LevelFormatter())
        handler.addFilter(_DeduplicateFilter())
        PACKAGE_LOGGER.addHandler(handler)
    PACKAGE_LOGGER.propagate = False
