from io import StringIO

import pandas as pd

from agage_archive.io_other_formats import read_wang_file


def test_io_other_formats_imports_without_config():
    """Importing the optional reader must not require a runtime config file."""
    import agage_archive.io_other_formats  # noqa: F401


def test_read_wang_file_parses_whitespace_separated_data():
    data = StringIO(
        "# header\n"
        "# header\n"
        "# header\n"
        "# header\n"
        "# header\n"
        "YYYY MM DD hh min time ABSDA CFC-11S\n"
        "2020 1 2 3 4 0 0 1.5\n"
    )

    result = read_wang_file(data)

    assert result.index[0] == pd.Timestamp("2020-01-02 03:04")
    assert result["CFC-11S"].iloc[0] == 1.5
