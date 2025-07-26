import pandas as pd
import xarray as xr
from pathlib import Path
from zipfile import ZipFile
import tempfile
import os
from pathlib import Path

from agage_archive.util import parse_fortran_format, nc_to_csv, archive_write_csv


def test_parse_fortran_format():

    format_string = "(F10.5, 2I4,I6, 2I4,I6,1X,70(F12.3,a1))"

    column_specs, column_types = parse_fortran_format(format_string)

    assert len(column_specs) == 7+2*70
    assert column_specs[0] == (0, 10)
    assert column_specs[1] == (10, 14)
    assert len(column_types) == 7+2*70
    assert column_types[0] == float

    format_string = "I4"
    column_specs, column_types = parse_fortran_format(format_string)
    assert len(column_specs) == 1
    assert column_specs[0] == (0, 4)
    assert len(column_types) == 1
    assert column_types[0] == int

    format_string = "10(F12.3)"
    column_specs, column_types = parse_fortran_format(format_string)
    assert len(column_specs) == 10
    assert column_specs[0] == (0, 12)
    assert len(column_types) == 10
    assert column_types[0] == float

    format_string = "(a1, 2I4, 2F10.5)"
    column_specs, column_types = parse_fortran_format(format_string)
    assert len(column_specs) == 5
    assert column_specs[0] == (0, 1)
    assert len(column_types) == 5
    assert column_types[0] == str
    assert column_types[1] == int
    assert column_types[2] == int
    assert column_types[3] == float


def test_nc_to_csv():
    """Test the nc_to_csv function with a sample xarray Dataset."""
    # Create a sample xarray Dataset
    times = pd.date_range("2023-01-01", periods=5, freq="D")
    data = xr.Dataset(
        {
            "mf": (["time"], [15.0, 16.5, 14.2, 15.8, 16.0]),
            "mf_repeatability": (["time"], [30, 45, 50, 55, 60]),
        },
        coords={
            "time": times,
        },
    )
    data.attrs["source"] = "Test Data"
    data.attrs["network"] = "agage-test"
    data.attrs["site_code"] = "TEST"
    data.attrs["instrument"] = "Test Instrument"
    data.attrs["species"] = "Test Species"
    data.mf.attrs["units"] = "ppt"
    data.mf_repeatability.attrs["units"] = "Percent"

    header, df = nc_to_csv(data)

    assert header[3] == "# GLOBAL ATTRIBUTES:"

    assert any(["units: ppt" in line for line in header])
    wh_mf = [i for i, line in enumerate(header) if "mf:" in line]
    assert "units: ppt" in header[wh_mf[0] + 1], "Units line for 'mf' not found after 'mf:' line."

    assert any("mf_repeatability:" in line for line in header)

    assert header[-1] == "# DATA:"
    
    # Check if the DataFrame has the expected columns
    expected_columns = ["time", "year", "month", "day", "hour", "minute", "second", "mf", "mf_repeatability"]
    assert all(col in df.columns for col in expected_columns), "DataFrame does not have the expected columns."
    # Check if the DataFrame has the expected number of rows
    assert len(df) == 5, "DataFrame does not have the expected number of rows."
    # Check if the DataFrame has the expected data
    assert df["mf"].tolist() == [15.0, 16.5, 14.2, 15.8, 16.0], "DataFrame 'mf' column does not have the expected data."
    assert df["mf_repeatability"].tolist() == [30, 45, 50, 55, 60], "DataFrame 'mf_repeatability' column does not have the expected data."

    # Check that the year, month, day, hour, minute, second columns are correctly populated
    assert df["year"].tolist() == [2023, 2023, 2023, 2023, 2023], "Year column does not have the expected data."
    assert df["month"].tolist() == [1, 1, 1, 1, 1], "Month column does not have the expected data."
    assert df["day"].tolist() == [1, 2, 3, 4, 5], "Day column does not have the expected data."
    assert df["hour"].tolist() == [0, 0, 0, 0, 0], "Hour column does not have the expected data."
    assert df["minute"].tolist() == [0, 0, 0, 0, 0], "Minute column does not have the expected data."
    assert df["second"].tolist() == [0, 0, 0, 0, 0], "Second column does not have the expected data."


def test_archive_write_csv():
    """Test the archive_write_csv function."""

    with tempfile.TemporaryDirectory() as tmpdirname:
        tmpdir = os.path.abspath(tmpdirname)
        
        zip_path = Path(tmpdir) / "test_archive.zip"
        non_zip_path = Path(tmpdir) / "test_archive"

        filename = "test_output.csv"
        filename_with_subpath = "subfolder/test_output.csv"
        
        data = "col1,col2\n1,2\n3,4"

        archive_write_csv(zip_path, filename, data)
        archive_write_csv(zip_path, filename_with_subpath, data)

        # Check if the file was created
        with ZipFile(zip_path, mode="r") as zip:

            # output_write_csv prepends the archive name to the output filename
            zip_stem = zip_path.stem
            filename_in_zip = f"{zip_stem}/{filename}"
            filename_with_subpath_in_zip = f"{zip_stem}/{filename_with_subpath}"

            # Check if the file exists in the zip
            assert filename_in_zip in zip.namelist(), f"{filename_in_zip} not found in zip"
            assert filename_with_subpath_in_zip in zip.namelist(), f"{filename_with_subpath_in_zip} not found in zip"
            with zip.open(filename_in_zip) as f:
                content = f.read().decode("utf-8-sig")
                assert content == data, f"Content of {filename_in_zip} does not match expected data"
            with zip.open(filename_with_subpath_in_zip) as f:
                content = f.read().decode("utf-8-sig")
                assert content == data, f"Content of {filename_with_subpath_in_zip} does not match expected data"
        
        archive_write_csv(non_zip_path, filename, data)
        archive_write_csv(non_zip_path, filename_with_subpath, data)

        # Check if the file was created
        assert (non_zip_path / filename).exists(), f"{non_zip_path / filename} does not exist"
        with open(non_zip_path / filename, mode="r", encoding="utf-8-sig") as f:
            content = f.read()
            assert content == data, f"Content of {non_zip_path / filename} does not match expected data"
        assert (non_zip_path / filename_with_subpath).exists(), f"{non_zip_path / filename_with_subpath} does not exist"
        with open(non_zip_path / filename_with_subpath, mode="r", encoding="utf-8-sig") as f:
            content = f.read()
            assert content == data, f"Content of {non_zip_path / filename_with_subpath} does not match expected data"
