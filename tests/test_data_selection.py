import numpy as np
import pandas as pd
import xarray as xr

from agage_archive.data_selection import read_data_exclude, calibration_scale_default, \
                                        read_data_combination, read_release_schedule, \
                                        choose_scale_defaults_file


def test_choose_scale_defaults_file():
    """ Test the choose_scale_defaults_file function """

    # Test with instrument and site
    # Note that the test file is called "scale_defaults_test_cgo.csv"
    assert choose_scale_defaults_file("agage_test", "test", site="CGO") == "defaults_test_cgo"

    # Test with instrument only: should return the defaults file for that instrument, since this file isn't there
    assert choose_scale_defaults_file("agage_test", "GCMD") == "defaults"

    # Test with the "test" instrument and no site: should return the defaults file for that instrument
    assert choose_scale_defaults_file("agage_test", "test") == "defaults_test"


def test_calibration_scale_defaults():
    '''Test calibration_scale_default function'''

    assert calibration_scale_default("agage_test", "CO2") == "WMO-X2019"
    assert calibration_scale_default("agage_test", "CH4") == "TU-87"
    assert calibration_scale_default("agage_test", "CO2", scale_defaults_file="defaults_test") == "TESTING"
    assert calibration_scale_default("agage_test", "CO2", scale_defaults_file="defaults_test_cgo") == "TESTING-CGO"
    

def test_read_data_exclude():
    '''Test read_data_exclude function'''

    # ALE CGO CH3CCl3 is flagged at 1978-10-12 06:00 - 07:00
    # Let's test that that point is removed, surrounding points are not
    ds = xr.Dataset(
        {
            "mf": (["time"], np.array([1., 2., 3.]))
        },
        coords = {
            "time": pd.date_range(start="1978-10-12 05:30", 
                                  end = "1978-10-12 07:30",
                                  periods=3)
        }
    )

    ds.attrs["network"] = "agage_test"

    ds = read_data_exclude(ds, "ch3ccl3", "CGO", "ALE")

    #This one should be nan
    assert np.isnan(ds["mf"][1].values)
    #These should not be nan
    assert not np.isnan(ds["mf"][0].values)
    assert ds["mf"][0].values == 1
    assert not np.isnan(ds["mf"][2].values)
    assert ds["mf"][2].values == 3

    # Test that the combined_only column works as expected
    # ALE CGO CH3CCl3 is flagged for combined only at 1972-01-01 06:00 - 07:00
    # Let's test that that point is removed, surrounding points are not, if a combined file is being created
    for combined in [True, False]:
        ds = xr.Dataset(
            {
                "mf": (["time"], np.array([1., 2., 3.]))
            },
            coords = {
                "time": pd.date_range(start="1972-01-01 05:30", 
                                    end = "1972-01-01 07:30",
                                    periods=3)
            }
        )

        ds.attrs["network"] = "agage_test"

        ds = read_data_exclude(ds, "ch3ccl3", "CGO", "ALE", combined=combined)

        #This one should be nan
        if combined:
            assert np.isnan(ds["mf"][1].values)
        else:
            assert not np.isnan(ds["mf"][1].values)
            assert ds["mf"][1].values == 2

        #These should not be nan
        assert not np.isnan(ds["mf"][0].values)
        assert ds["mf"][0].values == 1
        assert not np.isnan(ds["mf"][2].values)
        assert ds["mf"][2].values == 3


def test_read_release_schedule():
    '''Test read_release_schedule function'''

    df = read_release_schedule("agage_test", "GCMD",
                          species = None,
                          site = None)

    # Check that full dataframe has been returned
    assert df.shape == (10, 5)

    # Check that the default release date has been input in the relevant cells
    assert df.loc["cfc-11", "MHD"] == "2023-01-01 00:00"

    # Check some species-specific dates
    assert df.loc["ccl4", "SMO"] == "2019-11-21 00:00"
    assert df.loc["ch4", "RPB"] == "2017-02-15 00:00"

    # Return for one species and site
    assert read_release_schedule("agage_test", "GCMD", species = "cfc-113", site = "RPB") \
        == "2005-07-31 00:00"


def test_read_data_combination():

    instrument_dates = read_data_combination("agage_test", "ch3ccl3", "CGO")

    assert isinstance(instrument_dates, dict)

    assert instrument_dates["ALE"][0] == None
    assert instrument_dates["ALE"][1] == "1984-01-01 00:00"

    assert instrument_dates["GCMD"][0] == "1994-03-15 00:00"
    assert instrument_dates["GCMD"][1] == "2010-06-01 00:00"
