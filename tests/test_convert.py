import xarray as xr
import pandas as pd
import numpy as np
import warnings

from agage_archive.convert import resample, grouper, resampler, resample_variability,\
                     find_inlet_height_changes, monthly_baseline, scale_convert


def test_scale_convert():
    # Also relying on the tests in openghg_calscales

    def test_dataset(time, species, scale):
        coords = {"time": pd.date_range(time, time)}
        data = {"mf": (["time"], [1.]),
                "mf_repeatability": (["time"], [0.1])}
        ds = xr.Dataset(coords=coords, data_vars=data)
        ds.attrs["species"] = species
        ds.attrs["calibration_scale"] = scale
        return ds
    
    # Create xarray dataset with one time point
    ds = test_dataset("1991-01-01", "cfc-11", "SIO-93")

    # Test a set of conversion factors (chosen because these factors shouldn't change)
    tests = [("cfc-11", "SIO-93", "SIO-98", 1.0082),
             ("cfc-11", "SIO-93", "SIO-05", 1.0026549),
             ("ch4", "CSIRO-94", "TU-87", 1.0119),
             ("n2o", "SIO-93", "SIO-16", 1.0058)]

    for test in tests:
        ds = test_dataset("1991-01-01", test[0], test[1])
        ds_new = scale_convert(ds, test[2])

        # Just assert that values have been scaled
        assert np.allclose(ds_new.mf.values,  ds.mf.values * test[3], rtol=1e-5)
                
    # Period where N2O time conversion applies
    ds = test_dataset("1985-01-01", "n2o", "SIO-93")
    ds_new = scale_convert(ds, "SIO-98")

    # Check that the conversion has been applied
    assert np.allclose(ds_new.mf.values, ds.mf.values * 1.0058 * 0.9962230167482587, rtol=1e-5)


def test_resampler():

    # Create test dataset
    time = pd.date_range(start="2021-01-01", end="2021-01-03", freq="1min")
    data = np.random.rand(len(time))
    
    ds = xr.Dataset({"time": time, 
                "mf": ("time", data),
                "mf_repeatability": ("time", data * 0.1),
                "sampling_period": ("time", np.ones(len(time)) * 60),
                "baseline": ("time", np.ones(len(time))),
                "inlet_height": ("time", np.ones(len(time)) * 10)})

    ds["mf"].attrs["units"] = "1e-9"
    ds["mf"].attrs["calibration_scale"] = "TU-87"

    ds["mf_repeatability"].attrs["units"] = "1e-9"
    ds["mf_repeatability"].attrs["calibration_scale"] = "TU-87"
    ds["mf_repeatability"].attrs["long_name"] = "Repeatability"

    # Set the baseline variable to 0 once every two hours
    ds.baseline.values[::120] = 0

    last_timestamp = ds.time.to_dataframe().index[-1] + pd.Timedelta(ds.sampling_period.values[-1], "s")

    # Test resample function
    df_resample = resampler(ds.to_dataframe(),
                            {"mf": {"resample_method": "mean"},
                            "mf_repeatability": {"resample_method": "standard_error"},
                            "mf_variability": {"resample_method": "std"},
                            "mf_count": {"resample_method": "sum"},
                            "sampling_period": {"resample_method": ""},
                            "baseline": {"resample_method": ""},
                            "inlet_height": {"resample_method": "median"}},
                            last_timestamp,
                            resample_period="3600s")

    # Check that the dataset has been resampled to hourly
    assert df_resample.index.freq == pd.Timedelta("3600s")

    # Check that the baseline variable has been resampled correctly
    assert np.all(df_resample.baseline.values[::2] == 0)
    assert np.all(df_resample.baseline.values[1::2] == 1)

    # Check that the sampling_period variable has been resampled correctly
    assert np.all(df_resample.sampling_period.values[:-1] == 3600)

    # The last point should have a sampling period of 60 seconds
    assert df_resample.sampling_period.values[-1] == 60

    # Check that mf is the mean of the original data
    assert np.isclose(df_resample.mf.values[0], data[:60].mean())

    # Check variability has been calculated correctly
    assert np.isclose(df_resample["mf_variability"].values[0], np.std(data[:60], ddof=0))


def test_grouper():

    # Create test dataset
    time = pd.date_range(start="2021-01-01", end="2021-01-03", freq="1min")

    # Inlet height changes every 20 minutes, starting from some random time point after 2 hours
    inlet_height = np.ones(len(time))*10.
    inlet_height_steps = np.tile(np.concatenate([np.ones(20)*10, np.ones(20)*20]), len(time) // 40 + 1)
    inlet_height[2*60+3:] = inlet_height_steps[:len(inlet_height)-(2*60+3)]

    data = np.random.rand(len(time))

    ds = xr.Dataset({"time": time,
                    "mf": ("time", data),
                    "mf_repeatability": ("time", data * 0.1),
                    "sampling_period": ("time", np.ones(len(time)) * 60),
                    "baseline": ("time", np.ones(len(time))),
                    "inlet_height": ("time", inlet_height)})
    
    ds["mf"].attrs["units"] = "1e-9"
    ds["mf"].attrs["calibration_scale"] = "TU-87"

    ds["mf_repeatability"].attrs["units"] = "1e-9"
    ds["mf_repeatability"].attrs["calibration_scale"] = "TU-87"
    ds["mf_repeatability"].attrs["long_name"] = "Repeatability"

    # Set the baseline variable to 0 once every two hours
    ds.baseline.values[::120] = 0

    inlet_height_changes, inlet_height_change_times, inlet_height_change_times_delta = \
        find_inlet_height_changes(ds)

    # Test resample function
    df_grouped = grouper(ds.to_dataframe(),
                    inlet_height_changes, inlet_height_change_times, inlet_height_change_times_delta,
                    variable_defaults={"mf": {"resample_method": "mean"},
                                    "mf_repeatability": {"resample_method": "standard_error"},
                                    "mf_variability": {"resample_method": "std"},
                                    "mf_count": {"resample_method": "sum"},
                                    "sampling_period": {"resample_method": ""},
                                    "baseline": {"resample_method": ""},
                                    "inlet_height": {"resample_method": "median"}},
                    resample_period="3600s")

    # Check that the difference between each timestamp is equal to the sampling period
    for i in range(1, len(df_grouped.index)):
        assert (df_grouped.index[i] - df_grouped.index[i-1]).seconds == \
            df_grouped.loc[df_grouped.index[i-1], "sampling_period"]

    for i in range(0, len(df_grouped)):
        ds_slice = ds.sel(time=slice(df_grouped.index[i],
                                     df_grouped.index[i] + pd.Timedelta(df_grouped.loc[df_grouped.index[i], "sampling_period"]-1, unit="s")))

        # Check baseline is correctly set
        if 0. in ds_slice.baseline.values:
            assert df_grouped.loc[df_grouped.index[i], "baseline"] == 0
        else:
            assert df_grouped.loc[df_grouped.index[i], "baseline"] == 1
        
        # Check that mean is correct
        assert np.isclose(ds_slice.mf.mean().values, df_grouped.loc[df_grouped.index[i], "mf"])

        # Check that variability is correct
        assert np.isclose(ds_slice.mf.std(ddof=0).values, df_grouped.loc[df_grouped.index[i], "mf_variability"])

        # Check that inlet_height is correctly set
        assert np.isclose(ds_slice.inlet_height.median().values, df_grouped.loc[df_grouped.index[i], "inlet_height"])


def test_resample_variability():

   # Test weighted resampling for variability
    ##################################################

    # Create test dataset
    time = pd.date_range(start="2021-01-01", end="2021-01-02", freq="1min")
    data = np.random.rand(len(time))

    inlet_height = np.ones(len(time)) * 10

    df = pd.DataFrame({"time": time,
                        "mf": data,
                        "mf_repeatability": data * 0.1,
                        "mf_count": np.ones(len(time)),
                        "sampling_period": np.ones(len(time)) * 60,
                        "inlet_height": inlet_height,
                        "baseline": np.ones(len(time))})
    df.set_index("time", inplace=True)

    df_10min = df.resample("600S", closed="left", label="left").agg({"mf": "mean",
                                                                    "mf_repeatability": "mean",
                                                                    "mf_count": "sum",
                                                                    "sampling_period": "first",
                                                                    "inlet_height": "first",
                                                                    "baseline": "prod"})

    # Create a 10-minute average of the data, which we'll use as an intermediate step
    var_10min = resample_variability(df, ["600S"])
    assert np.isclose(var_10min.values[0], np.std(data[:10], ddof=0))

    df_10min["mf_variability"] = var_10min

    # Now resample a second time, this time to hourly
    var_60min = resample_variability(df_10min, ["3600S"])
    assert np.isclose(var_60min.values[0], np.std(data[:60], ddof=0))


def test_resample():
    """Test resample function"""

    # First test straight resampling
    ##################################################

    # Create test dataset
    time = pd.date_range(start="2021-01-01", end="2021-01-02", freq="1min")
    data = np.random.rand(len(time))

    inlet_height = np.ones(len(time)) * 10

    ds = xr.Dataset({"time": time, 
                    "mf": ("time", data),
                    "mf_repeatability": ("time", data * 0.1),
                    "sampling_period": ("time", np.ones(len(time)) * 60),
                    "inlet_height": ("time", inlet_height),
                    "baseline": ("time", np.ones(len(time)))})

    ds["mf"].attrs["units"] = "1e-9"
    ds["mf"].attrs["calibration_scale"] = "TU-87"

    ds["mf_repeatability"].attrs["units"] = "1e-9"
    ds["mf_repeatability"].attrs["calibration_scale"] = "TU-87"
    ds["mf_repeatability"].attrs["long_name"] = "Repeatability"

    ds.attrs["version"] = "test"
    ds.attrs["species"] = "ch4"
    ds.attrs["comment"] = "This is a test dataset"

    # Set the baseline variable to 0 once every two hours
    ds.baseline.values[::120] = 0

    # Test resample function
    ds_resample = resample(ds, resample_period="3600s", resample_threshold="600s")

    # Check that the dataset has been resampled to hourly
    assert ds_resample.time.diff("time").median() == np.timedelta64(3600, "s")

    # Check that the baseline variable has been resampled correctly
    assert np.all(ds_resample.baseline.values[::2] == 0)
    assert np.all(ds_resample.baseline.values[1::2] == 1)

    # Check that the sampling_period variable has been resampled correctly
    assert np.all(ds_resample.sampling_period.values[:-1] == 3600)

    # The last point should have a sampling period of 60 seconds
    assert ds_resample.sampling_period.values[-1] == 60

    # Check that mf is the mean of the original data
    assert np.isclose(ds_resample.mf.values[0], data[:60].mean())

    # Check variability has been calculated correctly
    assert np.isclose(ds_resample.mf_variability.values[0], np.std(data[:60], ddof=0))

    # Check that units are consistent
    assert ds_resample.mf_variability.attrs["units"] == ds_resample.mf.attrs["units"]
    assert ds_resample.mf_variability.attrs["calibration_scale"] == ds_resample.mf.attrs["calibration_scale"]
    assert ds_resample.mf_repeatability.attrs["units"] == ds_resample.mf.attrs["units"]

    # Test that all the output attributes are the same as the input attributes
    for attr in ds.attrs.keys():
        if attr.lower() == "comment":
            assert "resampled" in ds_resample.attrs[attr].lower()
        else:
            assert ds_resample.attrs[attr] == ds.attrs[attr]


    # Second test resampling and grouping by inlet height
    ##################################################

    # Create test dataset
    time = pd.date_range(start="2021-01-01", end="2021-01-03", freq="1min")

    # Inlet height changes every 20 minutes, starting from some random time point after 2 hours
    inlet_height = np.ones(len(time))*10.
    inlet_height_steps = np.tile(np.concatenate([np.ones(20)*10, np.ones(20)*20]), len(time) // 40 + 1)
    inlet_height[2*60+3:] = inlet_height_steps[:len(inlet_height)-(2*60+3)]

    data = np.random.rand(len(time))

    ds = xr.Dataset({"time": time,
                    "mf": ("time", data),
                    "mf_repeatability": ("time", data * 0.1),
                    "sampling_period": ("time", np.ones(len(time)) * 60),
                    "baseline": ("time", np.ones(len(time))),
                    "inlet_height": ("time", inlet_height)})
    
    ds["mf"].attrs["units"] = "1e-9"
    ds["mf"].attrs["calibration_scale"] = "TU-87"

    ds["mf_repeatability"].attrs["units"] = "1e-9"
    ds["mf_repeatability"].attrs["calibration_scale"] = "TU-87"
    ds["mf_repeatability"].attrs["long_name"] = "Repeatability"

    # Set the baseline variable to 0 once every two hours
    ds.baseline.values[::120] = 0

    ds.attrs["version"] = "test"
    ds.attrs["species"] = "ch4"
    ds.attrs["comment"] = "This is a test dataset"

    # Test resample function
    ds_resample = resample(ds, resample_period="3600s", resample_threshold="600s")

    # Test that inlets are either 10 or 20, but not fractional values
    assert np.all(ds_resample.inlet_height.values % 10 == 0)

    #TODO: Add more tests for resampling by inlet height


    # Test weighted resampling for variability
    ##################################################

    # Create test dataset
    time = pd.date_range(start="2021-01-01", end="2021-01-02", freq="1min")
    data = np.random.rand(len(time))

    inlet_height = np.ones(len(time)) * 10

    ds = xr.Dataset({"time": time, 
                    "mf": ("time", data),
                    "mf_repeatability": ("time", data * 0.1),
                    "sampling_period": ("time", np.ones(len(time)) * 60),
                    "inlet_height": ("time", inlet_height),
                    "baseline": ("time", np.ones(len(time)))})

    ds["mf"].attrs["units"] = "1e-9"
    ds["mf"].attrs["calibration_scale"] = "TU-87"

    ds["mf_repeatability"].attrs["units"] = "1e-9"
    ds["mf_repeatability"].attrs["calibration_scale"] = "TU-87"
    ds["mf_repeatability"].attrs["long_name"] = "Repeatability"

    ds.attrs["version"] = "test"
    ds.attrs["species"] = "ch4"
    ds.attrs["comment"] = "This is a test dataset"

    # Create a 10-minute average of the data, which we'll use as an intermediate step
    ds_10min = resample(ds, resample_period="600S")
    assert ds_10min.time.diff("time").median() == np.timedelta64(600, "s")
    assert np.isclose(ds_10min.mf_variability[0].values, np.std(data[:10], ddof=0))

    # Now resample a second time, this time to hourly
    ds_60min = resample(ds_10min, resample_period="3600S", resample_threshold="6000S")
    assert ds_60min.time.diff("time").median() == np.timedelta64(3600, "s")
    assert np.isclose(ds_60min.mf_variability[0].values, np.std(data[:60], ddof=0))


def test_monthly_baseline():
    
    # Create a sample dataset
    time = pd.date_range("1991-01-01", "1991-12-31", freq="D")
    data = {"mf": (["time"], np.random.rand(len(time))),
            "mf_repeatability": (["time"], np.random.rand(len(time))),
            "sampling_period": (["time"], np.ones(len(time)) * 60),
            "inlet_height": (["time"], np.ones(len(time))),
            "instrument_type": (["time"], np.ones(len(time))),}
    ds = xr.Dataset(coords={"time": time}, data_vars=data)
    ds.mf.attrs["units"] = "1e-12"
    ds.mf.attrs["calibration_scale"] = "TU-87"
    ds.mf_repeatability.attrs["units"] = "1e-12"
    ds.mf_repeatability.attrs["calibration_scale"] = "TU-87"
    ds.mf_repeatability.attrs["long_name"] = "Repeatability"
    ds.attrs["version"] = "test"
    ds.attrs["species"] = "ch4"
    ds.attrs["calibration_scale"] = "TU-87"
    ds.attrs["network"] = "agage_test"

    # Create a sample baseline dataset
    baseline_data = {"baseline": (["time"], np.random.choice([0, 1], size=len(time)))}
    # Make sure that the final baseline value is 1 
    # (otherwise final points are sometimes filtered out, which messes up sampling_period test)
    baseline_data["baseline"][-1][-1] = 1
    ds_baseline = xr.Dataset(coords={"time": time}, data_vars=baseline_data)
    ds_baseline.attrs["baseline_flag"] = "Baseline Flag"

    ds_baseline_points = ds.where(ds_baseline.baseline == 1, drop=True)

    # Call the monthly_baseline function
    ds_monthly = monthly_baseline(ds, ds_baseline)

    # Check if the monthly mean is calculated correctly
    assert np.allclose(ds_monthly.mf.values, ds_baseline_points.mf.resample(time="1MS").mean().values)

    # Check if the monthly standard deviation is calculated correctly
    expected_std = ds_baseline_points.mf.resample(time="1MS").std(ddof=0)
    assert np.allclose(ds_monthly.mf_variability.values, expected_std.values)
    assert ds_monthly.mf_variability.attrs["long_name"] == "Monthly standard deviation of baseline mole fractions"
    assert ds_monthly.mf_variability.attrs["units"] == ds.mf.attrs["units"]

    # Check if the monthly standard error in mean is calculated correctly
    expected_se = ds_baseline_points.mf_repeatability.resample(time="1MS").mean() / np.sqrt(ds_baseline_points.mf.resample(time="1MS").count())
    assert np.allclose(ds_monthly.mf_repeatability, expected_se)
    assert ds_monthly.mf_repeatability.attrs["long_name"] == "Monthly standard error in mean of baseline mole fractions"
    assert ds_monthly.mf_repeatability.attrs["units"] == ds.mf.attrs["units"]

    # Check if the baseline flag is added correctly
    assert ds_monthly.attrs["baseline_flag"] == ds_baseline.attrs["baseline_flag"]

    # Test that the sampling_period is correctly set
    assert ds_monthly.sampling_period.values[0] == 31*24*60*60
    assert ds_monthly.sampling_period.values[1] == 28*24*60*60
    assert ds_monthly.sampling_period.values[-2] == 30*24*60*60
    # For december, sampling period should be 30 days and one minute
    #  because final period is 1 - 31 Dec (midnight) + the 1 minute sampling period
    assert ds_monthly.sampling_period.values[-1].astype(int) == (30*24*60*60 + 60)

    # Check that a version number is present
    assert "version" in ds_monthly.attrs.keys()
