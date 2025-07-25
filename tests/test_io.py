import pandas as pd
import xarray as xr
import numpy as np
import json

from agage_archive.config import Paths, open_data_file
from agage_archive.io import read_ale_gage, read_nc, combine_datasets, read_nc_path, \
    read_baseline, combine_baseline, output_dataset, read_gcwerks_flask, drop_duplicates, \
    read_gcms_magnum_file, read_gcms_magnum, define_instrument_type
from agage_archive.convert import scale_convert
from agage_archive.definitions import instrument_type_definition, get_instrument_number
from agage_archive.definitions import nc4_types


paths = Paths("agage_test")


def type_test(var, var_name):

    with open_data_file("variables.json") as f:
        variables_defs = json.load(f)

    assert type(var).__name__ == nc4_types[variables_defs[var_name]["encoding"]["dtype"]]


def test_read_ale_gage():

    species = "ch3ccl3"

    ds_ale = read_ale_gage("agage_test", species, "CGO", "ALE",
                           dropna=False)

    # test UTC conversion
    ds_ale_local = read_ale_gage("agage_test", species, "CGO", "ALE",
                                 utc=False, data_exclude=False, dropna=False)
    
    # check that all df_ale_local timestamps are 10 hours ahead of df_ale (CGO is UTC+10)
    assert ((ds_ale_local.time.to_series().index - ds_ale.time.to_series().index) == pd.Timedelta("10h")).all()

    # Check that some data have been excluded
    assert ds_ale.mf.to_series().isnull().sum() > ds_ale_local.mf.to_series().isnull().sum()

    ds_ale_noscale = read_ale_gage("agage_test", species, "CGO", "ALE",
                                scale=None, dropna=False)

    # Check that the scale conversion has been applied (ch3ccl3 factors from openghg_calscales)
    assert np.isclose(np.nanmean(ds_ale.mf.values / ds_ale_noscale.mf.values), 0.9957*1.0184)

    # Check that an instrument_type attribute has been added
    assert ds_ale.attrs["instrument_type"] == "ALE"

    # check that dropna works
    ds_ale_dropna = read_ale_gage("agage_test", species, "CGO", "ALE",
                                  dropna=True)
    assert ds_ale_dropna.mf.notnull().all()

    # Check that types are all as specified in variables.json
    for var_name in ds_ale.data_vars.keys():
        type_test(ds_ale[var_name].values[0], var_name)


def test_combine_datasets():

    network = "agage_test"

    species = "ch3ccl3"

    ds = combine_datasets(network, species, "CGO")

    ds_ale_noscale = read_ale_gage(network, species, "CGO", "ALE", scale=None)

    # Check that the scale conversion has been applied
    assert np.isclose(np.nanmean(ds.reindex_like(ds_ale_noscale).mf.values / ds_ale_noscale.mf.values),
        0.9957*1.0184, rtol=0.00001)

    ds_gage_noscale = read_ale_gage(network, species, "CGO", "GAGE", scale=None)

    # Check that the scale conversion has been applied
    assert np.isclose(np.nanmean(ds.reindex_like(ds_gage_noscale).mf.values / ds_gage_noscale.mf.values),
        0.9957*1.0184, rtol=0.00001)

    # Test that a version number has been added
    assert "version" in ds.attrs.keys()
    assert ds.attrs["version"] != ""

    # Test that the instrument_type attribute has been added
    assert ds.attrs["instrument_type"] == "ALE/GAGE/GCMD/GCMS-Medusa"

    # Check that types are all as specified in variables.json
    for var_name in ds.data_vars.keys():
        type_test(ds[var_name].values[0], var_name)


def test_read_nc_path():

    nc_file, sub_path = read_nc_path("agage_test", "CH3CCl3", "CGO", "GCMS-Medusa")
    assert nc_file == "AGAGE-GCMS-Medusa_CGO_ch3ccl3.nc"
    assert sub_path == "data-gcms-nc"

    nc_file, sub_path = read_nc_path("agage_test", "NF3", "MHD", "GCMS-Medusa")
    assert nc_file == "AGAGE-GCMS-Medusa_MHD_nf3.nc"
    assert sub_path == "data-gcms-nc"

    nc_file, sub_path = read_nc_path("agage_test", "CH3CCl3", "CGO", "GCMD")
    assert nc_file == "AGAGE-GCMD_CGO_ch3ccl3.nc"
    assert sub_path == "data-nc"


def test_read_baseline():

    for flag_name in ["git_pollution_flag"]:
#    for flag_name in ["met_office_baseline_flag", "git_pollution_flag"]:

        ds_baseline = read_baseline("agage_test",
            "CH3CCl3", "CGO", "GCMS-Medusa",
            flag_name = flag_name)

        # Check baseline flags exist and has integer values
        assert "baseline" in ds_baseline.data_vars.keys()
        assert isinstance(ds_baseline.baseline.values[0], np.int8)

        # Check some attributes
        assert ds_baseline.attrs["species"] == "ch3ccl3"
        assert ds_baseline.attrs["instrument"] == "GCMS-Medusa"
        assert ds_baseline.attrs["site_code"] == "CGO"
        assert "citation" in ds_baseline.attrs.keys()

        assert ds_baseline.time.attrs["long_name"] == "time"

        # Check that the right baseline flag has been extracted
        if flag_name == "met_office_baseline_flag":
            assert "Met Office" in ds_baseline.attrs["comment"]
        elif flag_name == "git_pollution_flag":
            assert "Georgia Tech" in ds_baseline.attrs["comment"]

        # Test outputting baseline dataset
        output_dataset(ds_baseline, "agage_test", instrument="GCMS-Medusa",
            output_subpath="baselines/",
            extra = "-git-baseline",
            verbose=False)


def test_combine_baseline():

    ds_baseline = combine_baseline("agage_test", "CH3CCl3", "CGO")

    # Check baseline flags exist and has integer values
    assert "baseline" in ds_baseline.data_vars.keys()
    assert isinstance(ds_baseline.baseline.values[0], np.int8)

    # Check some attributes
    assert ds_baseline.attrs["species"] == "ch3ccl3"
    assert ds_baseline.attrs["site_code"] == "CGO"
    assert "citation" in ds_baseline.attrs.keys()

    assert ds_baseline.time.attrs["long_name"] == "time"

    # Check that ds_baseline has the same timestamps as the output of combine_datasets
    ds = combine_datasets("agage_test", "CH3CCl3", "CGO", verbose=False)
    assert (ds_baseline.time.values == ds.time.values).all()

    # Test that a version number has been added
    assert "version" in ds_baseline.attrs.keys()
    assert ds_baseline.attrs["version"] != ""


def test_timestamp():
    """Test that some timestamps are read correctly and corrected for the sampling time offset
    """

    # Medusa files have a substantial sampling period
    filename, sub_path = read_nc_path("agage_test", "CH3CCl3", "CGO", "GCMS-Medusa")

    with open_data_file(filename, network="agage_test", sub_path=sub_path) as f:
        ds_original = xr.open_dataset(f).load()
        
    ds = read_nc("agage_test", "CH3CCl3", "CGO", "GCMS-Medusa", dropna=False)

    time_offset = int(ds_original.time.attrs["sampling_time_seconds"])
    assert np.all(ds_original.time.values == ds.time.values + pd.Timedelta(seconds=time_offset)/2)

    assert ds.time.attrs["long_name"] == "time"
    assert ds.time.attrs["comment"] == "Timestamp is the start of the sampling period in UTC"


def test_picarro():
    """Test that Picarro file is read correctly, resampled to hourly and that the timestamp is at the beginning of the sampling period
    """

    filename, sub_path = read_nc_path("agage_test", "ch4", "THD", "Picarro")
    
    # Original data
    with open_data_file(filename, network="agage_test", sub_path=sub_path) as f:
        ds_original = xr.open_dataset(f).load()
    
    # Output of read_nc
    ds = read_nc("agage_test", "ch4", "THD", "Picarro")

    # Apply any scale conversion
    ds_original = scale_convert(ds_original, ds.attrs["calibration_scale"])

    # Find the first hour of the dataset
    first_hour = ds_original.time.dt.floor("h").min()
    first_hour_mean = ds_original.mf.sel(time=slice(first_hour, first_hour + pd.Timedelta("1h"))).mean()

    # Check that the dataset has been resampled to hourly
    assert np.isclose(ds.mf.values[0], first_hour_mean.values, rtol=0.0001)

    # Check that timestamp is at the beginning of the sampling period
    assert ds.time[0].values == first_hour.values

    # Check that the time variable has the correct attributes
    assert ds.time.attrs["long_name"] == "time"
    assert "Timestamp is the start of the sampling period in UTC" in ds.time.attrs["comment"]
    assert "Resampled" in ds.time.attrs["comment"]  

    # Check that instrument_type has been added
    assert ds.attrs["instrument_type"] == "Picarro"

    # Check that NaNs are removed
    assert ds.mf.notnull().all()


def test_picarro_inlet_grouping():
    """This is mostly a test of the inlet grouping in the Picarro data"""

    filename, sub_path = read_nc_path("agage_test", "ch4", "TAC", "Picarro")
    
    # Original data
    with open_data_file(filename, network="agage_test", sub_path=sub_path) as f:
        ds_original = xr.open_dataset(f).load()
    
    # Output of read_nc
    ds = read_nc("agage_test", "ch4", "TAC", "Picarro", dropna=False)

    # Apply any scale conversion
    ds_original = scale_convert(ds_original, ds.attrs["calibration_scale"])

    # Unique input and output inlet values should be the same
    assert np.all(np.unique(ds_original.inlet_height.values) == np.unique(ds.inlet_height.values))

    # In this dataset, the first and last outputs should be at inlet_heights of 185 and 54 respectively
    assert ds.inlet_height[0].values == 185
    assert ds.inlet_height[-1].values == 54

    # Check that the sum of the original and new datasets are the same for each inlet
    # Ideally, we'd do this with the means, but it's an average of an average
    for inlet in np.unique(ds_original.inlet_height.values):
        sum_original = ds_original.groupby("inlet_height").sum().mf_mean_N.sel(inlet_height=inlet).values
        sum_new = ds.mf_count.where(ds.inlet_height == inlet).sum().values

        # Check that the sum of the original and new datasets are the same
        assert np.isclose(sum_original, sum_new, rtol=0.0001)

    # Check that at least the first mean is OK
    first_inlet_change_index = 0
    while ds_original.inlet_height[first_inlet_change_index] == ds_original.inlet_height[0]:
        first_inlet_change_index += 1
    
    assert np.isclose(ds_original.mf.sel(time=slice(ds_original.time[0], ds_original.time[first_inlet_change_index])).mean().values,
                      ds.mf.isel(time=0).values, rtol=0.00001)

    # Check that the first time difference is correct
    timediff_original = (ds_original.time.isel({"time": first_inlet_change_index+1}).values - \
                          ds_original.time.isel({"time": 0}).values).astype("timedelta64[s]").astype(int)

    assert ds.sampling_period.isel({"time": 0}).values == timediff_original


def test_read_gcwerks_flask():

    ds = read_gcwerks_flask("agage_test", "cf4", "CBW", "GCMS-Medusa-flask")

    # Check that the dataset has the correct attributes
    assert ds.attrs["species"] == "cf4"

    # First sample should be at 2021-02-15 1300 minus 30 minutes
    assert ds.time[0].values == pd.Timestamp("2021-02-15 1230")

    # Check that the time variable has the correct attributes
    assert ds.time.attrs["long_name"] == "time"
    assert "Timestamp is the start of sampling period" in ds.time.attrs["comment"]

    # Check that some attributes that are only in site attributes have been added
    assert "sampling_period" in ds.attrs.keys()
    assert ds.attrs["inlet_latitude"] == 2


def test_drop_duplicates():

    n = 100
    # Create a dataset with some duplicate timestamps
    time = pd.date_range("2023-01-01", periods=n, freq="H")
    mf = xr.DataArray(np.random.rand(n), dims="time", coords={"time": time})
    mf_repeatability = xr.DataArray(np.random.rand(n), dims="time", coords={"time": time})
    instrument_type = xr.DataArray(np.repeat(1, n), dims="time", coords={"time": time})
    ds = xr.Dataset(data_vars={"mf": mf, "mf_repeatability": mf_repeatability, "instrument_type": instrument_type})

    # Add 3 duplicate timestamps
    ds_duplicated = ds.isel(time=slice(10, 13), drop=True).copy(deep=True)
    ds_duplicated["instrument_type"].values = np.repeat(2, len(ds_duplicated.time))

    # The first duplicate should remove instrument 2, because it's NaN
    ds_duplicated["mf"].values[0] = np.nan

    # The second duplicate should keep instrument 1, because it appears earlier
    # Therefore, the value "10" should be removed
    ds_duplicated["mf"].values[1] = 10
    
    # The third duplicate should keep instrument 2, because we'll make instrument 1 a NaN
    # Therefore, the value "20" should be removed
    ds_duplicated["mf"].values[2] = 20
    ds.mf.values[12] = np.nan

    # Create the dataset with duplicates
    ds = xr.concat([ds, ds_duplicated], dim="time")
    ds = ds.sortby("time")

    # Drop duplicates
    ds = drop_duplicates(ds)

    # Check that there are no duplicate timestamps
    assert len(ds.time) == len(ds.time.drop_duplicates(dim="time"))

    # Check that the new dataset is the same length as the original
    assert len(ds.time) == n

    # Check that the first duplicate has been removed
    assert 10 not in ds.mf.values
    assert 20 in ds.mf.values
    assert ds.mf.values[11] != 10
    assert ds.instrument_type.values[11] == 1
    assert ds.mf.values[12] == 20
    assert ds.instrument_type.values[12] == 2


def test_read_gcms_magnum_file():

    species = "HFC-134a"

    # Test file is in a subset of the tar archive, stored in a folder with the same name
    sub_path = paths.magnum_path.split(".tar.gz")[0]

    with open_data_file("MHD-ads_1994.dap",
                        network="agage_test",
                        sub_path=sub_path) as file:
        df, scale = read_gcms_magnum_file(file, species)

    assert df.index[0] == pd.Timestamp("1994-10-13 23:54:00")
    assert df.index[-1] == pd.Timestamp("1994-12-31 19:44:00")

    assert df["mf"].iloc[0] == 9.303
    assert not np.isfinite(df["mf"].iloc[3])
    assert df["mf"].iloc[-1] == 1.781

    assert df["baseline"].iloc[0] == 0
    assert df["baseline"].iloc[16] == 1
    assert df["baseline"].iloc[-1] == 1

    assert scale == "SIO-05"

    # Test another species
    species = "HCFC-142b"

    with open_data_file("MHD-ads_1994.dap",
                        network="agage_test",
                        sub_path=sub_path) as file:
        df, scale = read_gcms_magnum_file(file, species)

    assert df.index[0] == pd.Timestamp("1994-10-13 23:54:00")
    assert df["mf"].iloc[0] == 36.356

    assert scale == "SIO-05"


def test_read_gcms_magnum():

    network = "agage_test"
    species = "hfc-134a"

    ds = read_gcms_magnum(network, species,
                        verbose = True,
                        scale = "defaults",
                        baseline = False,
                        resample = False,
                        dropna = True)

    assert ds.attrs["species"] == "hfc-134a"
    assert ds.attrs["instrument_type"] == "GCMS-Magnum"
    assert ds.attrs["product_type"] == "mole fraction"
    assert ds.attrs["frequency"] == "high-frequency"
    assert ds.attrs["site_code"] == "MHD"    

    assert ds.time.dt.year[0] == 1994
    assert ds.time.dt.month[0] == 10
    assert ds.time.dt.day[0] == 13
    assert ds.time.dt.hour[0] == 23
    assert ds.time.dt.hour[0] == 23
    assert ds.time.dt.minute[0] == 54        

    # Release schedule says that the last year should be 1998
    assert ds.time.dt.year[-1] == 1998

    assert np.isclose(ds.mf.values[0], 9.303)

    assert ds.sampling_period.values[0] == 2400


def test_define_instrument_type():

    ds = xr.Dataset(data_vars={"mf": ("time", np.random.rand(10)),
                                "mf_repeatability": ("time", np.random.rand(10)),
                                "inlet_height": ("time", np.random.rand(10)),
                                "sampling_period": ("time", np.random.rand(10))},
                    coords={"time": pd.date_range("2023-01-01", periods=10, freq="H")})

    instrument = "ALE"

    ds.attrs["network"] = "agage_test"

    ds = define_instrument_type(ds, instrument)

    instrument_number, instrument_type_str = instrument_type_definition(ds.attrs["network"])

    assert ds["instrument_type"].attrs["long_name"] == "Instrument type"
    assert ds["instrument_type"].attrs["comment"] == instrument_type_str
    assert ds["instrument_type"].values[0] == get_instrument_number(instrument, ds.attrs["network"])
