import xarray as xr
import json
import pandas as pd
import numpy as np
from zipfile import ZipFile
from io import StringIO
import json

from agage_archive.config import Paths, open_data_file, data_file_list, \
    output_path
from agage_archive.convert import scale_convert, separate_inlets
from agage_archive.convert import resample as resample_function
from agage_archive.formatting import format_species, \
    format_variables, format_attributes, format_species_flask
from agage_archive.data_selection import read_release_schedule, read_data_exclude, \
    read_data_combination, calibration_scale_default
from agage_archive.definitions import instrument_type_definition, get_instrument_type, get_instrument_number
from agage_archive.util import tz_local_to_utc


gcwerks_species = {"c2f6": "pfc-116",
                   "c3f8": "pfc-218",
                   "c4f8": "pfc-318",
                   "ccl2ccl2": "pce",
                   "chclccl2": "tce",
                   "c6h6": "benzene",
                   "c6h5ch3": "toluene",
                   "c3h8": "propane",
                   "c2h6": "ethane",
                   "c2h2": "ethyne",
                   "c3h6": "c-propane",
                   }

baseline_attrs = {"git_pollution_flag":{
                    "comment": "Baseline flag from the Georgia Tech statistical filtering algorithm.",
                    "citation": "O'Doherty et al. (2001)",
                    "contact": "Ray Wang, Georgia Tech",
                    "contact_email": "raywang@eas.gatech.edu"
                    },
                "met_office_baseline_flag":{
                    "comment": "Baseline flag from the Met Office using the NAME model.",
                    "citation": "",
                    "contact": "Alistair Manning, Met Office",
                    "contact_email": "alistair.manning@metoffice.gov.uk"
                    },
                }


def drop_duplicates(ds):
    """Drop duplicate timestamps in a dataset
    Preferentially removes NaNs, and then removes duplicates based on the order of instrument types

    Args:
        ds (xarray.Dataset): Dataset

    Returns:
        xarray.Dataset: Dataset with duplicates removed
    """

    # Return if there are no duplicates
    if len(ds.time) == len(ds.time.drop_duplicates(dim="time")):
        return ds

    # Find list of instrument_types, and sort them by the order in which they appeared
    instrument_types = []
    for instrument in ds.instrument_type.values:
        if instrument not in instrument_types:
            instrument_types.append(instrument)

    # Create a column to drop duplicates
    ds["drop"] = xr.full_like(ds.time, 1., dtype=float)
    ds["i"] = xr.full_like(ds.time, 0, dtype=int)
    ds["i"].values = np.arange(0, len(ds.time), 1)

    timestamps = ds.time.to_series()
    duplicated_timestamps = timestamps[timestamps.duplicated(keep="first")]

    for timestamp in duplicated_timestamps:

        ds_duplicates = ds.sel(time=timestamp)
        
        # Is the mf a NaN for any of these?
        if "inlet" in ds.dims:
            i_nan = ds_duplicates["i"][np.isnan(ds_duplicates.mf.mean(dim="inlet").values)].values
        else:
            i_nan = ds_duplicates["i"][np.isnan(ds_duplicates.mf.values)].values

        # If they are all NaN, drop all but the first
        if len(i_nan) == len(ds_duplicates["i"]):
            ds["drop"].values[i_nan[1:]] = np.nan
        elif len(i_nan) > 0:
            # If there are one or more NaNs, drop them
            ds["drop"].values[i_nan] = np.nan
        else:
            pass
        
        # If there is more than one remaining value that isn't a NaN, 
        # drop the one which appears first in the instrument_types list
        if "inlet" in ds.dims:
            i_not_nan = ds_duplicates["i"][~np.isnan(ds_duplicates.mf.mean(dim="inlet").values)].values
        else:
            i_not_nan = ds_duplicates["i"][~np.isnan(ds_duplicates.mf.values)].values
        if len(i_not_nan) > 1:
            instruments_not_nan = [ds["instrument_type"].values[i] for i in i_not_nan]
            instrument_to_keep = instrument_types[min([instrument_types.index(instrument) for instrument in instruments_not_nan])]
            i_to_keep = [i for i in i_not_nan if ds["instrument_type"].values[i] == instrument_to_keep][0]
            i_to_drop = [i for i in i_not_nan if i != i_to_keep]
            ds["drop"].values[i_to_drop] = np.nan

    ds = ds.dropna(dim="time", subset=["drop"])

    # Remove the i and drop variables
    ds = ds.drop_vars(["i", "drop"])

    return ds


def read_nc_path(network, species, site, instrument):
    """Find path to netCDF file

    Args:
        network (str): Network
        species (str): Species
        site (str): Site
        instrument (str): Instrument

    Raises:
        ValueError: Instrument must be one of GCMD, GCECD, Picarro, LGR, GCMS-ADS, GCMS-Medusa, GCMS-MteCimone

    Returns:
        str: Path to netCDF file
        str: Sub-path within data directory
    """
    
    paths = Paths(network)

    species_search = format_species(species)
    if species_search in gcwerks_species:
        species_search = gcwerks_species[species_search]

    gcmd_instruments = ["GCMD", "GCECD", "GCPDD"]
    optical_instruments = ["Picarro", "LGR"]
    gcms_instruments = ["GCMS-ADS", "GCMS-Medusa", "GCMS-MteCimone", "GCTOFMS"]

    # Determine sub-path within data directory
    sub_path = None

    for gcmd_instrument in gcmd_instruments:
        if gcmd_instrument in instrument:
            sub_path = paths.md_path
            break
    for optical_instrument in optical_instruments:
        if optical_instrument in instrument:
            sub_path = paths.optical_path
            break
    for gcms_instrument in gcms_instruments:
        if gcms_instrument in instrument:
            sub_path = paths.gcms_path
            break
    
    if sub_path == None:
        raise ValueError(f"Instrument must be one of {gcmd_instruments} {gcms_instruments}")

    # search for netcdf files matching instrument, site and species
    nc_files = data_file_list(network, sub_path, f"*-{instrument}*_{site}_{species_search}.nc")[2]

    if len(nc_files) == 0:
        raise FileNotFoundError(f"Can't find file matching *-{instrument}*_{site}_{species_search}.nc in data/{network}/{sub_path}")
    elif len(nc_files) > 1:
        raise FileNotFoundError(f"Found more than one file matching *-{instrument}*_{site}_{species_search}.nc in data/{network}/{sub_path}")
    else:
        nc_file = nc_files[0]

    return nc_file, sub_path


def read_nc(network, species, site, instrument,
            verbose = False,
            data_exclude = True,
            baseline = None,
            resample = True,
            scale = "default",
            public = True,
            dropna = True,
            inlet_separate = True):
    """Read GCWerks netCDF files

    Args:
        network (str): Network, e.g., "agage"
        species (str): Species
        site (str): Site code
        instrument (str): Instrument
        verbose (bool, optional): Print verbose output. Defaults to False.
        data_exclude (bool, optional): Exclude data based on data_exclude.xlsx. Defaults to True.
        scale (str, optional): Scale to convert to. Defaults to "default". If None, will keep original scale.
        public (bool, optional): Whether the dataset is for public release. Default to True.
        resample (bool, optional): Whether to resample the data, if needed. Default to True.
        dropna (bool, optional): Drop NaN values. Default to True.
        
    Raises:
        FileNotFoundError: Can't find netCDF file

    Returns:
        xarray.Dataset: Contents of netCDF file
    """

    nc_file, sub_path = read_nc_path(network, species, site, instrument)

    if verbose:
        print(f"... reading {nc_file}")

    # Read netCDF file
    with open_data_file(nc_file, network, sub_path=sub_path, verbose=verbose) as f:
        try:
            with xr.open_dataset(f, engine="h5netcdf") as ds_file:
                ds = ds_file.load()
        except ValueError:
            with xr.open_dataset(f, engine="scipy") as ds_file:
                ds = ds_file.load()            

    # Read sampling time
    if "sampling_time_seconds" in ds.time.attrs:
        sampling_period = int(ds.time.attrs["sampling_time_seconds"])
        # Timestamp from GCWerks is the middle of the sampling period
        # Offset to be the start of the sampling period
        # Need to store and replace time attributes
        time_attrs = ds.time.attrs
        ds["time"] = ds.time - pd.Timedelta(sampling_period/2, unit="s")
        ds["time"].attrs = time_attrs
    else:
        # GCMD files don't have sampling time in the file
        # assume it's 1s (Peter Salameh, pers. comm., 2023-07-06)
        sampling_period = 1
    ds["time"].attrs["comment"] = "Timestamp is the start of the sampling period in UTC"

    # Add sampling time to variables
    ds["sampling_period"] = xr.DataArray(np.ones(len(ds.time)).astype(np.int16)*sampling_period,
                                        coords={"time": ds.time})

    # Baseline flags
    if baseline:
        ds_baseline = ds[baseline].copy(deep=True).to_dataset(name="baseline")

        # Convert to integer
        # When ASCII value is "B" (66), flag is 1, otherwise 0
        ds_baseline.baseline.values = ds_baseline.baseline == 66
        ds_baseline = ds_baseline.astype(np.int8)

        # Add baseline flag back in to main dataset so that it gets resampled, etc. consistently
        ds["baseline"] = xr.DataArray(ds_baseline.baseline.values, dims="time")

    # Everything should have been flagged in the AGAGE files already, but just in case...
    flagged = ds.data_flag != 0
    ds.mf[flagged] = np.nan
    ds.mf_repeatability[flagged] = np.nan

    # Add global attributes and format attributes
    ds.attrs["site_code"] = site.upper()

    # If no instrument attributes are present, add them using format_attributes
    if "instrument" not in ds.attrs:
        instruments = [{"instrument": instrument}]
    else:
        instruments = []

    ds = format_attributes(ds,
                        instruments=instruments,
                        network=network,
                        species=species,
                        public=public)

    # Set the instrument_type attribute
    # slightly convoluted method, but ensures consistency with combined files
    ds.attrs["instrument_type"] = get_instrument_type(get_instrument_number(instrument))

    # Remove any excluded data
    if data_exclude:
        ds = read_data_exclude(ds, format_species(species), site, instrument)

    # Check against release schedule and remove any data after end date
    rs = read_release_schedule(network, 
                            instrument,
                            species=format_species(species),
                            site=site,
                            public=public)
    ds = ds.sel(time=slice(None, rs))

    # Rename some variables, so that they can be resampled properly
    if "mf_mean_N" in ds:
        ds = ds.rename({"mf_mean_N": "mf_count"})
    if "mf_mean_stdev" in ds:
        ds = ds.rename({"mf_mean_stdev": "mf_variability"})

    # Check that time is monotonic and that there are no duplicate indices
    if not pd.Index(ds.time).is_monotonic_increasing:
        ds.sortby("time", inplace=True)
    if len(ds.time) != len(ds.time.drop_duplicates(dim="time")):
        ds = ds.drop_duplicates(dim="time")

    # Remove all time points where mf is NaN
    if dropna:
        ds = ds.dropna(dim="time", subset = ["mf"], how="all")

    # Separate into inlet dimensions
    if inlet_separate:
        ds = separate_inlets(ds)

    # Resample dataset, if needed and called
    if resample:
        ds = resample_function(ds)

    # If baseline is not None, return baseline dataset
    if baseline:
        ds_baseline = ds.baseline.copy(deep=True).to_dataset(name="baseline")
        ds_baseline.attrs = ds.attrs
        return ds_baseline

    # Apply standard variable formatting and return only variables in variable.json
    ds = format_variables(ds)

    # Convert scale, if needed
    ds = scale_convert(ds, scale)    

    return ds


def read_baseline(network, species, site, instrument,
                flag_name = "git_pollution_flag",
                verbose = False,
                public = True,
                dropna = True):
    """Read GCWerks netCDF files

    Args:
        network (str): Network, e.g., "agage"
        species (str): Species
        site (str): Site code
        instrument (str): Instrument
        flag_name (str, optional): Name of baseline flag variable. Defaults to "git_pollution_flag".
        verbose (bool, optional): Print verbose output. Defaults to False.

    Raises:
        FileNotFoundError: Can't find netCDF file

    Returns:
        xarray.Dataset: Contents of netCDF file
    """

    with open_data_file("attributes.json", network=network) as f:
        attributes_default = json.load(f)

    if not instrument.lower() in ["ale", "gage"]:

        ds_out = read_nc(network, species, site, instrument,
                        verbose=verbose,
                        baseline = flag_name,
                        public=public,
                        dropna=dropna)

    else:

        if flag_name != "git_pollution_flag":
            raise ValueError("Only git_pollution_flag is available for ALE/GAGE data")

        ds_out = read_ale_gage(network, species, site, instrument,
                           baseline = True,
                           verbose=verbose,
                           public=public,
                           dropna=dropna)

    # Add attributes
    ds_out.baseline.attrs = {
        "long_name": "baseline_flag",
        "flag_values": "0, 1",
        "flag_meanings": "not_baseline, baseline"
        }

    # Remove "sampling_time_seconds" from time attributes, if it exists
    if "sampling_time_seconds" in ds_out.time.attrs:
        del ds_out.time.attrs["sampling_time_seconds"]

    # Add global attributes
    ds_out.attrs = baseline_attrs[flag_name]

    # Add some global attributes
    ds_out.attrs["baseline_flag"] = flag_name
    ds_out.attrs["site_code"] = site.upper()
    ds_out.attrs["species"] = format_species(species)
    ds_out.attrs["instrument"] = instrument
    ds_out.attrs["network"] = network
    if public:
        ds_out.attrs["version"] = attributes_default["version"]
    else:
        ds_out.attrs["version"] = "NOT FOR PUBLIC RELEASE"

    return ds_out


def ale_gage_timestamp_issues(datetime, timestamp_issues,
                              verbose = True):
    """Check for timestamp issues in ALE/GAGE data

    Args:
        datetime (pd.Series): Datetime series
        timestamp_issues (dict): Dictionary of timestamp issues from ale_gage_timestamp_issues.json
        verbose (bool, optional): Print verbose output. Defaults to False.
        
    Returns:
        pd.Series: Datetime series with issues fixed
    """

    if len(timestamp_issues) == 0:
        return datetime
    
    for timestamp_issue in timestamp_issues:
        if timestamp_issue in datetime.values:
            if verbose:
                print(f"... Timestamp issue at {timestamp_issue} replacing with {timestamp_issues[timestamp_issue]}")
            datetime = datetime.replace(timestamp_issue, timestamp_issues[timestamp_issue])

    return datetime


def read_ale_gage(network, species, site, instrument,
                  verbose = True,
                  utc = True,
                  data_exclude = True,
                  scale = "default",
                  baseline = False,
                  public=True,
                  resample = False,
                  dropna = True,
                  inlet_separate = True):
    """Read GA Tech ALE/GAGE files, process and clean

    Args:
        network (str): Network. Can only be "agage" or "agage_test"
        species (str): Species
        site (str): Three-letter site code
        instrument (str): "ALE" or "GAGE"
        verbose (bool, optional): Print verbose output. Defaults to False.
        utc (bool, optional): Convert to UTC. Defaults to True.
        data_exclude (bool, optional): Exclude data based on data_exclude.xlsx. Defaults to True. 
            utc must also be true, as timestamps are in UTC.
        scale (str, optional): Calibration scale. Defaults to None, which means no conversion is attempted.
            Set to "default" to use value in scale_defaults.csv.
        baseline (bool, optional): Return baseline dataset. Defaults to False.
        public (bool, optional): Whether the dataset is for public release. Default to True.
        resample (bool, optional): Not used (see run_individual_instrument). Defaults to False.
        dropna (bool, optional): Drop NaN values. Defaults to True.
        inlet_separate (bool, optional): Separate inlets into new dimension. Defaults to False.

    Returns:
        pd.DataFrame: Pandas dataframe containing file contents
    """
    if network not in ["agage", "agage_test"]:
        raise ValueError("network must be agage or agage_test")
    
    if instrument not in ["ALE", "GAGE"]:
        raise ValueError("instrument must be ALE or GAGE")

    paths = Paths(network)

    # Get data on ALE/GAGE sites
    with open_data_file("ale_gage_sites.json", network = network, verbose=verbose) as f:
        site_info = json.load(f)

    # Get species info
    with open_data_file("ale_gage_species.json", network = network, verbose=verbose) as f:
        species_info = json.load(f)[format_species(species)]

    # Get Datetime issues list
    with open_data_file("ale_gage_timestamp_issues.json", network = network, verbose=verbose) as f:
        timestamp_issues = json.load(f)
        if site in timestamp_issues[instrument]:
            timestamp_issues = timestamp_issues[instrument][site]
        else:
            timestamp_issues = {}

    # Path to relevant sub-folder
    folder = paths.__getattribute__(f"{instrument.lower()}_path")

    with open_data_file(f"{site_info[site]['gcwerks_name']}_sio1993.gtar.gz",
                        network = network,
                        sub_path = folder,
                        verbose=verbose) as tar:

        dfs = []

        for member in tar.getmembers():

            # Extract tar file
            f = tar.extractfile(member)
            
            meta = f.readline().decode("ascii").strip()
            header = f.readline().decode("ascii").split()

            site_in_file = meta[:2]
            year = meta[2:4]
            month = meta[4:7]

            nspecies = len(header) - 3
            columns = header[:3]

            # Define column widths
            colspec = [3, 5, 7]
            coldtypes = [int, int, int]
            for sp in header[3:]:
                colspec += [7, 1]
                coldtypes += [np.float32, str]
                columns += [str(sp).replace("'", ""),
                            f"{sp}_pollution"]

            # Read data
            df = pd.read_fwf(f, skiprows=0,
                            widths=colspec,
                            names=columns,
                            na_values = -99.9)

            # Create datetime string. This format is a little weird, but it's an easy way to construct it
            datetime = df["DA"].astype(str).str.zfill(2) + \
                f"-{month}-{year} " + \
                df["TIME"].astype(str).str.zfill(4)
            
            # Check for datetime issues
            datetime = ale_gage_timestamp_issues(datetime, timestamp_issues,
                                                 verbose=verbose)

            # Convert datetime string
            df.index = pd.to_datetime(datetime, format="%d-%b-%y %H%M")

            # Drop duplicates
            if "duplicates" in timestamp_issues:
                keep = timestamp_issues["duplicates"]
            else:
                keep = "first"
            df = df[~df.index.duplicated(keep=keep)]

            # Timestamps are local time (no daylight savings)
            if utc:
                df.index = tz_local_to_utc(df.index, network, site)

            # append data frame if it's not all NaN
            if not df.empty:
                dfs.append(df)

    # Concatenate monthly dataframes into single dataframe
    df_combined = pd.concat(dfs)

    # Sort
    df_combined.sort_index(inplace=True)

    # Check if there are NaN indices
    if len(df_combined.index[df_combined.index.isna()]) > 0:
        raise ValueError("NaN indices found. Check timestamp issues.")

    # Drop na indices
    df_combined = df_combined.loc[~df_combined.index.isna(),:]

    # Check if there are duplicate indices
    if len(df_combined.index) != len(df_combined.index.drop_duplicates()):
        # Find which indices are duplicated
        duplicated_indices = df_combined.index[df_combined.index.duplicated(keep=False)]
        raise ValueError(f"Duplicate indices found. Check timestamp issues: {duplicated_indices}")

    # Store pollution flag
    da_baseline = (df_combined[f"{species_info['species_name_gatech']}_pollution"] != "P").astype(np.int8)

    # Output one species
    df_combined = df_combined[species_info["species_name_gatech"]]

    # Estimate of repeatability
    repeatability = species_info[f"{instrument.lower()}_repeatability_percent"]/100.

    nt = len(df_combined.index)

    # Create xarray dataset
    ds = xr.Dataset(data_vars={"mf": ("time", df_combined.values.copy()),
                            "mf_repeatability": ("time", df_combined.values.copy()*repeatability),
                            "inlet_height": ("time", np.repeat(site_info[site]["inlet_height"], nt)),
                            "sampling_period": ("time", np.repeat(30, nt)),
                            },
                    coords={"time": df_combined.index.copy()})

    # Global attributes
    comment = f"{instrument} {species} data from {site_info[site]['station_long_name']}. " + \
        "This data was originally processed by Georgia Institute of Technology, " + \
        "from the original files and has now been reprocessed into netCDF format."

    ds.attrs = {"comment": comment,
                "data_owner_email": site_info[site]["data_owner_email"],
                "data_owner": site_info[site]["data_owner"],
                "station_long_name": site_info[site]["station_long_name"],
                "inlet_base_elevation_masl": site_info[site]["inlet_base_elevation_masl"],
                "inlet_latitude": site_info[site]["latitude"],
                "inlet_longitude": site_info[site]["longitude"],
                "inlet_comment": "",
                "site_code": site}

    ds = format_attributes(ds,
                        instruments=[{"instrument": f"{instrument.upper()}_GCMD"}],
                        network=network,
                        species=format_species(species),
                        calibration_scale=species_info["scale"],
                        public=public)

    # Set the instrument_type attribute
    # slightly convoluted method, but ensures consistency with combined files
    ds.attrs["instrument_type"] = get_instrument_type(get_instrument_number(instrument))

    ds = format_variables(ds, units=species_info["units"])

    # Add pollution flag back in temporarily with dimension time
    ds["baseline"] = xr.DataArray(da_baseline.values, dims="time")

    # Remove any excluded data. Only do this if time is UTC, otherwise it won't be in the file
    if data_exclude:
        if not utc:
            raise ValueError("Can't exclude data if time is not UTC")
        ds = read_data_exclude(ds, format_species(species), site, instrument)

    # Check against release schedule if for public release. 
    rs = read_release_schedule(network, 
                            instrument,
                            species=format_species(species),
                            site=site,
                            public=public)
    ds = ds.sel(time=slice(None, rs))

    # Remove all time points where mf is NaN
    if dropna:
        ds = ds.dropna(dim="time", subset = ["mf"], how="all")
    
    if inlet_separate:
        ds = separate_inlets(ds)

    # Remove pollution flag
    ds_baseline = ds.baseline.copy(deep=True).to_dataset(name="baseline")
    ds = ds.drop_vars("baseline")

    # Raise error if baseline dataset is different length to main dataset
    if len(ds_baseline.time) != len(ds.time):
        raise ValueError("Baseline dataset is different length to main dataset. " + \
                         "Check timestamp issues.")

    # Convert scale, if needed
    ds = scale_convert(ds, scale)

    if baseline:
        return ds_baseline
    else:
        return ds


def read_gcwerks_flask(network, species, site, instrument,
                       verbose = True,
                       public = True,
                       dropna=True,
                       resample = False,
                       inlet_separate = False):
    '''Read GCWerks flask data

    Args:
        network (str): Network
        species (str): Species
        site (str): Site
        instrument (str): Instrument
        verbose (bool, optional): Print verbose output. Defaults to False.
        public (bool, optional): Whether the dataset is for public release. Default to True.
        dropna (bool, optional): Drop NaN values. Default to True.
        resample (bool, optional): Dummy kwarg, needed for consistency with other functions. Default to False.

    Returns:
        xr.Dataset: Dataset containing data
    '''

    # Need to get some information from the attributes_site.json file
    with open_data_file("attributes_site.json", network=network, errors = "ignore") as f:
        site_info_all = json.load(f)
    if site not in site_info_all:
        raise ValueError(f"Site {site} not found in attributes_site.json")
    site_info = site_info_all[site]

    if "sampling_period" not in site_info:
        raise ValueError(f"Sampling period not found in attributes_site.json for {site}")
    else:
        sampling_period = site_info["sampling_period"]
    
    if "inlet_height" not in site_info:
        raise ValueError(f"Inlet height not found in attributes_site.json for {site}")
    else:
        inlet_height = site_info["inlet_height"]

    if instrument != "GCMS-Medusa-flask":
        raise ValueError(f"Only valid for instrument GCMS-Medusa-flask, not {instrument}")
    
    species_search = format_species(species)
    species_flask = format_species_flask(species)

    sub_path = Paths(network, site=site.lower()).gcms_flask_path
    
    network_out, sub_path, nc_files = data_file_list(network, sub_path, f"{species_flask.lower()}_air.nc", site=site.lower())

    if len(nc_files) == 0:
        raise ValueError(f"No files found for {species_search} in {network} network")
    elif len(nc_files) > 1:
        raise ValueError(f"Multiple files found for {species_search} in {network} network")
    else:
        nc_file = nc_files[0]

    with open_data_file(nc_file, network, sub_path=sub_path, verbose=verbose, site=site.lower()) as f:
        with xr.open_dataset(f, engine="h5netcdf") as ds_file:
            ds_raw = ds_file.load()

    # Create new dataset with the time coordinate as sample time in seconds since 1970-01-01
    ds = xr.Dataset(data_vars={
            "mf": ("time", ds_raw[f"{species_flask}_C"].values),
            "mf_repeatability": ("time", ds_raw[f"{species_flask}_std_stdev"].values),
            "inlet_height": ("time", np.repeat(inlet_height, len(ds_raw[f"{species_flask}_C"]))),
            "sampling_period": ("time", np.repeat(sampling_period, len(ds_raw[f"{species_flask}_C"]))),
            "mf_count": ("time", np.repeat(1, len(ds_raw[f"{species_flask}_C"]))),
        },
        # Sampling time is the middle of the sampling period, so offset to the start
        coords={"time": xr.coding.times.decode_cf_datetime(ds_raw["sample_time"].values - sampling_period/2,
                                                           units="seconds since 1970-01-01")},
        attrs={"comment": f"GCMS Medusa flask data for {species_search} at {site_info['station_long_name']}.",
               "site_code": site}
    )

    # Sort by sampling time
    ds = ds.sortby("time")

    # Find duplicate timestamps and average those points
    if len(ds.time) != len(ds.time.drop_duplicates(dim="time")):
        # For mf_count, just sum, otherwise average, if mf isn't NaN
        mf_count = ds.mf.to_series().groupby("time").apply(lambda x: x.dropna().count())

        # If there are more than two points at the same time, calculate a standard deviation. Otherwise, set to 0
        mf_std = ds.mf.to_series().groupby("time").apply(lambda x: x.dropna().std() if len(x.dropna()) > 2 else 0.)

        # Average other variables
        ds = ds.groupby("time").mean(skipna=True)
        ds["mf_count"] = mf_count
        ds["mf_std"] = mf_std

    # Get cal scale from scale_defaults file
    scale = calibration_scale_default(network, species)

    ds = format_attributes(ds,
                        network = network,
                        species = species,
                        calibration_scale = scale,
                        public=public,
                        site = True)
    
    ds = format_variables(ds, species = species,
                        units="ppt",
                        calibration_scale = scale)
    
    # Set the instrument_type attribute
    # slightly convoluted method, but ensures consistency with combined files
    ds.attrs["instrument_type"] = get_instrument_type(get_instrument_number(instrument))

    # Remove all time points where mf is NaN
    if dropna:
        ds = ds.dropna(dim="time", subset = ["mf"])

    if inlet_separate:
        ds = separate_inlets(ds)

    return ds


def combine_datasets(network, species, site, 
                    scale = "default",
                    verbose = True,
                    public = True,
                    resample = True,
                    dropna = True):
    '''Combine ALE/GAGE/AGAGE datasets for a given species and site

    Args:
        network (str): Network
        species (str): Species
        site (str): Site
        scale (str, optional): Calibration scale. Defaults to value in scale_defaults.csv.
            If None, will attempt to leave scale unchanged.
        verbose (bool, optional): Print verbose output. Defaults to False.
        public (bool, optional): Whether the dataset is for public release. Default to True.
        resample (bool, optional): Whether to resample the data, if needed. Default to True.


    Returns:
        xr.Dataset: Dataset containing data
    '''

    # Read instrument dates from CSV files
    instruments = read_data_combination(network, format_species(species), site)

    instrument_types, instrument_number_str = instrument_type_definition()

    # Combine datasets
    dss = []
    comments = []
    attrs = []
    instrument_rec = []
    dates_rec = []
    networks = []
    scales = []

    for instrument, date in instruments.items():

        # Read data
        if instrument in ["ALE", "GAGE"]:
            ds = read_ale_gage(network, species, site, instrument,
                               verbose=verbose,
                               scale=scale,
                               public=public)
        else:
            ds = read_nc(network, species, site, instrument,
                        verbose=verbose,
                        scale=scale,
                        public=public,
                        resample = resample)

        # Run data_exclude again, to remove any data that should be excluded for the combined dataset
        ds = read_data_exclude(ds, format_species(species), site, instrument,
                               combined=True)

        # Store attributes
        attrs.append(ds.attrs)

        # Store instrument info
        instrument_rec.append({key:value for key, value in ds.attrs.items() if "instrument" in key})

        # Store comments
        comments.append(ds.attrs["comment"])

        # Store network
        networks.append(ds.attrs["network"])

        # Subset date
        ds = ds.sel(time=slice(*date))

        if len(ds.time) == 0:
            raise ValueError(f"No data retained for {species} {site} {instrument}. " + \
                             "Check dates in data_combination or omit this instrument.")
        dates_rec.append(ds.time[0].dt.strftime("%Y-%m-%d").values)

        # Record scale
        scales.append(ds.attrs["calibration_scale"])

        # Add instrument_type to dataset as variable
        instrument_type = get_instrument_number(instrument)
        ds["instrument_type"] = xr.DataArray(np.repeat(instrument_type, len(ds.time)),
                                        dims="time", coords={"time": ds.time})

        dss.append(ds)

    # Check that we don't have different scales
    if len(set(scales)) > 1:
        error_message = "Can't combine scales that do not match. Either specify a scale, or add to scale_defaults.csv. "
        for instrument, sc in zip(instruments.keys(), scales):
            error_message += f"{instrument}:{sc}, " 
        raise ValueError(error_message)

    # Combine datasets
    ds_combined = xr.concat(dss, dim="time",
                            data_vars="all",
                            coords="all",
                            combine_attrs="override")

    # Sort by time
    ds_combined = ds_combined.sortby("time")

    # Add details on instruments to global attributes
    ds_combined = format_attributes(ds_combined, instrument_rec, public=public)

    # Extend comment attribute describing all datasets
    if len(comments) > 1:
        comment_str = "Combined AGAGE/GAGE/ALE dataset from the following individual sources:\n"
        for i, comment in enumerate(comments):
            comment_str += f"{i}) {comment}\n"
    else:
        comment_str = comments[0]

    ds_combined.attrs["comment"] = comment_str

    # Format variables
    ds_combined = format_variables(ds_combined)

    # Drop duplicates, which may have been introduced by overlapping instruments
    ds_combined = drop_duplicates(ds_combined)

    # Remove all time points where mf is NaN
    if dropna:
        ds_combined = ds_combined.dropna(dim="time", subset = ["mf"], how="all")

    # Summarise instrument types in attributes
    # and remove instrument_type variable if all the same
    instrument_numbers = list(np.unique(ds_combined.instrument_type.values))
    if len(instrument_numbers) == 1:
        instrument_name = get_instrument_type(instrument_numbers[0])
        ds_combined.attrs["instrument_type"] = instrument_name
        ds_combined = ds_combined.drop_vars("instrument_type")
    else:
        instrument_name = get_instrument_type(instrument_numbers)
        ds_combined.attrs["instrument_type"] = "/".join(instrument_name)

    # Update network attribute
    ds_combined.attrs["network"] = "/".join(set(networks))

    return ds_combined


def combine_baseline(network, species, site,
                     verbose = True, public = True,
                     dropna = True):
    '''Combine ALE/GAGE/AGAGE baseline datasets for a given species and site

    Args:
        network (str): Network
        species (str): Species
        site (str): Site
        verbose (bool, optional): Print verbose output. Defaults to False.
        public (bool, optional): Whether the dataset is for public release. Default to True.
        dropna (bool, optional): Drop all time points where mf is NaN. Default to

    Returns:
        xr.Dataset: Dataset containing data
    '''

    # Read instrument dates from CSV files
    instruments = read_data_combination(network, format_species(species), site)

    # Combine datasets
    dss = []

    for instrument, date in instruments.items():

        # Read baseline. Only git_pollution_flag is available for ALE/GAGE data
        ds = read_baseline(network, species, site, instrument,
                           verbose=verbose,
                           flag_name="git_pollution_flag",
                           public=public, dropna=dropna)

        # Subset date
        ds = ds.sel(time=slice(*date))

        if len(ds.time) == 0:
            raise ValueError(f"No data retained for {species} {site} {instrument}. " + \
                             "Check dates in data_combination or omit this instrument.")

        dss.append(ds)

    ds_combined = xr.concat(dss, dim="time")

    # Sort by time
    ds_combined = ds_combined.sortby("time")

    # Remove duplicates
    # There's a danger that this is removing different points to the mole fraction dataset
    # but we don't have access to the instrument type. Impact should be minimal
    if len(ds_combined.time) != len(ds_combined.time.drop_duplicates(dim="time")):
        ds_combined = ds_combined.drop_duplicates(dim="time")

    return ds_combined


def output_write(ds, out_path, filename,
                output_subpath = "",
                verbose = False):
    '''Write dataset to netCDF file

    Args:
        ds (xr.Dataset): Dataset to output
        out_path (str): Path to output directory
        filename (str): Filename
        output_subpath (str, optional): Sub-path within output directory. Defaults to "".
            Used to put species in sub-directories.
        verbose (bool, optional): Print verbose output. Defaults to False.
    '''

    if verbose:
        print(f"... writing {str(out_path) + '/' + output_subpath + '/' + filename}")

    # Can't have some time attributes
    if "units" in ds.time.attrs:
        del ds.time.attrs["units"]
    if "calendar" in ds.time.attrs:
        del ds.time.attrs["calendar"]

    # Write file
    if out_path.suffix == ".zip":
        with ZipFile(out_path, mode="a") as zip:
            zip.writestr(output_subpath + "/" + filename, ds.to_netcdf())
    
    else:
        # Test if output_path exists and if not create it
        if not (out_path / output_subpath).exists():
            (out_path / output_subpath).mkdir(parents=True, exist_ok=True)

        with open(out_path / output_subpath / filename, mode="wb") as f:
            # ds_out.to_netcdf(f, mode="w", format="NETCDF4", engine="h5netcdf")
            ds.to_netcdf(f, mode="w")


def output_dataset(ds, network,
                   instrument = "GCMD",
                   end_date = None,
                   output_subpath = "",
                   extra = "",
                   version = True,
                   public = True,
                   verbose = False,
                   network_out = ""):
    '''Output dataset to netCDF file

    Args:
        ds (xr.Dataset): Dataset to output
        network (str): Network
        instrument (str, optional): Instrument. Defaults to "GCMD".
        end_date (str, optional): End date to subset to. Defaults to None.
        output_subpath (str, optional): Sub-path within output directory. Defaults to "".
            Used to put species in sub-directories.
        extra (str, optional): Extra string to add to filename. 
            Defaults to using the version number from global attributes.
        public (bool, optional): Whether the dataset is for public release. Default to True.
        verbose (bool, optional): Print verbose output. Defaults to False.
        network_out (str, optional): Network to use for filename. Defaults to "".
    '''
    if version:
        version_str = f"{ds.attrs['version']}"
    else:
        version_str = ""
        
    out_path, filename = output_path(network,
                                     format_species(ds.attrs["species"]),
                                     ds.attrs["site_code"],
                                     instrument,
                                     extra=extra, version=version_str, public=public,
                                     network_out=network_out)

    ds_out = ds.copy(deep = True)

    # Select time slice
    ds_out = ds_out.sel(time=slice(None, end_date))

    if len(ds_out.time) == 0:
        raise ValueError(f"No data retained for {ds_out.attrs['species']} when trying write {filename} after applying release schedule end dates. " + \
                        "Check dates in release schedule or omit this instrument.")

    output_write(ds_out, out_path, filename,
                output_subpath=output_subpath, verbose=verbose)

