import xarray as xr
import json
import pandas as pd
import numpy as np
from zipfile import ZipFile, ZIP_DEFLATED
from functools import lru_cache

from agage_archive.config import Paths, open_data_file, data_file_list, \
    output_path, load_json
from agage_archive.convert import scale_convert
from agage_archive.convert import resample as resample_function
from agage_archive.formatting import format_species, \
    format_variables, format_attributes, format_species_flask, \
    format_attributes_global_instruments
from agage_archive.data_selection import read_release_schedule, read_data_exclude, \
    read_data_combination, calibration_scale_default
from agage_archive.definitions import instrument_type_definition, get_instrument_type, \
    get_instrument_number, instrument_selection_text
from agage_archive.util import tz_local_to_utc, parse_fortran_format, nc_to_csv


gcwerks_species = {"c2f6": "pfc-116",
                   "c3f8": "pfc-218",
                   "c4f8": "pfc-318",
                   "ccl2ccl2": "pce",
                   "chclccl2": "tce",
                   "c6h6": "benzene",
                   "c6h5ch3": "toluene",
                   "c3h8": "propane",
                   "c2h6": "ethane",
                   "c2h4": "ethene",
                   "c2h2": "ethyne",
                   "c3h6": "c-propane",
                   }


magnum_species = {"hfc-134a": "HFC-134a",
                "hfc-152a": "HFC-152a",
                "hcfc-142b": "HCFC-142b",
                "cfc-11": "CFC-11",
                "cfc-12": "CFC-12"}
                  

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

    # Rank each instrument type by when its data first appears in the record: the
    # instrument that starts earliest gets rank 0, the next new one rank 1, and so on.
    # When two real values collide at the same timestamp, the value from the
    # earliest-starting instrument (the lowest rank) is kept and the later-starting
    # one is dropped.
    first_appearance_rank = {}
    for instrument in ds.instrument_type.values:
        if instrument not in first_appearance_rank:
            first_appearance_rank[instrument] = len(first_appearance_rank)

    is_nan = np.isnan(ds.mf.values)
    ranks = np.array([first_appearance_rank[instrument]
                      for instrument in ds.instrument_type.values])

    # For each timestamp keep exactly one row. Sorting ascending on these keys and taking
    # the first row per timestamp applies the tie-breaks in order:
    #   1. is_nan   — a real value (False) is kept over a NaN (True);
    #   2. rank     — among real values, keep the earliest-starting instrument, drop later;
    #   3. position — otherwise fall back to the earliest row in the record.
    # Rank must not decide between NaNs — an all-NaN timestamp keeps its first row
    # whatever the instruments — so the rank key is zeroed for NaN rows. That can't let a
    # NaN win a mixed timestamp, because is_nan already sorts every real value ahead of it.
    selection = pd.DataFrame({
        "time": ds.time.values,
        "is_nan": is_nan,
        "rank": np.where(is_nan, 0, ranks),
        "position": np.arange(len(ds.time)),
    })

    keep = selection.sort_values(["time", "is_nan", "rank", "position"]) \
                    .drop_duplicates("time", keep="first")["position"].to_numpy()
    keep.sort()

    return ds.isel(time=keep)


def define_instrument_type(ds, instrument):
    """Define instrument type for a dataset
    Args:
        ds (xarray.Dataset): Dataset
        instrument (str): Instrument name

    Returns:
        xarray.Dataset: Dataset with instrument_type defined
    """

    # Add instrument_type to dataset as variable
    instrument_type = get_instrument_number(instrument, ds.attrs["network"])
    ds["instrument_type"] = xr.DataArray(np.repeat(instrument_type, len(ds.time)),
                                    dims="time", coords={"time": ds.time})
    instrument_number, instrument_type_str = instrument_type_definition(ds.attrs["network"])

    ds["instrument_type"].attrs = {
        "long_name": "Instrument type",
        "comment": instrument_type_str,
        }

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

    # Determine sub-path within data directory
    sub_path = paths.__getattribute__(f"{instrument}_path")
    
    # search for netcdf files matching instrument, site and species
    nc_files = data_file_list(network, sub_path, f"*-{instrument}*_{site}_{species_search}.nc")[2]

    if len(nc_files) == 0:
        errorMessage = f"Can't find file matching *-{instrument}*_{site}_{species_search}.nc in data/{network}/{sub_path}. " + \
            "Have you seen the v0.2 change: paths should be specified as <instrument>_path in config.yaml?"
        raise FileNotFoundError(errorMessage)
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
            scale = "defaults",
            dropna = True):
    """Read GCWerks netCDF files

    Args:
        network (str): Network, e.g., "agage"
        species (str): Species
        site (str): Site code
        instrument (str): Instrument
        verbose (bool, optional): Print verbose output. Defaults to False.
        data_exclude (bool, optional): Exclude data based on data_exclude csv file. Defaults to True.
        scale (str, optional): Scale to convert to. Defaults to "defaults", which will read scale_defaults file. 
            If None, will keep original scale. If you want to use a different default scale, create a new scale defaults file, 
            with the name scale_defaults_<suffix>.csv and set to "defaults_<suffix>".
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
                        extra_attributes={
                            "product_type": "mole fraction",
                            "instrument_selection": "Individual instruments",
                            "frequency": "high-frequency"})

    # Set the instrument_type attribute
    # slightly convoluted method, but ensures consistency with combined files
    ds.attrs["instrument_type"] = get_instrument_type(get_instrument_number(instrument, network), network)

    # Remove any excluded data
    if data_exclude:
        ds = read_data_exclude(ds, format_species(species), site, instrument)

    # Check against release schedule and remove any data after end date
    rs = read_release_schedule(network,
                            instrument,
                            species=format_species(species),
                            site=site)
    if pd.Index(ds.time).is_monotonic_increasing:
        ds = ds.sel(time=slice(None, rs))
    else:
        ds = ds.where(ds.time <= np.datetime64(rs), drop=True)

    # Rename some variables, so that they can be resampled properly
    if "mf_mean_N" in ds:
        ds = ds.rename({"mf_mean_N": "mf_count"})
    if "mf_mean_stdev" in ds:
        ds = ds.rename({"mf_mean_stdev": "mf_variability"})

    # Add instrument_type to dataset as variable
    ds = define_instrument_type(ds, instrument)

    # Resample dataset, if needed and called
    if resample:
        ds = resample_function(ds)

    # Check that time is monotonic and remove duplicate indices
    if not pd.Index(ds.time).is_monotonic_increasing:
        ds = ds.sortby("time")
    if len(ds.time) != len(ds.time.drop_duplicates(dim="time")):
        ds = ds.drop_duplicates(dim="time")

    # Remove all time points where mf is NaN
    if dropna:
        ds = ds.dropna(dim="time", subset = ["mf"])

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

    attributes_default = load_json("attributes.json", network=network)

    read_function = get_data_read_function(network, instrument)

    if read_function.__name__ == "read_gcms_magnum" or read_function.__name__ == "read_ale_gage":
        if flag_name != "git_pollution_flag":
            raise ValueError("Only git_pollution_flag is available for ALE/GAGE data")
        else:
            flag = True
    else:
        flag = flag_name

    ds_out = read_function(network, species, site, instrument,
                    verbose=verbose,
                    baseline = flag,
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

    attrs = ds_out.attrs.copy()

    # Add global attributes
    ds_out.attrs = baseline_attrs[flag_name]

    # Copy across a few attributes
    for att in ["inlet_latitude", "inlet_longitude", "inlet_base_elevation_masl",
                "doi", "file_created_by", "station_long_name",
                "processing_code_url", "processing_code_version"]:
        ds_out.attrs[att] = attrs[att]

    # Add some global attributes
    ds_out.attrs["baseline_flag"] = flag_name
    ds_out.attrs["site_code"] = site.upper()
    ds_out.attrs["species"] = format_species(species)
    ds_out.attrs["instrument"] = instrument
    ds_out.attrs["instrument_type"] = get_instrument_type(get_instrument_number(instrument, network), network)
    ds_out.attrs["network"] = network
    ds_out.attrs["product_type"] = "baseline flag"
    ds_out.attrs["instrument_selection"] = "Individual instruments"
    ds_out.attrs["frequency"] = "high-frequency"
    ds_out.attrs["version"] = attributes_default["version"]
    if len(ds_out.time) > 0:
        ds_out.attrs["start_date"] = str(ds_out.time[0].dt.strftime("%Y-%m-%d %H:%M:%S").values)
        ds_out.attrs["end_date"] = str(ds_out.time[-1].dt.strftime("%Y-%m-%d %H:%M:%S").values)
    else:
        ds_out.attrs["start_date"] = ""
        ds_out.attrs["end_date"] = ""

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


def read_ale_gage_file(f, network, site,
                    timestamp_issues = {},
                    utc = True,
                    verbose = True):
    """Read individual ALE/GAGE file

    Args:
        f (file): File object
        network (str): Network
        site (str): Site
        timestamp_issues (dict): Dictionary of timestamp issues
        utc (bool): Convert to UTC
        verbose (bool): Print verbose output

    Returns:
        pd.DataFrame: Dataframe containing file contents
    """

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

    return df


@lru_cache(maxsize=None)
def _read_ale_gage_raw(network, site, instrument, utc):
    """Read and concatenate all monthly ALE/GAGE files for a site into one dataframe.

    This is the expensive, species-independent core of read_ale_gage: opening the tar
    archive and parsing every monthly fixed-width file (hundreds of read_fwf calls). The
    result keeps every species' columns; read_ale_gage picks out the one it needs. Since
    it is identical across species and across the individual and combined workflows — each
    of which would otherwise re-read the whole archive — it is memoised on
    (network, site, instrument, utc). Callers must copy it before mutating.

    Args:
        network (str): Network.
        site (str): Site code.
        instrument (str): "ALE" or "GAGE".
        utc (bool): Convert timestamps to UTC.

    Returns:
        pd.DataFrame: All monthly records concatenated, sorted, de-NaN-indexed and
            duplicate-checked, with every species column retained.
    """

    paths = Paths(network)
    site_info = load_json("ale_gage_sites.json", network=network)

    # Get Datetime issues list
    with open_data_file("ale_gage_timestamp_issues.json", network=network) as f:
        timestamp_issues = json.load(f)
        if site in timestamp_issues[instrument]:
            timestamp_issues = timestamp_issues[instrument][site]
        else:
            timestamp_issues = {}

    # Path to relevant sub-folder
    folder = paths.__getattribute__(f"{instrument}_path")

    with open_data_file(f"{site_info[site]['gcwerks_name']}_sio1993.gtar.gz",
                        network=network,
                        sub_path=folder) as tar:

        dfs = []

        for member in tar.getmembers():

            # Extract tar file
            f = tar.extractfile(member)

            df = read_ale_gage_file(f, network, site,
                                timestamp_issues=timestamp_issues,
                                utc=utc,
                                verbose=False)

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
    df_combined = df_combined.loc[~df_combined.index.isna(), :]

    # Check if there are duplicate indices
    if len(df_combined.index) != len(df_combined.index.drop_duplicates()):
        # Find which indices are duplicated
        duplicated_indices = df_combined.index[df_combined.index.duplicated(keep=False)]
        raise ValueError(f"Duplicate indices found. Check timestamp issues: {duplicated_indices}")

    return df_combined


def read_ale_gage(network, species, site, instrument,
                  verbose = True,
                  utc = True,
                  data_exclude = True,
                  scale = "defaults",
                  baseline = False,
                  resample = False,
                  dropna = True):
    """Read GA Tech ALE/GAGE files, process and clean

    Args:
        network (str): Network. Can only be "agage" or "agage_test"
        species (str): Species
        site (str): Three-letter site code
        instrument (str): "ALE" or "GAGE"
        verbose (bool, optional): Print verbose output. Defaults to False.
        utc (bool, optional): Convert to UTC. Defaults to True.
        data_exclude (bool, optional): Exclude data based on data_exclude csv file. Defaults to True. 
            utc must also be true, as timestamps are in UTC.
        scale (str, optional): Calibration scale. Defaults to None, which means no conversion is attempted.
            Set to "default" to use value in scale_defaults.csv.
        baseline (bool, optional): Return baseline dataset. Defaults to False.
        resample (bool, optional): Not used (see run_individual_instrument). Defaults to False.
        dropna (bool, optional): Drop NaN values. Defaults to True.

    Returns:
        pd.DataFrame: Pandas dataframe containing file contents
    """
    # if "agage" not in network:
    #     raise ValueError("network must be agage or agage_test")
    
    if instrument not in ["ALE", "GAGE"]:
        raise ValueError("instrument must be ALE or GAGE")

    # Get data on ALE/GAGE sites
    site_info = load_json("ale_gage_sites.json", network=network)

    # Get species info
    with open_data_file("ale_gage_species.json", network = network, verbose=verbose) as f:
        species_info = json.load(f)[format_species(species)]

    # Read and concatenate every monthly file (all species). This is the expensive part
    # and is species-independent, so it is cached and shared across species and across the
    # individual/combined workflows. Copy so downstream edits never touch the cached frame.
    df_combined = _read_ale_gage_raw(network, site, instrument, utc).copy()

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
                            "sampling_period": ("time", np.repeat(1, nt)),
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
                "site_code": site.upper(),
                "product_type": "mole fraction",
                "instrument_selection": "Individual instruments",
                "frequency": "high-frequency",}

    if instrument == "ALE":
        ds.attrs["instrument_comment"] = "NOTE: Some data points may have been removed from the original dataset " + \
            "because they were not felt to be representative of the baseline air masses (Paul Fraser, pers. comm.). "

    ds = format_attributes(ds,
                        instruments=[{"instrument": f"{instrument.upper()}_GCMD"}],
                        network=network,
                        species=format_species(species),
                        calibration_scale=species_info["scale"],
                        )

    # Add instrument_type to dataset as variable
    ds = define_instrument_type(ds, instrument)

    # Set the instrument_type attribute
    # slightly convoluted method, but ensures consistency with combined files
    ds.attrs["instrument_type"] = get_instrument_type(get_instrument_number(instrument, network), network)

    ds = format_variables(ds, units=species_info["units"])

    # Add pollution flag back in temporarily with dimension time
    ds["baseline"] = xr.DataArray(da_baseline.values, dims="time")

    # Remove any excluded data. Only do this if time is UTC, otherwise it won't be in the file
    if data_exclude:
        if not utc:
            raise ValueError("Can't exclude data if time is not UTC")
        ds = read_data_exclude(ds, format_species(species), site, instrument)

    # Check against release schedule
    rs = read_release_schedule(network,
                            instrument,
                            species=format_species(species),
                            site=site)
    ds = ds.sel(time=slice(None, rs))

    # Remove all time points where mf is NaN
    if dropna:
        ds = ds.dropna(dim="time", subset = ["mf"])
    
    # Remove pollution flag
    ds_baseline = ds.baseline.copy(deep=True).to_dataset(name="baseline")
    ds_baseline.attrs = ds.attrs.copy()
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


def read_gcms_magnum_file(file, species,
                        species_name_in_file = None):
    """Read GCMS Magnum file

    Args:
        file (file): File object
        species (str): Species

    Returns:
        Tuple[pd.DataFrame, str]: Dataframe containing file contents, calibration scale
    """

    fortran_code_identifier = "You can use the following format in Fortran to read data in different columns,"

    if not species_name_in_file:
        species_file = species
    else:
        species_file = species_name_in_file

    # Get relevant header lines and format code
    header_li = 0
    while True:
        header_li += 1
        line = file.readline()
        if fortran_code_identifier in str(line):
            widths_string = str(line).split(fortran_code_identifier)[1].strip()
            break

    # Remove any trailing characters from widths_string
    widths_string = widths_string.split("\\")[0]

    # Return to beginning of file
    file.seek(0)

    header_line = header_li + 2
    scales_line = header_li + 1

    # Interpret fortan format code
    column_specs, column_types = parse_fortran_format(widths_string)

    # Read column names and scales
    header = pd.read_fwf(file,
                        colspecs=column_specs,
                        header=None,
                        skiprows = header_line,
                        nrows=1)
    columns = header.values[0]

    # Return to beginning of file
    file.seek(0)

    header = pd.read_fwf(file,
                        colspecs=column_specs,
                        header=None,
                        skiprows = scales_line-1,
                        nrows=1)
    scales = header.values[0]

    # Return to beginning of file
    file.seek(0)

    # After two null values in columns, relabel as "missingX" where X is a number
    for ci in range(1, len(columns)):
        if not isinstance(columns[ci], str):
            if not isinstance(columns[ci-1], str):
                columns[ci] = "missing" + str(ci)

    for ci in range(1, len(columns)):
        if not isinstance(columns[ci], str):
            if isinstance(columns[ci-1], str):
                # rename the column to the previous column name + "_pollution"
                columns[ci]=columns[ci-1] + "_pollution"

    # Read data
    df = pd.read_fwf(file,
                    colspecs=column_specs,
                    header=None,
                    skiprows=header_line+1,
                    names=columns,
                    column_types=column_types)

    # For data columns (after ABSDA) replace zeros with NaN
    wh = df.columns.get_loc("ABSDA")

    for ci in range(wh+1, len(df.columns)):
        df.iloc[:, ci] = df.iloc[:, ci].replace(0, np.nan)

    # Create datetime column and set to index
    df['datetime'] = pd.to_datetime({
            'year': df['YYYY'],
            'month': df['MM'],
            'day': df['DD'],
            'hour': df['hh'],
            'minute': df['min']
        }, errors='coerce')

    # Drop all rows where datetime is NaT
    df = df.dropna(subset=['datetime'])

    # Set datetime as index
    df.set_index('datetime', inplace=True)

    # Drop original date columns
    drop_cols = ["time", 'YYYY', 'MM', 'DD', 'hh', 'min', "ABSDA"]
    for col in df.columns:
        if "missing" in col:
            drop_cols.append(col)
    df = df.drop(columns=drop_cols)

    # store only the species column and baseline
    df["baseline"] = (df[f"{species_file}_pollution"] != "P").astype(np.int8)
    df["mf"] = df[species_file]
    df = df[["mf", "baseline"]]

    scale = scales[np.where(columns == species_file)[0][0]]

    return df, scale


def read_gcms_magnum(network, species,
                  site = "MHD",
                  instrument = "GCMS-Magnum",
                  verbose = True,
                  data_exclude = True,
                  scale = "defaults",
                  baseline = False,
                  resample = False,
                  dropna = True):
    """Read GCMS Magnum data

    Args:
        network (str): Network
        species (str): Species
        site (str, optional): Site. Defaults to "MHD".
        instrument (str, optional): Instrument. Defaults to "GCMS-Magnum".
        verbose (bool, optional): Print verbose output. Defaults to False.
        data_exclude (bool, optional): Exclude data based on data_exclude csv file. Defaults to True.
        scale (str, optional): Calibration scale. Defaults to "defaults".
        baseline (bool, optional): Return baseline dataset. Defaults to False.
        resample (bool, optional): Not used (see run_individual_instrument). Defaults to False.
        dropna (bool, optional): Drop NaN values. Defaults to True.

    Returns:
        xarray.Dataset: Dataset
    """

    # Get data on ALE/GAGE sites
    site_info = load_json("ale_gage_sites.json", network=network)

    # Get species info
    with open_data_file("gcms-magnum_species.json", network = network, verbose=verbose) as f:
        species_info = json.load(f)[format_species(species)]

    paths = Paths(network)
    if not hasattr(paths, "GCMS-Magnum_path"):
        raise ValueError("No GCMS-Magnum_path attribute in config.yaml.")

    with open_data_file(paths.__getattribute__("GCMS-Magnum_path"),
                        network = network,
                        verbose=verbose) as tar:

        dfs = []

        for member in tar.getmembers():

            # Extract tar file
            f = tar.extractfile(member)
            
            df, scale = read_gcms_magnum_file(f, species,
                                            species_name_in_file=species_info["species_name_gatech"])
    
            dfs.append(df)

    # Concatenate monthly dataframes into single dataframe
    df_combined = pd.concat(dfs)

    # Sort
    df_combined.sort_index(inplace=True)

    # Check if there are NaN indices
    if len(df_combined.index[df_combined.index.isna()]) > 0:
        raise ValueError("NaN indices found. Check timestamp issues.")

    # Check if there are duplicate indices
    if len(df_combined.index) != len(df_combined.index.drop_duplicates()):
        # Find which indices are duplicated
        duplicated_indices = df_combined.index[df_combined.index.duplicated(keep=False)]
        raise ValueError(f"Duplicate indices found. Check timestamp issues: {duplicated_indices}")
    
    # Store pollution flag
    da_baseline = df_combined["baseline"]

    repeatability = species_info["repeatability_percent"]/100.

    # Convert to dataset
    ds = xr.Dataset(data_vars={"mf": ("time", df_combined["mf"].values.copy()),
                            "mf_repeatability": ("time", df_combined["mf"].values.copy()*repeatability),
                            "inlet_height": ("time", np.repeat(site_info[site]["inlet_height"], len(df_combined["mf"]))),
                            "sampling_period": ("time", np.repeat(2400, len(df_combined["mf"]))),
                            },
                    coords={"time": df_combined.index.values.copy()})

    # Global attributes
    ds.attrs["comment"] = f"{instrument} {species} data from {site}. " + \
        "This data was originally processed by Georgia Institute of Technology, " + \
        "from the original files and has now been reprocessed into netCDF format."
    ds.attrs["calibration_scale"] = scale

    extra_attrs = site_info[site].copy()
    extra_attrs["inlet_latitude"] = extra_attrs["latitude"]
    extra_attrs["inlet_longitude"] = extra_attrs["longitude"]
    del extra_attrs["latitude"]
    del extra_attrs["longitude"]
    del extra_attrs["tz"]
    extra_attrs["product_type"] = "mole fraction"
    extra_attrs["instrument_selection"] = "Individual instruments"
    extra_attrs["frequency"] = "high-frequency"
    extra_attrs["instrument_type"] = get_instrument_type(get_instrument_number(instrument, network), network)
    extra_attrs["site_code"] = site.upper()

    # Add attributes
    ds = format_attributes(ds,
                        instruments=[{"instrument": instrument,
                                    "instrument_comment": "GCMS ADS with Finnigan Magnum Iron Trap",
                                    "instrument_date": ds.time[0].dt.strftime("%Y-%m-%d").values}],
                        network=network,
                        species=format_species(species),
                        site=False,
                        extra_attributes = extra_attrs)

    # Add instrument_type to dataset as variable
    ds = define_instrument_type(ds, instrument)

    ds = format_variables(ds, units = species_info["units"])

    # Add pollution flag back in temporarily with dimension time
    ds["baseline"] = xr.DataArray(da_baseline.values, dims="time")

    # Remove any excluded data
    if data_exclude:
        ds = read_data_exclude(ds, format_species(species), site, instrument)

    # Check against release schedule
    rs = read_release_schedule(network,
                            instrument,
                            species=format_species(species),
                            site=site)
    ds = ds.sel(time=slice(None, rs))

    # Remove all time points where mf is NaN
    if dropna:
        ds = ds.dropna(dim="time", subset = ["mf"])
    
    # Remove pollution flag
    ds_baseline = ds.baseline.copy(deep=True).to_dataset(name="baseline")
    ds_baseline.attrs = ds.attrs.copy()
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


def _merge_duplicate_flask_measurements(ds, flask_pair_agreement = False):
    """Merge duplicate flask sampling times, optionally checking pair agreement.

    Duplicate groups are always averaged to a single sample. If
    flask_pair_agreement is enabled, groups are first retained only when the
    sample pair agreement is within twice the mean reported standard deviation
    for that sampling time.

    Args:
        ds (xr.Dataset): Flask dataset with time coordinate and mf values.
        flask_pair_agreement (bool, optional): Whether to reject duplicate
            sampling times with poor pair agreement before averaging.
            Defaults to False.

    Returns:
        xr.Dataset: Dataset with duplicate sampling times merged.
    """

    if len(ds.time) == len(ds.time.drop_duplicates(dim="time")):
        return ds

    mf_by_time = ds.mf.to_series().groupby("time")
    mf_range = mf_by_time.apply(
        lambda x: x.dropna().max() - x.dropna().min() if len(x.dropna()) > 0 else np.nan
    )

    if flask_pair_agreement and "mf_repeatability" in ds:
        mean_std = ds.mf_repeatability.to_series().groupby("time").apply(
            lambda x: x.dropna().mean()
        )
        keep_times = mf_range.index[
            (mf_range <= 2 * mean_std) | mf_range.isna() | mean_std.isna()
        ]
        keep_mask = ds.time.isin(keep_times.to_numpy())
        ds = ds.isel(time=keep_mask)

    mf_count = ds.mf.to_series().groupby("time").apply(lambda x: x.dropna().count())
    mf_std = ds.mf.to_series().groupby("time").apply(
        lambda x: x.dropna().std() if len(x.dropna()) > 1 else 0.
    )

    ds = ds.groupby("time").mean(skipna=True, keep_attrs=True)
    ds["mf_count"] = mf_count.astype(np.int32)
    ds["mf_std"] = mf_std

    return ds


def read_gcwerks_flask(network, species, site, instrument,
                       verbose = True,
                       data_exclude = True,
                       dropna=True,
                       resample = False,
                       scale = "defaults",
                       flask_pair_agreement = False):
    '''Read GCWerks flask data

    Args:
        network (str): Network
        species (str): Species
        site (str): Site
        instrument (str): Instrument
        verbose (bool, optional): Print verbose output. Defaults to False.
        data_exclude (bool, optional): Exclude data based on data_exclude csv file. Defaults to True.
        dropna (bool, optional): Drop NaN values. Default to True.
        resample (bool, optional): Dummy kwarg, needed for consistency with other functions. Default to False.
        scale (str, optional): Scale to convert to - currently only accepts "defaults", which will read scale_defaults file.
        flask_pair_agreement (bool, optional): Apply the flask pair-agreement
            check to duplicate sampling times before averaging. Defaults to
            False.

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
    
    species_search = format_species(species)
    species_flask = format_species_flask(species)

    sub_path = Paths(network, site=site.lower()).__getattribute__(f"{instrument}_path")
    
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
               "site_code": site.upper()}
    )

    # Sort by sampling time
    ds = ds.sortby("time")

    # Merge duplicate sampling times, optionally rejecting poor flask pair
    # agreement before averaging
    ds = _merge_duplicate_flask_measurements(ds,
                                            flask_pair_agreement=flask_pair_agreement)

    # Get cal scale from scale_defaults file
    # TODO: THIS IS DANGEROUS, but there's currently no way to specify a scale for flask data
    if scale == "defaults":
        scale = calibration_scale_default(network, species)
    else:
        raise ValueError("Flask data must use scale_defaults file")

    ds = format_attributes(ds,
                        network = network,
                        species = species,
                        calibration_scale = scale,
                        site = True)

    # Add instrument_type to dataset as variable
    ds = define_instrument_type(ds, instrument)
    
    ds = format_variables(ds, species = species,
                        units="ppt",
                        calibration_scale = scale)
    
    # Set the instrument_type attribute
    # slightly convoluted method, but ensures consistency with combined files
    ds.attrs["instrument_type"] = get_instrument_type(get_instrument_number(instrument, network), network)

    # Remove any excluded data
    if data_exclude:
        ds = read_data_exclude(ds, format_species(species), site, instrument)

    # Check against release schedule
    rs = read_release_schedule(network,
                            instrument,
                            species=format_species(species),
                            site=site)
    ds = ds.sel(time=slice(None, rs))

    # Remove all time points where mf is NaN
    if dropna:
        ds = ds.dropna(dim="time", subset = ["mf"])

    return ds


def combine_datasets(network, species, site, 
                    scale = "defaults",
                    verbose = True,
                    resample = True,
                    dropna = True,
                    flask_pair_agreement = False):
    '''Combine ALE/GAGE/AGAGE datasets for a given species and site

    Args:
        network (str): Network
        species (str): Species
        site (str): Site
        scale (str, optional): Calibration scale. Defaults to value in scale_defaults.csv.
            If None, will attempt to leave scale unchanged.
        verbose (bool, optional): Print verbose output. Defaults to False.
        resample (bool, optional): Whether to resample the data, if needed. Default to True.
        dropna (bool, optional): Drop NaN values. Defaults to True.
        flask_pair_agreement (bool, optional): Apply the flask pair-agreement
            screen when reading flask datasets. Defaults to False.

    Returns:
        xr.Dataset: Dataset containing data
    '''

    def dataset_reader(instrument):
        read_function = get_data_read_function(network, instrument)
        kwargs = dict(verbose=verbose,
                      scale=scale,
                      resample=resample,
                      dropna=dropna)
        if read_function.__name__ == "read_gcwerks_flask":
            kwargs["flask_pair_agreement"] = flask_pair_agreement
        return read_function(network, species, site, instrument, **kwargs)

    datasets = _read_combined_instrument_datasets(network,
                                                  species,
                                                  site,
                                                  dataset_reader)

    dss = []
    comments = []
    comment_dates = []
    instrument_rec = []
    networks = []
    scales = []
    data_owners = []
    data_owner_emails = []

    for instrument, ds in datasets.items():
        comments.append(ds.attrs["comment"])
        comment_dates.append(ds.attrs.get("instrument_date", ""))
        networks.append(ds.attrs["network"])
        data_owners.append(ds.attrs.get("data_owner", ""))
        data_owner_emails.append(ds.attrs.get("data_owner_email", ""))
        instrument_rec.append({key:value for key, value in ds.attrs.items() if "instrument" in key})

        # If variable mf_count is not present, add it (1 measurement per time point)
        if "mf_count" not in ds:
            ds["mf_count"] = xr.DataArray(np.ones(len(ds.time)).astype(int),
                                        dims="time", coords={"time": ds.time})
            ds["mf_count"].attrs = {"long_name": "Number of data points in mean",
                                    "units": ""}

        scales.append(ds.attrs["calibration_scale"])

        if "instrument_type" not in ds.variables:
            raise ValueError(f"Instrument type {ds.instrument_type} not found in instrument_type_definition.json")

        dss.append(ds)

    # Check that we don't have different scales
    if len(set(scales)) > 1:
        error_message = "Can't combine scales that do not match. Either specify a scale, or add to scale_defaults.csv. "
        for instrument, sc in zip(datasets.keys(), scales):
            error_message += f"{instrument}:{sc}, " 
        raise ValueError(error_message)

    # Combine datasets
    ds_combined = xr.concat(dss, dim="time",
                            data_vars="all",
                            coords="all",
                            combine_attrs="override")

    # combine_attrs="override" takes the global attributes from the first dataset, which
    # is the first instrument listed in the data_combination file and so usually the
    # oldest. Take them from the most recently operating instrument instead: station
    # metadata such as inlet height and inlet comments is more likely to describe the
    # current state of the site. See issue #167.
    ds_combined.attrs = most_recent_dataset(dss).attrs.copy()

    # Sort by time
    ds_combined = ds_combined.sortby("time")

    # Add details on instruments to global attributes
    ds_combined = format_attributes(ds_combined, instrument_rec,
                                    extra_attributes={"instrument_selection": instrument_selection_text})

    # Extend comment attribute describing all datasets, ordered most-recent first to
    # match the numbered instrument_* attributes.
    ds_combined.attrs["comment"] = combine_comments(comments, comment_dates)

    # Format variables
    ds_combined = format_variables(ds_combined)

    # Drop duplicates, which may have been introduced by overlapping instruments
    ds_combined = drop_duplicates(ds_combined)

    # Remove all time points where mf is NaN
    if dropna:
        ds_combined = ds_combined.dropna(dim="time", subset = ["mf"])

    # Summarise instrument types in attributes
    instrument_numbers = list(np.unique(ds_combined.instrument_type.values))
    instrument_name = get_instrument_type(instrument_numbers, network)
    ds_combined.attrs["instrument_type"] = "/".join(instrument_name)

    # Update network attribute
    ds_combined.attrs["network"] = "/".join(set(networks))

    # Data owners must credit every contributing instrument, not just the one whose
    # attributes were inherited above (issue #169).
    ds_combined.attrs["data_owner"], ds_combined.attrs["data_owner_email"] = \
        combine_data_owners(data_owners, data_owner_emails)

    return ds_combined


def most_recent_dataset(datasets):
    """Return the dataset whose record ends latest.

    Used to decide which instrument's global attributes a combined file should inherit.
    The instrument that operated most recently is the best description of the current
    state of the site, so its station metadata is preferred over that of an instrument
    that was decommissioned decades ago (issue #167).

    Args:
        datasets (list[xr.Dataset]): Datasets, each with at least one time point.

    Returns:
        xr.Dataset: The dataset with the latest final timestamp.

    Raises:
        ValueError: If no datasets are given.
    """

    if not datasets:
        raise ValueError("Need at least one dataset to find the most recent")

    return max(datasets, key=lambda ds: ds.time.values[-1])


def combine_comments(comments, dates=None):
    """Build a combined-file comment listing each contributing instrument's comment.

    Identical instrument comments are de-duplicated so that instruments sharing a
    comment (e.g. the same source note) are not listed twice (issue #175).

    If instrument dates are supplied, the comments are ordered most-recent first, so
    they run in the same direction as the numbered instrument_* attributes (which
    format_attributes_global_instruments sorts by date descending). Otherwise order is
    preserved from first appearance. The same ``np.argsort`` used for the instrument
    numbering is reused here so the two orderings agree.

    Args:
        comments (list[str]): Each contributing instrument's comment attribute.
        dates (list[str], optional): Each instrument's date, aligned with ``comments``.
            When given, comments are ordered by date descending before de-duplication.

    Returns:
        str: A single comment string. If more than one distinct comment is present,
            they are enumerated under a header; otherwise the sole comment is returned
            unchanged.
    """

    if dates is not None:
        order = np.argsort(dates)[::-1]
        comments = [comments[i] for i in order]

    unique_comments = []
    for comment in comments:
        if comment not in unique_comments:
            unique_comments.append(comment)

    if len(unique_comments) > 1:
        comment_str = "Combined dataset from the following individual sources:\n"
        for i, comment in enumerate(unique_comments):
            comment_str += f"{i}) {comment}\n"
    else:
        comment_str = unique_comments[0]

    return comment_str


def combine_data_owners(owners, emails):
    """De-duplicate data owners across the instruments in a combined file.

    A combined file draws data from several instruments, which may have different
    data owners. Taking the owner from a single instrument (issue #169) silently
    credits one owner and drops the others, so the combined value must be the union
    of every contributing owner.

    Owners are paired with their email addresses positionally, matching the existing
    convention where a single instrument already lists multiple people as
    "Ray F. Weiss, Jens Muhle" / "rfweiss@ucsd.edu, jmuhle@ucsd.edu". Pairs are
    de-duplicated while preserving first-seen order (i.e. the data_combination
    order of the instruments), so owner and email stay aligned.

    Args:
        owners (list[str]): Each instrument's data_owner attribute, in data_combination
            order. Individual entries may themselves be ", "-separated lists of people.
        emails (list[str]): Each instrument's data_owner_email attribute, aligned with
            owners.

    Returns:
        tuple[str, str]: The combined (data_owner, data_owner_email), each a
            ", "-separated string, kept as a plain string to match the input encoding
            and the existing convention.
    """

    seen = []

    for owner, email in zip(owners, emails):
        names = [n.strip() for n in owner.split(",")] if owner else []
        addrs = [a.strip() for a in email.split(",")] if email else []

        for i, name in enumerate(names):
            if not name:
                continue
            addr = addrs[i] if i < len(addrs) else ""
            pair = (name, addr)
            if pair not in seen:
                seen.append(pair)

    return ", ".join(name for name, _ in seen), ", ".join(addr for _, addr in seen)


def _read_combined_instrument_datasets(network, species, site, dataset_reader):
    """Read and filter per-instrument datasets used by combined products.

    This applies the shared combined-data selection rules: the combined-only
    exclusions and the instrument date windows from data_combination.

    Args:
        network (str): Network.
        species (str): Species.
        site (str): Site code.
        dataset_reader (Callable[[str], xr.Dataset]): Function that reads one
            instrument dataset.

    Returns:
        dict[str, xr.Dataset]: Filtered datasets keyed by instrument name.
    """

    instruments = read_data_combination(network, format_species(species), site)
    datasets = {}

    for instrument, date in instruments.items():
        ds = dataset_reader(instrument)
        ds = read_data_exclude(ds, format_species(species), site, instrument,
                               combined=True)
        ds = ds.sel(time=slice(*date))

        if len(ds.time) == 0:
            raise ValueError(f"No data retained for {species} {site} {instrument}. " + \
                             "Check dates in data_combination or omit this instrument.")

        datasets[instrument] = ds

    return datasets


def combine_baseline(network, species, site,
                     verbose = True,
                     dropna = True,
                     reference_dataset = None):
    '''Combine ALE/GAGE/AGAGE baseline datasets for a given species and site

    Args:
        network (str): Network
        species (str): Species
        site (str): Site
        verbose (bool, optional): Print verbose output. Defaults to False.
        dropna (bool, optional): Drop all time points where mf is NaN. Default to True.
        reference_dataset (xr.Dataset, optional): Combined mole fraction dataset to
            use as the instrument/time reference when selecting baseline flags.
            If None, combine_datasets is called internally.

    Returns:
        xr.Dataset: Dataset containing data
    '''

    def baseline_reader(instrument):
        ds = read_baseline(network, species, site, instrument,
                           verbose=verbose,
                           flag_name="git_pollution_flag",
                           dropna=dropna)
        return define_instrument_type(ds, instrument)

    datasets = _read_combined_instrument_datasets(network,
                                                  species,
                                                  site,
                                                  baseline_reader)
    dss = list(datasets.values())

    ds_candidates = xr.concat(dss, dim="time", combine_attrs="override")
    ds_candidates = ds_candidates.sortby("time")

    if reference_dataset is None:
        reference_dataset = combine_datasets(network,
                                             species,
                                             site,
                                             verbose=verbose,
                                             dropna=dropna)

    if "instrument_type" not in reference_dataset.variables:
        raise ValueError("reference_dataset must contain instrument_type to align baseline flags")

    candidate_index = pd.MultiIndex.from_arrays(
        [ds_candidates.time.values, ds_candidates.instrument_type.values],
        names=["time", "instrument_type"]
    )
    baseline_lookup = pd.Series(ds_candidates.baseline.values, index=candidate_index)
    baseline_lookup = baseline_lookup[~baseline_lookup.index.duplicated(keep="first")]

    reference_index = pd.MultiIndex.from_arrays(
        [reference_dataset.time.values, reference_dataset.instrument_type.values],
        names=["time", "instrument_type"]
    )
    baseline_aligned = baseline_lookup.reindex(reference_index)

    if baseline_aligned.isna().any():
        n_missing = int(baseline_aligned.isna().sum())
        raise ValueError(f"Missing baseline flags for {n_missing} combined data points")

    ds_combined = xr.Dataset(
        data_vars={"baseline": ("time", baseline_aligned.astype(np.int8).to_numpy())},
        coords={"time": reference_dataset.time.values},
    )

    ds_combined["time"].attrs = reference_dataset.time.attrs.copy()
    ds_combined["baseline"].attrs = ds_candidates.baseline.attrs.copy()
    # Take global attributes from the most recently operating instrument, for consistency
    # with combine_datasets (issue #167)
    ds_combined.attrs = most_recent_dataset(dss).attrs.copy()

    # Summarise the instrument provenance across every contributing instrument, mirroring
    # the combined mole fraction file, rather than inheriting a single instrument's
    # identity from most_recent_dataset above. Otherwise a baseline file spanning
    # ALE+GAGE+GCMD+Medusa would report instrument = instrument_type = "GCMS-Medusa"
    # (issue #168).
    instrument_rec = [{"instrument": ds.attrs["instrument"],
                       "instrument_date": str(ds.time[0].dt.strftime("%Y-%m-%d").values),
                       "instrument_comment": ds.attrs.get("instrument_comment", "")}
                      for ds in dss]
    ds_combined = format_attributes_global_instruments(ds_combined, instrument_rec)

    # instrument_type lists every instrument, ordered like the mole fraction file.
    instrument_numbers = list(np.unique(ds_candidates.instrument_type.values))
    ds_combined.attrs["instrument_type"] = "/".join(get_instrument_type(instrument_numbers,
                                                                        network))

    # Global attributes
    ds_combined.attrs["instrument_selection"] = instrument_selection_text
    ds_combined.attrs["start_date"] = str(ds_combined.time[0].dt.strftime("%Y-%m-%d %H:%M:%S").values)
    ds_combined.attrs["end_date"] = str(ds_combined.time[-1].dt.strftime("%Y-%m-%d %H:%M:%S").values)

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
        with ZipFile(out_path, mode="a", compression=ZIP_DEFLATED, compresslevel=6) as zip:
            member_path = "/".join(part for part in output_subpath.split("/") if part)
            member_path = f"{member_path}/{filename}" if member_path else filename
            if member_path in zip.namelist():
                source_members = [member for member in zip.infolist() if member.filename != member_path]
                existing_data = {member.filename: zip.read(member) for member in source_members}
                zip.close()
                with ZipFile(out_path, mode="w", compression=ZIP_DEFLATED, compresslevel=6) as replacement:
                    for name, data in existing_data.items():
                        replacement.writestr(name, data)
                    replacement.writestr(member_path, ds.to_netcdf())
                return
            zip.writestr(member_path, ds.to_netcdf())
    
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
                                     extra=extra, version=version_str,
                                     network_out=network_out)

    ds_out = ds.copy(deep = True)

    # Select time slice
    if end_date:
        ds_out = ds_out.sel(time=slice(None, end_date))

        if len(ds_out.time) == 0:
            raise ValueError(f"No data retained for {ds_out.attrs['species']} when trying write {filename} after applying end date. " + \
                            "Check dates in release schedule or omit this instrument.")

        ds_out.attrs["end_date"] = str(ds_out.time[-1].dt.strftime("%Y-%m-%d %H:%M:%S").values)

    if len(ds_out.time) == 0:
        raise ValueError(f"No data retained for {ds_out.attrs['species']} when trying write {filename} after applying release schedule end dates. " + \
                        "Check dates in release schedule or omit this instrument.")

    ds_out.attrs["start_date"] = str(ds_out.time[0].dt.strftime("%Y-%m-%d %H:%M:%S").values)
    ds_out.attrs["end_date"] = str(ds_out.time[-1].dt.strftime("%Y-%m-%d %H:%M:%S").values)

    output_write(ds_out, out_path, filename,
                output_subpath=output_subpath, verbose=verbose)


def get_data_read_function(network, instrument):
    """Get the data read function for a given network and instrument.
    
    Args:
        network (str): Network name.
        instrument (str): Instrument name.
    
    Returns:
        function: The data read function for the specified network and instrument.
    """
    
    try:
        with open_data_file("data_read_functions.json", network=network) as f:
            read_functions = json.load(f)
    except FileNotFoundError:
        error_message = f"data_read_functions.json not found for network {network}. " + \
                        "Please ensure the file exists and is correctly formatted. " + \
                        "This is a new requirement for the agage_archive package. " + \
                        "Please add to your repository in data/network/data_read_functions.json."
        raise FileNotFoundError(error_message)
    
    if instrument not in read_functions:
        error_message = f"Instrument {instrument} not found in {network}/data_read_functions.json for network {network}."
        raise ValueError(error_message)

    return globals()[read_functions[instrument]]
