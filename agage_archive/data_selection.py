import pandas as pd
import numpy as np
import json

from agage_archive.config import open_data_file, data_file_list
from agage_archive.formatting import format_species


def read_data_combination(network, species, site,
                        verbose=True):
    '''Read instrument dates from Excel file

    Args:
        species (str): Species
        site (str): Site code
        verbose (bool): Print warning message if no instrument dates found

    Returns:
        dict: Dictionary of instrument dates
    '''

    warning_message = f"WARNING: No instrument dates found for {species} at {site.upper()}."

    default_output = {"GCMS-Medusa": [None, None]}

    # Check if relevant file exists
    _, _, files = data_file_list(network = network,
                                sub_path = "data_combination",
                                pattern = f"*{site.upper()}.csv"
                                )

    if len(files) == 0:
        if verbose:
            print(warning_message)
        return default_output

    if len(files) > 1:
        raise ValueError(f"Found too many data_combination files for {site}")

    # Read the data_combination file
    with open_data_file(files[0], network = network, sub_path = "data_combination") as f:
        df = pd.read_csv(f, comment = "#", index_col = "Species")
    
    # Look for species name in table
    df = df[df.index == format_species(species)]

    if len(df) == 0:
        if verbose:
            print(warning_message)
        return default_output
    
    if len(df) > 1: 
        raise ValueError("Can only have each species appear at most once in data_selection table")

    # Extract instrument names from column names
    instruments = [col.split(" ")[0] for col in df.columns]

    # For each instrument, find start and end dates
    instrument_dates = {}

    for instrument in set(instruments):

        dates = df.loc[:, [f"{instrument} start", f"{instrument} end"]].values[0].astype(str)

        if "x" not in dates:
            instrument_dates[instrument] = [date if date != "nan" else None for date in dates]

    if len(instrument_dates) == 0:
        if verbose:
            print(warning_message)
        return default_output
    
    if len(instrument_dates) == 1:
        # Do nothing. This gives us a way of preferring one instrument
        pass

    return instrument_dates


def read_release_schedule(network, instrument,
                          species = None,
                          site = None):
    '''Read release schedule from csv files

    Args:
        network (str): Network
        instrument (str): Instrument
        species (str): Species
        site (str): Site code

    Returns:
        pd.DataFrame: Release schedule
    '''
    # Read csv file
    with open_data_file(f"data_release_schedule_{instrument}.csv",
                        network = network, sub_path = "data_release_schedule") as f:
        # Read header lines
        header = [f.readline().decode("utf-8-sig")]
        while header[-1][0] == "#":
            pos = f.tell()
            header.append(f.readline().decode("utf-8"))

        f.seek(pos)

        df = pd.read_csv(f)

    # Find general release line
    general_end_line = [h for h in header if "# GENERAL" in h.upper()][0].upper()

    general_end_date = general_end_line.split("# GENERAL RELEASE DATE:")[1].strip()

    # Remove whitespace
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

    # If NaN in df, replace with general end date
    df = df.fillna(general_end_date)

    df.set_index("Species", inplace=True)

    if species is not None:

        if format_species(species) not in df.index.str.lower():
            raise ValueError(f"No release schedule found for {species} at {site}")

        if df.loc[species, site] == "x":
            # Return a value before any data was collected, to remove everything
            return "1970-01-01"
        elif not isinstance(df.loc[species, site], str):
            return None
        else:
            return df.loc[species, site]
    else:
        return df


def choose_scale_defaults_file(network, instrument, site=""):
    """ Choose the scale_defaults file. If a file exists with an appropriate name, use that. 
    Otherwise, use the defaults file.

    Args:
        network (str): Network
        instrument (str): Instrument

    Returns:
        str: Name of the scale_defaults file
    """

    scale_defaults = "defaults"
    _, _, scale_defaults_files = data_file_list(network=network,
                                        pattern = f"scale_defaults_*.csv",
                                        errors="ignore")
    
    # Split the file name into parts, separated by '-'
    # Sort by the length of the parts, so that more specific files are checked first
    scale_defaults_files_sorted = sorted(scale_defaults_files, key=lambda x: len(x.split("_")), reverse=True)

    for file in scale_defaults_files_sorted:
        filename_parts = file.split(".")[0].split("_")

        if len(filename_parts) == 4:
            # An instrument and site is specified
            _, _, file_inst, file_site = filename_parts
            if file_inst.lower() == instrument.lower() and file_site.lower() == site.lower():
                scale_defaults = "defaults_" + file_inst + "_" + file_site
                break
        elif len(filename_parts) == 3:
            file_inst = filename_parts[2]
            if file_inst.lower() == instrument.lower():
                scale_defaults = "defaults_" + file_inst
                break
    
    return scale_defaults


def calibration_scale_default(network, species,
                              scale_defaults_file="defaults"):
    '''Get default calibration scale

    Args:
        species (str): Species

    Returns:
        str: Calibration scale
    '''

    # Check if scale_defaults_file exists
    with open_data_file(f"scale_{scale_defaults_file}.csv", network=network) as f:
        if f is None:
            raise ValueError(f"No scale_{scale_defaults_file}.csv file found. Check the path.")

    # Read scale_defaults csv file
    with open_data_file(f"scale_{scale_defaults_file}.csv", network=network) as f:
        scale_defaults = pd.read_csv(f, index_col="Species")
    
    scale_defaults = scale_defaults.map(lambda x: x.strip() if isinstance(x, str) else x)

    if format_species(species) not in scale_defaults.index.str.lower():
        raise ValueError(f"No default calibration scale found for {species}")
    else:
        return scale_defaults.loc[format_species(species), "calibration_scale"]


def read_data_exclude(ds, species, site, instrument,
                      combined=False):
    '''Read data_exclude file and return start and end date for exclusion

    Args:
        ds (xr.Dataset): Dataset
        species (str): Species
        site (str): Site code
        instrument (str): Instrument
        combined (bool): If True, only exclude data if combined_only is set to 'y'. 
            Used for excluding data in combined data file

    Returns:
        xr.Dataset: Dataset with NaNs between start and end dates
    '''

    # Determine if data_exclude folder contains file for site.upper()
    _, _, files = data_file_list(network = ds.attrs["network"],
                                 sub_path="data_exclude",
                                 pattern = f"*{site.upper()}.csv")

    if len(files) == 0:
        return ds

    if len(files) > 1:
        raise ValueError(f"Multiple data_exclude files found for {site.upper()}. Check the path.")

    # Read data_exclude
    with open_data_file(files[0], network = ds.attrs["network"],
                        sub_path="data_exclude") as f:
        data_exclude = pd.read_csv(f, comment="#")

    # Read variable defaults to find what to do with missing data
    with open_data_file("variables.json", this_repo=True) as f:
        variable_defaults = json.load(f)

    # Read variable defaults for non-public data to find what to do with missing data
    with open_data_file("variables_not_public.json", this_repo=True) as f:
        variable_np_defaults = json.load(f)
    variable_defaults.update(variable_np_defaults)

    # Remove whitespace from strings
    data_exclude = data_exclude.map(lambda x: x.strip() if isinstance(x, str) else x)

    # columns are Species, Instrument, Start, End, combined_only
    # See if there is a combined_only column (case insensitive)
    if "combined_only" in data_exclude.columns.str.lower():
        combined_only_col = data_exclude.columns[data_exclude.columns.str.lower() == "combined_only"][0]
        if combined:
            data_exclude = data_exclude[data_exclude[combined_only_col].str.lower() == "y"]
        else:
            data_exclude = data_exclude[data_exclude[combined_only_col].str.lower() != "y"]

    # Find rows that match species and instrument and then extract start and end dates
    data_exclude = data_exclude[(data_exclude["Species"] == format_species(species)) &
                                (data_exclude["Instrument"] == instrument)][["Start", "End"]]

    if len(data_exclude) == 0:
        return ds
    else:
        # Replace values in xarray dataset with NaN between start and end dates
        # The remove_flagged value in variables.json determines what to do with the missing data
        for start, end in data_exclude.values:
            exclude_dict = dict(time = slice(start, end))
            for var in ds.variables:
                if "time" in var:
                    continue
                elif variable_defaults[var]["remove_flagged"] == "True":
                    ds[var].loc[exclude_dict] = np.nan
                elif variable_defaults[var]["remove_flagged"] == "False":
                    continue
                elif variable_defaults[var]["remove_flagged"] == "Zero":
                    ds[var].loc[exclude_dict] = 0
                else:
                    raise ValueError(f"Unknown remove_flagged value for {var}. Check in variables.json or variables_not_public.json")

        return ds
    

