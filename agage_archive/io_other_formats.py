import pandas as pd
from pathlib import Path
import numpy as np
import json
import tarfile
from fnmatch import fnmatch

from agage_archive.config import Paths as Pth
from agage_archive.util import tz_local_to_utc

home = Path.home()

species_wang = {
    "cfc-11": "CFC-11S",
    "cfc-12": "CFC-12",
    "cfc-113": "F-113",
    "ccl4": "CCl4",
    "ch3ccl3": "CH3CCl3",
    "n2o": "N2O",
    "ch4": "CH4"
}

sites_wang = {
    "CMO": "ORG",
    "CGO": "CGO",
    "MHD": "MHD",
    "SMO": "SMO",
    "THD": "THD",
    "ADR": "ADR",
    "RPB": "RPB",
}

def read_wang_file(file, header_lines=6):
    """ Read one file prepared by Ray Wang (see data/ancilliary) and return a dataframe

    Args:
        file (file): File object that can be read by pandas.read_csv

    Returns:
        df (pd.DataFrame): Dataframe with one column for the species
    """


    # Reading the first N lines of the file to understand its structure
    first_lines = [file.readline().strip() for _ in range(header_lines + 1)]

    # Go back to beginning of file
    file.seek(0)

    # Read the file into a Pandas dataframe with correct headers
    complete_data = pd.read_csv(file, skiprows=header_lines, sep=r"\s+",
                                names=first_lines[header_lines-1].split(), encoding='ascii')

    complete_data.columns = complete_data.columns.astype(str)

    # Construct the datetime column and set it as the index
    complete_data['datetime'] = pd.to_datetime(complete_data['YYYY'].astype(str) + '-' + 
                                            complete_data['MM'].astype(str).str.zfill(2) + '-' + 
                                            complete_data['DD'].astype(str).str.zfill(2) + ' ' + 
                                            complete_data['hh'].astype(str).str.zfill(2) + ':' + 
                                            complete_data['min'].astype(str).str.zfill(2))
    complete_data.set_index('datetime', inplace=True)

    # Drop unnecessary columns
    complete_data.drop(columns=['time', 'DD', 'MM', 'YYYY', 'hh', 'min', 'ABSDA'], inplace=True)

    # Remove non-numeric characters
    complete_data = complete_data.replace(to_replace=r'[^\d.]', value='', regex=True)

    # Convert all data columns to float type
    complete_data = complete_data.astype(float)

    # Replace 0.0 values with NaN
    complete_data.replace(0.0, np.nan, inplace=True)

    # Return the first few rows for verification
    complete_data.head()

    return complete_data


def read_wang(species, site, network, instrument, utc = False):
    """ Read data from Ray Wang's files, concatinating individual years

    Args:
        species (str): Species name
        site (str): Site name
        network (str): Network name
        instrument (str): Instrument name
        utc (bool): Convert to UTC

    Returns:
        df (pd.DataFrame): Dataframe with one column for the species
    """

    paths = Pth()

    # Read ale_gage_sites.json
    with open(paths.root.parent / f"data/{network}/ale_gage_sites.json", "r") as file:
        site_info = json.load(file)

    # Read species_info.json
    with open(paths.root.parent / f"data/{network}/ale_gage_species.json", "r") as file:
        species_info = json.load(file)[species]

    site_name = site_info[site]["gcwerks_name"]
    site_code = sites_wang[site]

    # Open tar file for network
    tar_file = paths.root.parent / f"data/ancilliary/wang_{instrument.lower()}.tar.gz"

    with tarfile.open(tar_file, "r:gz") as tar:
        files = []

        # Determine which matching files are in the tar file
        for tarinfo in tar:
            search_str = f"*/*{site_name}/{site_code}-{instrument.lower()}*.dap"
            if fnmatch(tarinfo.name, search_str):
                files.append(tarinfo.name)

        dfs = []

        # Read each file and append to list
        for file in files:
            with tar.extractfile(file) as f:
                dfs.append(read_wang_file(f))

    df = pd.concat(dfs)

    # Only output one species
    df = df[[species_wang[species]]]

    # Rename species column
    df.rename(columns={species_wang[species]: "mf"}, inplace=True)

    df.index.name = "time"

    # Sort by time
    df.sort_index(inplace=True)

    # Remove duplicate indices
    df = df[~df.index.duplicated(keep='first')]

    # Convert to UTC, if needed
    if utc:
        df.index = tz_local_to_utc(df.index, network, site)

    return df
