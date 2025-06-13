import pandas as pd
import traceback

from agage_archive.config import Paths, open_data_file, data_file_list, data_file_path, \
    copy_to_archive, delete_archive, create_empty_archive
from agage_archive.data_selection import read_release_schedule, read_data_combination, choose_scale_defaults_file
from agage_archive.io import combine_datasets, combine_baseline, \
    read_nc, read_baseline, read_ale_gage, read_gcwerks_flask, read_gcms_magnum, \
    output_dataset
from agage_archive.formatting import format_species
from agage_archive.convert import monthly_baseline
from agage_archive.definitions import instrument_number, instrument_selection_text


def get_error(e):
    """Get error message from exception

    Args:
        e (Exception): Exception object

    Returns:
        str: Error message
    """
    tb = traceback.extract_tb(e.__traceback__)

    stack_files_and_lines = []
    
    for t in tb:
        if "_archive" in t.filename:
            # Only include the filename and line no, not the full path
            stack_files_and_lines.append(f"{t.filename.split('/')[-1].split('.')[0]} (line {t.lineno})")

    error_type = type(e).__name__
    return f"{error_type} in stack: {' / '.join(stack_files_and_lines)}. {str(e)}"


def run_timestamp_checks(ds,
                        ds_baseline=None,
                        species="",
                        site=""):

    # Check for duplicate time stamps
    timestamps = ds["time"].to_series()
    if timestamps.duplicated().any():
        # Create list of duplicated timestamps
        duplicated = timestamps[timestamps.duplicated()].unique()
        duplicated_str = ", ".join([str(d) for d in duplicated])

        # List of instrument types that have duplicate timestamps
        instrument_types = ds["instrument_type"].to_series()
        instrument_types = instrument_types[timestamps.duplicated()].unique()

        # find instrument name in instrument_number
        instrument_names = [k for k, v in instrument_number.items() if v in instrument_types]
        instrument_names = ", ".join(instrument_names)

        raise ValueError(f"Duplicate timestamps in {species} at {site}: {duplicated_str} for instrument {instrument_names}")

    if ds_baseline:
        if ds_baseline["time"].to_series().duplicated().any():
            raise ValueError(f"Duplicate timestamps in baseline for {species} at {site}")
    
    # check that the time stamps are the same in the data and baseline files
    if ds_baseline:
        if (ds_baseline.time != ds.time).any():
            raise ValueError(f"Data and baseline files for {species} at {site} have different timestamps")
        

def run_individual_site(site, species, network, instrument,
                        rs, read_function, read_baseline_function, instrument_out,
                        baseline=False,
                        monthly=False,
                        verbose=False,
                        resample=True,
                        top_level_only=False):
    """Process individual data files for a given site.
    Reads the release schedule for the site

    Args:
        site (str): Site to process. Must match sheet names in release schedule, e.g.:
            "MHD", "MLO", "SMO", ...
        species (str): species to process. If empty, process all species
        network (str): Network for output filenames
        instrument (str): Instrument to process. Must match sheet names in release schedule, e.g.:
            "AGAGE", "ALE", "GAGE", "GCMD", ...
        rs (pd.DataFrame): Release schedule
        read_function (function): Function to read data files
        read_baseline_function (function): Function to read baseline files
        instrument_out (str): Instrument name for output filenames
        baseline (bool): Process baselines. Boolean as only one baseline flag is available (GIT)
        monthly (bool): Produce monthly baseline files
        verbose (bool): Print progress to screen
        resample (bool, optional): Whether to resample the data, if needed. Default to True.
        top_level_only (bool, optional): Whether to only output to the top-level directory, 
            and ignore the individual instrument folder. Default to False.
    """

    if read_function.__name__ == "read_gcwerks_flask":
        site_str = site.lower()
    else:
        site_str = ""

    paths = Paths(network, errors="ignore_outputs", site = site_str)

    error_log = []

    try:

        if rs.loc[species, site].lower() != "x":

            ds = read_function(network, species, site, instrument,
                            verbose=verbose,
                            resample=resample,
                            scale=choose_scale_defaults_file(network, instrument))

            if baseline:
                ds_baseline = read_baseline_function(network, species, site, instrument,
                                            flag_name = baseline,
                                            verbose = verbose)
            else:
                ds_baseline = None
                
            run_timestamp_checks(ds, ds_baseline, species, site)

            # If multiple instruments, store individual file in subdirectory
            instrument_dates = read_data_combination(network, species, site,
                                                    verbose=False)

            if top_level_only:
                folders = []
            else:
                folders = [f"{species}/individual-instruments"]

            # if there is no combined data file, also store individual file in top-level directory
            if len(instrument_dates) <= 1:
                # Add to top-level directory
                folders.append(f"{species}")
            else:
                if top_level_only:
                    raise ValueError(f"Looks like combined instruments has been run for {species} at {site}, but top_level_only is set to True")

            for output_subpath in folders:

                if "individual" in output_subpath:
                    instrument_str = instrument_out
                    instrument_selection_text_str = ds.attrs["instrument_selection"] #Should default to "Individual instruments"
                else:
                    instrument_str = ""
                    # In this case, change the instrument selection text to show that it's the recommended file
                    instrument_selection_text_str = instrument_selection_text

                # Check if file already exists in the top-level directory. 
                # This can happen if only one instrument is specified in data_combination
                if output_subpath == f"{species}":
                    if data_file_list(network=network,
                                    sub_path=paths.output_path,
                                    pattern = f"{format_species(species)}/{network.lower()}_{site.lower()}_{format_species(species)}*.nc",
                                    errors="ignore")[2]:
                        # New behaviour: This is OK, as it provides a way for us to have one recommended instrument
                        return (site, species, "")

                ds.attrs["instrument_selection"] = instrument_selection_text_str
                output_dataset(ds, network, instrument=instrument_str,
                            output_subpath=output_subpath,
                            end_date=rs.loc[species, site],
                            verbose=verbose)

                if baseline:
                    if (ds_baseline.time != ds.time).any():
                        raise ValueError(f"Baseline and data files for {species} at {site} have different timestamps")
                    # Try-except to catch errors when baseline flags are missing, but still continue processing
                    try:
                        ds_baseline.attrs["instrument_selection"] = instrument_selection_text_str
                        output_dataset(ds_baseline, network, instrument=instrument_str,
                                output_subpath=output_subpath + "/baseline-flags",
                                end_date=rs.loc[species, site],
                                extra="git-baseline",
                                verbose=verbose)

                        if monthly:
                            ds_baseline_monthly = monthly_baseline(ds, ds_baseline)
                            ds_baseline_monthly.attrs["instrument_selection"] = instrument_selection_text_str
                            output_dataset(ds_baseline_monthly, network, instrument=instrument_str,
                                output_subpath=output_subpath + "/monthly-baseline",
                                end_date=rs.loc[species, site],
                                extra="monthly-baseline",
                                verbose=verbose)
                    except Exception as e:
                        error_log.append(get_error(e))
                else:
                    if monthly:
                        raise NotImplementedError("Monthly baseline files can only be produced if baseline flag is specified")

            error_log.append("")

        else:

            error_log.append("")

    except Exception as e:

        error_log.append(get_error(e))
    
    return (site, species, error_log[0])


def run_individual_instrument(network, instrument,
                              verbose = False,
                              baseline = "",
                              monthly = False,
                              species = [],
                              sites = [],
                              resample=True,
                              top_level_only=False):
    """Process individual data files for a given instrument.
    Reads the release schedule for the instrument

    Args:
        instrument (str): Instrument to process. Must match sheet names in release schedule, e.g.:
            "AGAGE", "ALE", "GAGE", "GCMD", ...
        verbose (bool): Print progress to screen
        baseline (str): Baseline flag to use. If empty, don't process baselines
        monthly (bool): Produce monthly baseline files
        species (list): List of species to process. If empty, process all species
        resample (bool, optional): Whether to resample the data, if needed. Default to True.
        top_level_only (bool, optional): Whether to only output to the top-level directory,
            and ignore the individual instrument folder. Default to False.
    """
    
    rs = read_release_schedule(network, instrument)

    if instrument.upper() == "ALE" or instrument.upper() == "GAGE":
        read_function = read_ale_gage
        read_baseline_function = read_baseline
        instrument_out = instrument.lower() + "-gcmd"
    elif instrument.upper() == "GCMS-MEDUSA-FLASK":
        read_function = read_gcwerks_flask
        read_baseline_function = None
        instrument_out = "gcms-medusa-flask"
    elif instrument.upper() == "GCMS-MAGNUM":
        read_function = read_gcms_magnum
        read_baseline_function = read_baseline
        instrument_out = "gcms-magnum"
    else:
        read_function = read_nc
        read_baseline_function = read_baseline
        instrument_out = instrument.lower()

    if species:
        # Process only those species that are in the release schedule
        species_to_process = [sp for sp in species if sp in rs.index.values]
        if not species_to_process:
            print(f"No species to process for {instrument}, skipping...")
            return
    else:
        # Process all species in the release schedule
        species_to_process = rs.index.values

    error_log = []

    # Process for all species and sites
    for sp in species_to_process:
        for site in rs.columns:
            if site in sites or not sites:
                if verbose:
                    print(f"Processing {sp} at {site} for {instrument}")
                result = run_individual_site(site, sp, network, instrument,
                                            rs, read_function, read_baseline_function, instrument_out,
                                            baseline, monthly, verbose, resample, top_level_only)
                error_log.append(result)

    has_errors = any([error[2] for error in error_log])

    if has_errors:
        # save errors to file
        with open(data_file_path("error_log_individual.txt", network=network, errors="ignore"), "a") as f:
            # write the date and time of the error
            f.write("Processing attempted on " + pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S") + "\n")
            for error in error_log:
                if error[2]:
                    f.write(f"{error[0]} {error[1]}: {error[2]}\n")


def run_combined_site(site, species, network, 
                    baseline=False,
                    monthly=False,
                    verbose=False,
                    resample=True):
    """Process combined data files for a given site.
    Reads the data selection file to determine which species to process

    Args:
        site (str): Site to process. Must match sheet names in data selection file
        species (list): List of species to process. If empty, process all species
        network (str): Network for output filenames
        baseline (bool): Process baselines. Boolean as only one baseline flag is available (GIT)
        monthly (bool): Produce monthly baseline files
        verbose (bool): Print progress to screen
        resample (bool, optional): Whether to resample the data, if needed. Default to True.
    """

    print(f"Processing files for {site}")

    instrument_dates = {}
    for sp in species:
        instrument_dates[sp] = read_data_combination(network, sp, site, verbose=verbose)

    # Read the data_combination file to get list of species
    with open_data_file(f"data_combination_{site.upper()}.csv",
                        sub_path="data_combination", network=network, errors="ignore") as f:
        df = pd.read_csv(f, comment="#", index_col="Species")

    # Determine species to process
    if species:
        # Process only those species that are in the data selection file
        species_to_process = [sp for sp in species if sp in df.index.values]
        if not species_to_process:
            print(f"No species to process for {site}, skipping...")
            return [(site, "None", "")]
    else:
        # Process all species in the data selection file
        species_to_process = df.index.values

    error_log = []

    # Loop through species in index
    for sp in species_to_process:

        try:

            # Produce combined dataset
            if verbose:
                print(f"... combining datasets for {sp} at {site}")
            ds = combine_datasets(network, sp, site,
                                verbose=verbose, resample=resample)

            if baseline:
                if verbose:
                    print(f"... combining baselines for {sp} at {site}")
                # Note that GIT baselines is hard-wired here because Met Office not available for ALE/GAGE
                ds_baseline = combine_baseline(network, sp, site,
                                            verbose=verbose)

            else:
                ds_baseline = None

            # Check for duplicate time stamps
            run_timestamp_checks(ds, ds_baseline, sp, site)

            output_subpath = f"{sp}"

            if verbose:
                print(f"... outputting combined dataset for {sp} at {site}")
            output_dataset(ds, network,
                        output_subpath=output_subpath,
                        instrument="",
                        verbose=verbose)
            
            if baseline:
                if verbose:
                    print(f"... outputting combined baseline for {sp} at {site}")
                output_dataset(ds_baseline, network,
                            output_subpath=output_subpath + "/baseline-flags",
                            instrument="",
                            extra="git-baseline",
                            verbose=verbose)

                if monthly:
                    ds_baseline_monthly = monthly_baseline(ds, ds_baseline)
                    output_dataset(ds_baseline_monthly, network,
                            output_subpath=output_subpath + "/monthly-baseline",
                            instrument="",
                            extra="monthly-baseline",
                            verbose=verbose)

            else:
                if monthly:
                    raise NotImplementedError("Monthly baseline files can only be produced if baseline flag is specified")

            error_log.append("")

        except Exception as e:

            error_log.append(get_error(e))

    return [(site, sp, error) for sp, error in zip(species_to_process, error_log)]


def run_combined_instruments(network,
                             baseline = False,
                             monthly = False,
                             verbose = False,
                             species = [],
                             sites = [],
                             resample=True):
    """Process combined data files for a given network.
    Reads the data selection file to determine which sites to process

    Args:
        network (str): Network for output filenames
        baseline (bool): Process baselines. Boolean as only one baseline flag is available (GIT)
        monthly (bool): Produce monthly baseline files
        verbose (bool): Print progress to screen
        species (list): List of species to process. If empty, process all species
        resample (bool, optional): Whether to resample the data, if needed. Default to True.
    """

    if not isinstance(species, list):
        raise TypeError("Species must be a list")

    # Check data combination files for list of sites
    _, _, files = data_file_list(network = network,
                                sub_path = "data_combination",
                                pattern = f"*.csv")
    
    sites_dc = [f.split(".")[0].split("_")[-1] for f in files]

    if not sites:
        sites = sites_dc.copy()
    else:
        # Check if sites are in data_combination files if not, remove from sites
        sites = [site for site in sites if site in sites_dc]

    error_log = []

    for site in sites:
        result = run_combined_site(site, species, network, baseline, monthly, verbose, resample)
        error_log.extend(result)

    has_errors = any([error[2] for error in error_log])

    if has_errors:
        # save errors to file
        with open(data_file_path("error_log_combined.txt", network=network, errors="ignore"), "a") as f:
            # write the date and time of the error
            f.write("Processing attempted on " + pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S") + "\n")
            for error in error_log:
                if error[2]:
                    f.write(f"{error[0]} {error[1]}: {error[2]}\n")


def run_all(network,
            delete = True,
            combined = True,
            baseline = True,
            monthly = True,
            instrument_include = [],
            instrument_exclude = [],
            species = [],
            sites = [],
            resample=True,
            top_level_only=False,):
    """Process data files for multiple instruments. Reads the release schedule to determine which
    instruments to process

    Args:
        delete (bool): Delete all files in output directory before running
        combined (bool): Process combined data files
        include (list): List of instruments to process. If empty, process all instruments
        exclude (list): List of instruments to exclude from processing
        baseline (bool): Process baselines. Boolean as only one baseline flag is available (GIT)
        monthly (bool): Produce monthly baseline files
        verbose (bool): Print progress to screen
        species (list): List of species to process. If empty, process all species
        resample (bool, optional): Whether to resample the data, if needed. Default to True.
        top_level_only (bool, optional): Whether to only output to the top-level directory,
            and ignore the individual instrument folder. Default to False.
    """

    if not network:
        raise ValueError("Must specify network")

    if not isinstance(network, str):
        raise TypeError("network must be a string")
    
    if not isinstance(delete, bool):
        raise TypeError("delete must be a boolean")
    
    if not isinstance(combined, bool):
        raise TypeError("combined must be a boolean")
    
    if not isinstance(baseline, bool):
        raise TypeError("baseline must be a boolean")
    
    if not isinstance(monthly, bool):
        raise TypeError("monthly must be a boolean")
    
    if not isinstance(instrument_include, list):
        raise TypeError("instrument_include must be a list")
    
    if not isinstance(instrument_exclude, list):
        raise TypeError("instrument_exclude must be a list")
    
    if not isinstance(species, list):
        raise TypeError("species must be a list")

    if not isinstance(sites, list):
        raise TypeError("sites must be a list")

    path = Paths(network, errors="ignore")

    # Delete log files, if they exist
    for log_file in ["error_log_combined.txt", "error_log_individual.txt"]:
        try:
            data_file_path(log_file, network=network, errors="ignore").unlink()
        except FileNotFoundError:
            pass

    # Check if output_path attribute is available
    if not hasattr(path, "output_path"):
        raise AttributeError("Output path not set in config.yaml")

    if delete:
        delete_archive(network)
        
    # If either out_pth is a zip file that doesn't exist, create
    create_empty_archive(network)

    # Must run combined instruments first
    if combined:
        run_combined_instruments(network,
                                baseline=baseline, verbose=True,
                                monthly=monthly, species=species, sites=sites,
                                resample=resample)

    # If include is empty, process all instruments in release schedule
    if len(instrument_include) == 0:
        _, _, files = data_file_list(network = network,
                                    sub_path = "data_release_schedule",
                                    pattern = f"*.csv")
        instruments = [f.split(".")[0].split("_")[-1] for f in files]
    else:
        instruments = instrument_include

    # Processing
    for instrument in instruments:
        if instrument not in instrument_exclude:
            baseline_flag = {True: "git_pollution_flag", False: ""}[baseline]
            run_individual_instrument(network, instrument, 
                                    baseline=baseline_flag, verbose=True,
                                    monthly=monthly, species=species, sites=sites,
                                    resample=resample, top_level_only=top_level_only)

    # Incorporate README and CHANGELOG into output directory or zip file
    try:
        readme_file = data_file_path(filename='README.md',
                                    network=network, errors = "ignore_inputs")
        copy_to_archive(readme_file, network)
    except FileNotFoundError:
        print("No README file found")

    try:
        changelog_file = data_file_path(filename='CHANGELOG.md',
                                    network=network, errors = "ignore_inputs")
        copy_to_archive(changelog_file, network)
    except FileNotFoundError:
        print("No CHANGELOG file found")

    # If error log files have been created, warn the user
    if data_file_path("error_log_combined.txt", network=network, errors="ignore").exists():
        print("!!! Errors occurred during processing. See error_log_combined.txt for details")
    if data_file_path("error_log_individual.txt", network=network, errors="ignore").exists():
        print("!!! Errors occurred during processing. See error_log_individual.txt for details")
