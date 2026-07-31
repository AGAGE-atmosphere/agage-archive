import numpy as np
import pandas as pd
import traceback
from tqdm import tqdm

from agage_archive.config import Paths, open_data_file, data_file_list, data_file_path, \
    copy_to_archive, delete_archive, create_empty_archive
from agage_archive.data_selection import read_release_schedule, read_data_combination, \
    choose_scale_defaults_file, data_combination_species
from agage_archive.io import combine_datasets, combine_baseline, \
    read_baseline, output_dataset, get_data_read_function
from agage_archive.checks import check_input_files
from agage_archive.formatting import format_species
from agage_archive.convert import monthly_baseline
from agage_archive.definitions import define_instrument_number, instrument_selection_text


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


def timestamps_match(ds_a, ds_b):
    """Check that two datasets have exactly the same time axis

    Args:
        ds_a (xr.Dataset): First dataset
        ds_b (xr.Dataset): Second dataset

    Returns:
        bool: True if the time coordinates are identical in length, order and value
    """

    return np.array_equal(ds_a.time.values, ds_b.time.values)


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
        instrument_names = [k for k, v in define_instrument_number(ds.attrs["network"]).items() if v in instrument_types]
        instrument_names = ", ".join(instrument_names)

        raise ValueError(f"Duplicate timestamps in {species} at {site}: {duplicated_str} for instrument {instrument_names}")

    if ds_baseline is None:
        return

    if ds_baseline["time"].to_series().duplicated().any():
        raise ValueError(f"Duplicate timestamps in baseline for {species} at {site}")

    # check that the time stamps are the same in the data and baseline files.
    # Compare the raw values: an xarray comparison aligns both operands on the time
    # coordinate first, so ds_baseline.time != ds.time compares each timestamp with
    # itself and is always False, whatever the two datasets actually contain
    if not timestamps_match(ds_baseline, ds):
        raise ValueError(f"Data and baseline files for {species} at {site} have different timestamps: "
                         f"{len(ds.time)} data points, {len(ds_baseline.time)} baseline points")
        

def run_individual_site(site, species, network, instrument,
                        rs, read_function, read_baseline_function, instrument_out,
                        baseline=False,
                        monthly=False,
                        verbose=False,
                        resample=True,
                        top_level_only=False,
                        flask_pair_agreement=False):
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
        flask_pair_agreement (bool, optional): Apply the flask pair-agreement
            screen when reading flask datasets. Default to False.
    """

    if read_function.__name__ == "read_gcwerks_flask":
        site_str = site.lower()
    else:
        site_str = ""

    paths = Paths(network, errors="ignore_outputs", site = site_str)

    error_log = []

    try:

        if rs.loc[species, site].lower() != "x":

            read_kwargs = dict(verbose=verbose,
                               resample=resample,
                               scale=choose_scale_defaults_file(network, instrument, site=site))
            if read_function.__name__ == "read_gcwerks_flask":
                read_kwargs["flask_pair_agreement"] = flask_pair_agreement

            ds = read_function(network, species, site, instrument, **read_kwargs)

            # read_baseline_function is None for instruments that have no baseline flags
            # (e.g. flask data), in which case baseline products are skipped
            if baseline and read_baseline_function is not None:
                ds_baseline = read_baseline_function(network, species, site, instrument,
                                            flag_name = baseline,
                                            verbose = verbose)
            else:
                ds_baseline = None
                
            run_timestamp_checks(ds, ds_baseline, species, site)

            # If multiple instruments, store individual file in subdirectory
            instrument_dates = read_data_combination(network, species, site,
                                                    verbose=False)

            # Whether this species is named explicitly in the site's data_combination
            # file. read_data_combination returns a default when it isn't, so its return
            # value alone can't distinguish "one instrument, chosen deliberately" from
            # "no entry, so anyone may claim the top-level file"
            has_data_combination_entry = format_species(species) in \
                data_combination_species(network, site)

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
                    if not "instrument_selection" in ds.attrs:
                        raise ValueError(f"Instrument selection text not found in attributes")
                    instrument_selection_text_str = ds.attrs["instrument_selection"] #Should default to "Individual instruments"
                else:
                    instrument_str = ""
                    # In this case, change the instrument selection text to show that it's the recommended file
                    instrument_selection_text_str = instrument_selection_text

                # Check if a file already exists in the top-level (recommended) directory
                if output_subpath == f"{species}":
                    existing = data_file_list(network=network,
                                    sub_path=paths.output_path,
                                    pattern = f"{format_species(species)}/{network.lower()}_{site.lower()}_{format_species(species)}*.nc",
                                    errors="ignore")[2]
                    if existing:
                        if has_data_combination_entry:
                            # run_combined_site owns the top-level file for this species,
                            # which is how a single instrument can be marked as the
                            # recommended record. Nothing more to do here.
                            return (site, species, "")

                        # Otherwise another individual instrument has already claimed the
                        # recommended slot, and which one got there first is decided by
                        # processing order rather than by anyone's judgement. Fail loudly
                        # rather than silently discarding this instrument's record.
                        raise ValueError(
                            f"More than one instrument is eligible to write the top-level "
                            f"file for {species} at {site}: {instrument} found "
                            f"{existing[0]} already written by another instrument. "
                            f"Add a row for {format_species(species)} to "
                            f"data_combination_{site.upper()}.csv so that the recommended "
                            "record is chosen explicitly.")

                ds.attrs["instrument_selection"] = instrument_selection_text_str
                output_dataset(ds, network, instrument=instrument_str,
                            output_subpath=output_subpath,
                            end_date=rs.loc[species, site],
                            verbose=verbose)

                if ds_baseline is not None:
                    if not timestamps_match(ds_baseline, ds):
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
                elif monthly and not baseline:
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
                              top_level_only=False,
                              flask_pair_agreement=False):
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
        flask_pair_agreement (bool, optional): Apply the flask pair-agreement
            screen when reading flask datasets. Default to False.
    """
    
    rs = read_release_schedule(network, instrument)

    read_function = get_data_read_function(network, instrument)
    read_baseline_function = read_baseline
    instrument_out = instrument.lower()

    if instrument.upper() == "ALE" or instrument.upper() == "GAGE":
        instrument_out = instrument.lower() + "-gcmd"
    elif instrument.upper() == "GCMS-MEDUSA-FLASK":
        read_baseline_function = None

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
                                            baseline, monthly, verbose, resample, top_level_only,
                                            flask_pair_agreement)
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
                    resample=True,
                    flask_pair_agreement=False):
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
        flask_pair_agreement (bool, optional): Apply the flask pair-agreement
            screen when reading flask datasets. Default to False.
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
                                  scale=choose_scale_defaults_file(network, "combined", site=site),
                                  verbose=verbose, resample=resample,
                                  flask_pair_agreement=flask_pair_agreement)

            if baseline:
                if verbose:
                    print(f"... combining baselines for {sp} at {site}")
                # Note that GIT baselines is hard-wired here because Met Office not available for ALE/GAGE
                ds_baseline = combine_baseline(network, sp, site,
                                            verbose=verbose,
                                            reference_dataset=ds)

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
                             resample=True,
                             flask_pair_agreement=False):
    """Process combined data files for a given network.
    Reads the data selection file to determine which sites to process

    Args:
        network (str): Network for output filenames
        baseline (bool): Process baselines. Boolean as only one baseline flag is available (GIT)
        monthly (bool): Produce monthly baseline files
        verbose (bool): Print progress to screen
        species (list): List of species to process. If empty, process all species
        resample (bool, optional): Whether to resample the data, if needed. Default to True.
        flask_pair_agreement (bool, optional): Apply the flask pair-agreement
            screen when reading flask datasets. Default to False.
    """

    if not isinstance(species, list):
        raise TypeError("Species must be a list")

    # Check data combination files for list of sites
    _, _, files = data_file_list(network = network,
                                sub_path = "data_combination",
                                pattern = f"*.csv")
    
    sites_dc = sorted([f.split(".")[0].split("_")[-1] for f in files])

    if not sites:
        sites = sites_dc.copy()
    else:
        # Check if sites are in data_combination files if not, remove from sites
        sites = [site for site in sites if site in sites_dc]

    error_log = []

    for site in tqdm(sites):
        result = run_combined_site(site, species, network, baseline, monthly,
                                   verbose, resample, flask_pair_agreement)
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
            top_level_only=False,
            flask_pair_agreement=False,
            check_inputs=False):
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
        flask_pair_agreement (bool, optional): Only accept data points where paired flask measurements agree within 2-sigma.
            Default to False.
        check_inputs (bool, optional): Run the input-file consistency checks
            (`checks.check_input_files`) before processing, and abort with a single error
            listing every problem if any are found. Default to False.
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

    if not isinstance(flask_pair_agreement, bool):
        raise TypeError("flask_pair_agreement must be a boolean")

    if not isinstance(check_inputs, bool):
        raise TypeError("check_inputs must be a boolean")

    # Validate the input configuration before touching the archive, so a bad config fails
    # up front rather than part-way through a run.
    if check_inputs:
        check_input_files(network)

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
        print("#########################################")
        print("#####Processing combined instruments######")
        print("#########################################")

        run_combined_instruments(network,
                                baseline=baseline, verbose=True,
                                monthly=monthly, species=species, sites=sites,
                                resample=resample,
                                flask_pair_agreement=flask_pair_agreement)

    # If include is empty, process all instruments in release schedule
    if len(instrument_include) == 0:
        _, _, files = data_file_list(network = network,
                                    sub_path = "data_release_schedule",
                                    pattern = f"*.csv")
        # Sort so that processing order doesn't depend on the order in which the
        # filesystem happens to return the release schedule files. Where more than one
        # instrument is eligible to write the top-level file for a species, the first
        # one processed wins, so an unsorted list makes the archive irreproducible.
        instruments = sorted([f.split(".")[0].split("_")[-1] for f in files])
    else:
        instruments = instrument_include

    print("#########################################")
    print("#####Processing individual instruments######")
    print("#########################################")

    for instrument in tqdm(instruments):
        if instrument not in instrument_exclude:
            baseline_flag = {True: "git_pollution_flag", False: ""}[baseline]
            run_individual_instrument(network, instrument, 
                                    baseline=baseline_flag, verbose=True,
                                    monthly=monthly, species=species, sites=sites,
                                    resample=resample, top_level_only=top_level_only,
                                    flask_pair_agreement=flask_pair_agreement)

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
