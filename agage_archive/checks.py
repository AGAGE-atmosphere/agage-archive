"""Pre-flight consistency checks for a network's input configuration files.

Addresses issue #97: before a processing run, verify that

- species names used in the release schedules and ``data_combination`` files are defined
  in the network's default scale file or in ``standard_names.json`` (i.e. no typos or
  inconsistent spellings that would silently fail deep in a run), and
- every configured date parses, and no end date precedes its start date.

The entry point is :func:`check_input_files`, which runs every check and raises a single
``ValueError`` listing all problems (so one pass surfaces everything, not just the first
failure). The individual ``check_*`` helpers are pure — they take already-parsed data and
return a list of problem descriptions — so they are straightforward to unit test.
"""

import logging
import pandas as pd

from agage_archive.config import open_data_file, data_file_list, load_json
from agage_archive.formatting import format_species

logger = logging.getLogger(__name__)


def _is_blank_or_excluded(value):
    """Return True for cells that carry no date: empty/NaN, or the ``x`` exclusion marker."""

    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    text = str(value).strip()
    return text == "" or text.lower() == "x"


def _try_parse_date(value):
    """Parse a date cell.

    Args:
        value: Cell value.

    Returns:
        tuple[bool, pandas.Timestamp | None]: (True, timestamp) if it parses, else
            (False, None).
    """

    try:
        return True, pd.to_datetime(str(value).strip())
    except (ValueError, TypeError):
        return False, None


def check_species_known(species_names, known_species, source):
    """Flag species whose canonical name is not defined in the reference vocabularies.

    Args:
        species_names (Iterable): Species names as they appear in a file.
        known_species (set): Canonical species names from the scale defaults and
            ``standard_names.json``.
        source (str): Name of the file being checked, for the message.

    Returns:
        list[str]: One problem per unknown species.
    """

    problems = []
    for species in species_names:
        if _is_blank_or_excluded(species):
            continue
        if format_species(str(species).strip()) not in known_species:
            problems.append(
                f"{source}: species '{species}' is not defined in the scale defaults "
                "or standard_names.json")
    return problems


def check_release_schedule_df(df, general_release_date, source):
    """Check the date cells of a release-schedule dataframe.

    Args:
        df (pandas.DataFrame): Raw release schedule (``Species`` column plus one column
            per site; cells are dates, ``x``, or blank).
        general_release_date (str): The general release date parsed from the file header.
        source (str): File name, for messages.

    Returns:
        list[str]: Problems found.
    """

    problems = []

    ok, _ = _try_parse_date(general_release_date)
    if not ok:
        problems.append(
            f"{source}: general release date '{general_release_date}' is missing or not a "
            "valid date")

    site_columns = [c for c in df.columns if c != "Species"]
    for _, row in df.iterrows():
        species = row["Species"]
        for site in site_columns:
            value = row[site]
            if _is_blank_or_excluded(value):
                continue
            ok, _ = _try_parse_date(value)
            if not ok:
                problems.append(
                    f"{source}: {species} at {site} has an invalid date '{value}'")
    return problems


def check_data_combination_df(df, source):
    """Check the date cells and start/end ordering of a data_combination dataframe.

    Args:
        df (pandas.DataFrame): Raw data_combination (``Species`` column plus paired
            ``<instrument> start`` / ``<instrument> end`` columns).
        source (str): File name, for messages.

    Returns:
        list[str]: Problems found.
    """

    problems = []
    start_columns = [c for c in df.columns if c.endswith(" start")]

    for _, row in df.iterrows():
        species = row["Species"]
        for start_column in start_columns:
            instrument = start_column[:-len(" start")]
            end_column = f"{instrument} end"

            start_ok, start_dt = False, None
            end_ok, end_dt = False, None

            start_value = row[start_column]
            if not _is_blank_or_excluded(start_value):
                start_ok, start_dt = _try_parse_date(start_value)
                if not start_ok:
                    problems.append(
                        f"{source}: {species} '{start_column}' has an invalid date "
                        f"'{start_value}'")

            if end_column in df.columns:
                end_value = row[end_column]
                if not _is_blank_or_excluded(end_value):
                    end_ok, end_dt = _try_parse_date(end_value)
                    if not end_ok:
                        problems.append(
                            f"{source}: {species} '{end_column}' has an invalid date "
                            f"'{end_value}'")

            if start_ok and end_ok and end_dt < start_dt:
                problems.append(
                    f"{source}: {species} {instrument} end date '{end_value}' is before "
                    f"its start date '{start_value}'")

    return problems


def known_species(network):
    """Canonical species names defined for a network.

    The union of the species in the network's default scale file and the keys of the
    shared ``standard_names.json``. These are the names the rest of the code can resolve,
    so any species used in a schedule or data_combination file should be among them.

    Args:
        network (str): Network.

    Returns:
        set[str]: Canonical (``format_species``) species names.
    """

    with open_data_file("scale_defaults.csv", network=network) as f:
        scale_df = pd.read_csv(f, comment="#")
    scale_species = {format_species(str(s).strip()) for s in scale_df["Species"]}

    standard_names = load_json("standard_names.json", this_repo=True)
    standard_species = {format_species(s) for s in standard_names}

    return scale_species | standard_species


def _read_release_schedule_raw(network, filename):
    """Read a release schedule as raw strings, plus its general release date.

    Unlike ``read_release_schedule`` this keeps cells verbatim (blanks stay blank, dates
    are not coerced), which is what the date checks need.

    Args:
        network (str): Network.
        filename (str): Release schedule file name.

    Returns:
        tuple[pandas.DataFrame, str]: The schedule and the general release date string
            (empty if the header has none).
    """

    with open_data_file(filename, network=network,
                        sub_path="data_release_schedule") as f:
        header = [f.readline().decode("utf-8-sig")]
        pos = 0
        while header[-1].startswith("#"):
            pos = f.tell()
            header.append(f.readline().decode("utf-8"))
        f.seek(pos)
        df = pd.read_csv(f, dtype=str)

    general_lines = [h for h in header if "# GENERAL" in h.upper()]
    if general_lines:
        general_release_date = general_lines[0].upper().split(
            "# GENERAL RELEASE DATE:")[1].strip()
    else:
        general_release_date = ""

    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    return df, general_release_date


def _read_data_combination_raw(network, filename):
    """Read a data_combination file as raw strings (blanks and ``x`` preserved).

    Args:
        network (str): Network.
        filename (str): Data combination file name.

    Returns:
        pandas.DataFrame: The raw table.
    """

    with open_data_file(filename, network=network, sub_path="data_combination") as f:
        df = pd.read_csv(f, comment="#", dtype=str)
    return df.map(lambda x: x.strip() if isinstance(x, str) else x)


def collect_input_file_problems(network):
    """Run every input-file check for a network and return all problems.

    Args:
        network (str): Network.

    Returns:
        list[str]: Problem descriptions, each naming the offending file and value. Empty
            if everything is consistent.
    """

    problems = []
    known = known_species(network)

    _, _, release_files = data_file_list(network,
                                        sub_path="data_release_schedule",
                                        pattern="*.csv")
    for filename in sorted(release_files):
        df, general_release_date = _read_release_schedule_raw(network, filename)
        problems += check_species_known(df["Species"], known, filename)
        problems += check_release_schedule_df(df, general_release_date, filename)

    _, _, combination_files = data_file_list(network,
                                            sub_path="data_combination",
                                            pattern="*.csv")
    for filename in sorted(combination_files):
        df = _read_data_combination_raw(network, filename)
        problems += check_species_known(df["Species"], known, filename)
        problems += check_data_combination_df(df, filename)

    return problems


def check_input_files(network, verbose=True):
    """Validate a network's input configuration files before a processing run.

    Checks that species names in the release schedules and data_combination files are
    defined in the scale defaults or ``standard_names.json``, that every date parses, and
    that no end date precedes its start date.

    Args:
        network (str): Network.
        verbose (bool, optional): Print a confirmation line when all checks pass.
            Defaults to True.

    Raises:
        ValueError: If any problems are found; the message lists every one.
    """

    problems = collect_input_file_problems(network)
    if problems:
        raise ValueError(
            f"Input configuration check failed for '{network}' with {len(problems)} "
            "problem(s):\n" + "\n".join(f"  - {p}" for p in problems))
    if verbose:
        logger.info(f"Input configuration for '{network}': all checks passed.")
