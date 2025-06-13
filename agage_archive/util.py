import os
import json
import pytz
import yaml
import pandas as pd
import re
import xarray as xr

from agage_archive.config import Paths, open_data_file, data_file_path, \
    data_file_list, archive_suffix, delete_archive, create_empty_archive


def is_number(s):
    """ Check if a string is a number. 
    
    Args:
        s (str): String to check

    Returns:
        bool: True if s is a number, False otherwise
    """
    try:
        float(s)
        return True
    except ValueError:
        return False


def lookup_username():
    '''Look up username

    Returns:
        str: Username
    '''
    
    paths = Paths()

    # Take username from config file if it exists, otherwise try to get it from the system
    with open(paths.root / "config.yaml", "r") as f:
        config = yaml.safe_load(f)

    if config["user"]["name"] != "":
        return config["user"]["name"]
    else:
        try:
            return os.environ["USER"]
        except:
            try:
                return os.environ["USERNAME"]
            except:
                try:
                    return os.environ["LOGNAME"]
                except:
                    return "unknown user"


def tz_local_to_utc(index, network, site):
    """ Convert local time to UTC. 
    
    Args:
        index (pandas.DatetimeIndex): Datetime index in local time
        network (str): Network name
        site (str): Site name

    Returns:
        pandas.DatetimeIndex: Datetime index in UTC
    """

    with open_data_file("ale_gage_sites.json", network=network) as file:
        site_info = json.load(file)

    tzoffset_hours = site_info[site]["tz"].split("UTC")[1]

    local_offset = pytz.FixedOffset(int(tzoffset_hours)*60)
    
    ind = index.tz_localize(local_offset)
    
    return ind.tz_convert(None)


def excel_to_csv(file, network):
    """ Convert Excel data specification file to CSVs.

    Args:
        file (str): File name. Must be either:
            "data_release_schedule", "data_combination" or "data_exclude"
        network (str): Network name
    """

    filename = data_file_path(file + ".xlsx", network)

    if not filename.exists():
        raise ValueError(f"Check filename: {filename}")

    csv_folder_name = filename.parent / filename.name.split(".")[0]

    if not csv_folder_name.exists():
        raise ValueError(f"Create folder {csv_folder_name}")

    # Read Excel file and output worksheets to CSVs
    sheet_names = pd.ExcelFile(filename).sheet_names

    for sheet in sheet_names:
        # Read header
        header_sheet = pd.read_excel(filename,
                                    sheet_name=sheet)

        header = [header_sheet.columns[0]]
        i=0
        while header_sheet.iloc[i, 0][0] == "#":
            header.append(header_sheet.iloc[i, 0])
            i+=1

        # Special case of general release date
        if header_sheet.iloc[i, 0][:7].upper() == "GENERAL":
            header.append("# " + header_sheet.iloc[i, 0] + ": " + header_sheet.iloc[i, 1])
            i+=1

        xlsx_data = pd.read_excel(filename, sheet_name=sheet,
                                skiprows=i+1, dtype=str)
        
        csv_filename = csv_folder_name / f"{file}_{sheet}.csv"

        with open(csv_filename, "w") as f:
            for h in header:
                f.writelines(h + "\n")
            xlsx_data.to_csv(f, index = None)


def parse_fortran_format(format_string):
    """
    Parse a Fortran format string (e.g., (F10.5, 2I4,I6, 2I4,I6,1X,70(F12.3,a1)))
    and return:
      - column_specs: list of (start, end) for each field
      - column_types: list of Python types (float, int, str)

    Args:
        format_string (str): Fortran format string

    Returns:
        Tuple[List[Tuple[int, int]], List[type]]: column_specs, column_types
    """
    # Remove any leading/trailing spaces and wrapping parentheses if present
    format_string = format_string.strip()
    if format_string.startswith("(") and format_string.endswith(")"):
        format_string = format_string[1:-1].strip()

    column_specs = []
    column_types = []
    current_start = 0

    def add_column(width, pytype):
        nonlocal current_start
        # Add (start, end) to the specs
        column_specs.append((current_start, current_start + width))
        column_types.append(pytype)
        current_start += width

    def parse_token(token):
        """
        Parse a single token such as:
          - F10.5
          - I4
          - 1X
          - 2I4
          - 70(F12.3,a1)
        and expand it into column_specs and column_types.
        """
        nonlocal current_start

        # Whitespace or empty means nothing
        token = token.strip()
        if not token:
            return

        # If it starts with a digit, it might be repeat syntax: e.g. '2I4' or '70(F12.3,a1)'
        match_repeat = re.match(r"^(\d+)\((.*)\)$", token)  # e.g. '70(F12.3,a1)'
        if match_repeat:
            # This means we have N(...some tokens...) form
            repeat_count = int(match_repeat.group(1))
            inner_str = match_repeat.group(2).strip()
            # Parse the inside as an entire format (which could have multiple tokens)
            for _ in range(repeat_count):
                parse_list_of_tokens(inner_str)
            return

        # If we don't have parentheses, we might have '2I4', '2F10.5' etc.
        match_inline_repeat = re.match(r"^(\d+)([A-Z]\d+(\.\d+)?)$", token, re.IGNORECASE)
        # e.g. '2I4' => repeat=2, format=I4
        #      '2F10.5' => repeat=2, format=F10.5
        if match_inline_repeat:
            repeat_count = int(match_inline_repeat.group(1))
            format_part = match_inline_repeat.group(2)
            for _ in range(repeat_count):
                parse_token(format_part)
            return

        # Now handle single tokens: F10.5, I6, 1X, etc.
        # 1) 'Fw.d' => float
        # 2) 'Iw'   => int
        # 3) '1X'   => skip 1
        # 4) 'a1'   => string (though often used in repeated patterns)
        match_float = re.match(r"^F(\d+)\.(\d+)$", token, re.IGNORECASE)
        match_int   = re.match(r"^I(\d+)$", token, re.IGNORECASE)
        match_skip  = re.match(r"^(\d+)X$", token, re.IGNORECASE)
        match_a     = re.match(r"^a(\d+)$", token, re.IGNORECASE)

        if match_float:
            width = int(match_float.group(1))
            # parse as float
            add_column(width, float)
            return
        elif match_int:
            width = int(match_int.group(1))
            add_column(width, int)
            return
        elif match_skip:
            width = int(match_skip.group(1))
            # Just skip, no data extracted
            current_start += width
            return
        elif match_a:
            width = int(match_a.group(1))
            add_column(width, str)
            return

        # If none matched, it could be multiple tokens separated by commas:
        # e.g. "F12.3,a1". We'll let parse_list_of_tokens handle that.
        if ',' in token:
            # Break apart with parse_list_of_tokens
            parse_list_of_tokens(token)
            return

        raise ValueError(f"Unrecognized format token: '{token}'")

    def parse_list_of_tokens(format_str):
        """
        Split format_str by commas at the top level (not inside parentheses)
        and parse each token. 
        """
        # We'll split by commas that are not inside parentheses.
        # A simple approach is to track parentheses depth and split accordingly.
        parts = []
        depth = 0
        current = []
        for char in format_str:
            if char == '(':
                depth += 1
                current.append(char)
            elif char == ')':
                depth -= 1
                current.append(char)
            elif char == ',' and depth == 0:
                # top-level comma => split
                parts.append(''.join(current).strip())
                current = []
            else:
                current.append(char)
        # add the last piece
        if current:
            parts.append(''.join(current).strip())

        for p in parts:
            parse_token(p)

    # Now parse the top level
    parse_list_of_tokens(format_string)

    return column_specs, column_types


def nc_to_csv(ds):
    """Convert an xarray Dataset to a CSV format with header and data.
    Args:
        ds (xarray.Dataset): The xarray Dataset to convert
    Returns:
        Tuple[List[str], pandas.DataFrame]: A tuple containing:
            - header: A list of strings representing the header
            - df: A pandas DataFrame containing the data
    """

    # Prepare header from attributes
    header = ["# GLOBAL ATTRIBUTES:", "# ------------------------------"]
    for attr, attr_val in sorted(ds.attrs.items()):
        header_line = f"# {attr}: {str(attr_val)}"
        header.append(header_line)

    # For each variable, add its attributes to the header
    header.append("# VARIABLE ATTRIBUTES:")
    for var_name, var in ds.data_vars.items():
        header.append(f"# {var_name}:")
        for attr, attr_val in var.attrs.items():
            header_line = f"#   {attr}: {str(attr_val)}"
            header.append(header_line)
        header.append("# ------------------------------")
    header.append("# DATA:")
    # Convert to DataFrame
    df = ds.to_dataframe().reset_index()
    # Add year, month, day, hour, minute, second columns
    if "time" in df.columns:
        df["year"] = df["time"].dt.year
        df["month"] = df["time"].dt.month
        df["day"] = df["time"].dt.day
        df["hour"] = df["time"].dt.hour
        df["minute"] = df["time"].dt.minute
        df["second"] = df["time"].dt.second
    else:
        raise ValueError("Dataset does not contain a 'time' variable.")

    # Make the time columns appear at the start of the dataframe
    columns = list(df.columns)
    time_columns = ["time", "year", "month", "day", "hour", "minute", "second"]
    for col in time_columns:
        if col in columns:
            columns.remove(col)
    columns = time_columns + columns
    df = df[columns]

    return header, df


def archive_to_csv(network):
    """Convert AGAGE archive data files to CSV format.
    Args:
        network (str): Network name, e.g. "agage"
    Returns:
        None: Writes CSV files to the output directory
    """

    archive_suffix_str = "-csv"

    paths = Paths(network, errors="ignore_inputs")

    # Delete the csv archive if it exists and create an empty one
    delete_archive(network,
                   archive_suffix_string = archive_suffix_str)
    create_empty_archive(network, archive_suffix_string = archive_suffix_str)

    # Get the file list for the nc archive
    _, nc_sub_path, files = data_file_list(network,
                                        paths.output_path,
                                        errors="ignore_inputs")

    for f in files:
        filename_csv = f"{f.split('.nc')[0]}.csv"

        with open_data_file(f, network, sub_path = nc_sub_path) as ncf:
            with xr.open_dataset(ncf) as nc_ds:
                ds = nc_ds.load()
        
        # Convert to CSV
        header, df = nc_to_csv(ds)

        # Write to CSV file
        with open_data_file(filename_csv, network, 
                            sub_path = archive_suffix(paths.output_path, archive_suffix_str),
                            mode = "w") as csv_file:
            for line in header:
                csv_file.write(line + "\n")
            df.to_csv(csv_file, index=False, float_format='%.6f')

        pass


