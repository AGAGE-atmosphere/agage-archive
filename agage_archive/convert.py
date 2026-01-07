import pandas as pd
import numpy as np
import xarray as xr
import json
from openghg_calscales import convert as convert_scale

from agage_archive.config import open_data_file
from agage_archive.data_selection import calibration_scale_default
from agage_archive.formatting import format_species, format_variables, comment_append


def std_ddof0(x):
    """Standard deviation with ddof=0

    Args:
        x (ndarray): Array

    Returns:
        float: Standard deviation
    """

    return np.std(x, ddof=0)


def resample_variability(df_in, grouping_args,
                         grouping_method="resample",
                         grouping_kwargs={"closed": "left", "label": "left"}):
    """Resample the variability of the dataset
    If no variability is present, the standard deviation is calculated.
    If variability is present, a weighted standard deviation is calculated,
    based on VarT = (1/N) sum[nj(Varj + SqrMeanj)]  - SqrMeanT
    
    Args:
        df_in (pd.DataFrame): DataFrame
        grouping_method (str, optional): Grouping method to use: either groupby or resample. Defaults to "resample".
        grouping_args (str, optional): Arguments for the grouping method. Defaults to "resample_period".
        grouping_kwargs (dict, optional): Keyword arguments for the grouping method. Defaults to {"closed": "left", "label": "left"}.

    Returns:
        pd.Series: Resampled variability
    """

    if not isinstance(df_in, pd.DataFrame):
        raise ValueError("Input must be a pandas DataFrame")

    if grouping_method not in ["resample", "groupby"]:
        raise ValueError("Grouping method must be either 'resample' or 'groupby'")

    if "mf_variability" not in df_in.columns:
        # If the variability isn't in the dataset, we can just resample the data and calculate the std

        if grouping_method == "resample":
            df = df_in[["mf"]].copy()
            df_grouped = df.resample(*grouping_args,
                                    **grouping_kwargs).agg(std_ddof0)
        elif grouping_method == "groupby":
            df = df_in[["mf", *grouping_args]].copy()
            df = df.reset_index()
            df_grouped = df.groupby(*grouping_args, 
                                    **grouping_kwargs).agg({"mf": std_ddof0, "time": "first"})
            df_grouped.set_index("time", inplace=True)
        
        return df_grouped["mf"]

    else:

        # If the variability is already in the dataset, we need to do a weighted resample        
        if "mf_count" not in df_in.columns:
            raise ValueError("mf_count must be in the DataFrame")

        vars_list = ["mf", "mf_count", "mf_variability"]
        if grouping_method == "groupby":
            vars_list.append(*grouping_args)
        df = df_in[vars_list].copy()

        # Some variables to resample for the calculation
        df["mfN"] = df["mf"] * df["mf_count"]
        df["sumOfSquares"] = df["mf_count"] * (df["mf_variability"]**2 + df["mf"]**2)
        df.drop(columns=["mf", "mf_variability"], inplace=True)

        # Resample or group
        agg_dict = {"mfN": "sum",
                    "mf_count": "sum",
                    "sumOfSquares": "sum"}
        
        if grouping_method == "resample":
            df_grouped = df.resample(*grouping_args,
                                    **grouping_kwargs).agg(agg_dict)
        elif grouping_method == "groupby":
            df = df.reset_index()
            if "time" not in agg_dict.keys():
                agg_dict["time"] = "first"
            df_grouped = df.groupby(*grouping_args, 
                                    **grouping_kwargs).agg(agg_dict)
            df_grouped.set_index("time", inplace=True)

        weighted_resample_mf = df_grouped["mfN"] / (df_grouped["mf_count"])
        variance = df_grouped["sumOfSquares"] / df_grouped["mf_count"] - weighted_resample_mf**2
        variance[variance < 0] = 0
        df_grouped["mf_variability"] = np.sqrt(variance)

        return df_grouped["mf_variability"]


def define_agg_dict(variable_defaults, resample_period, columns,
                    no_time = False):
    """Define the aggregation dictionary for the resample method

    Args:
        variable_defaults (dict): Variable defaults
        resample_period (str): Period to resample to
        columns (list): List of columns in the DataFrame
        no_time (bool, optional): If True, don't include time in the aggregation dictionary. Defaults to False.

    Returns:
        dict: Aggregation dictionary
    """

    def get_seconds(resample_period):
        """Get the number of seconds in a resample period
        """
        offset = pd.tseries.frequencies.to_offset(resample_period)
        # If the offset has a fixed timedelta, use it directly
        try:
            return pd.Timedelta(offset).total_seconds()
        except:
            # Otherwise, estimate by using a reference time difference (choose a 30-day month)
            ref = pd.Timestamp("2021-06-01")
            return (ref + offset - ref).total_seconds()

    agg_dict = {}

    for var in columns:
        if var =="time":
            continue
        if variable_defaults[var]["resample_method"] == "mean":
            agg_dict[var] = "mean"
        elif variable_defaults[var]["resample_method"] == "median":
            agg_dict[var] = "median"
        elif variable_defaults[var]["resample_method"] == "sum":
            agg_dict[var] = "sum"
        elif variable_defaults[var]["resample_method"] == "standard_error":
            agg_dict[var] = lambda x: np.mean(x) / np.sqrt(x.count())
        elif variable_defaults[var]["resample_method"] == "mode":
            agg_dict[var] = lambda x: x.mode().iloc[0] if not x.mode().empty else np.nan
        elif variable_defaults[var]["resample_method"] == "first":
            agg_dict[var] = "first"
        else:
            if var == "baseline":
                agg_dict[var] = "prod"
            elif var == "sampling_period":
                agg_dict[var] = lambda x: get_seconds(resample_period)
            elif var == "mf_variability":
                # Careful! This requires that mf_variability is overwritten by mf before resampling
                agg_dict[var] = std_ddof0
            else:
                raise ValueError(f"Resample method not defined for {var}")

    if not no_time:
        agg_dict["time"] = "first"

    return agg_dict


def resampler(df, variable_defaults, last_timestamp, resample_period="3600s"):
    """Resample the dataset to a regular time interval

    Args:
        df (pd.DataFrame): DataFrame created from xarray dataset
        variable_defaults (dict): Variable defaults
        last_timestamp (pd.Timestamp): Last timestamp in the slice. Used to calculate the final sampling period.
        resample_period (str, optional): Period to resample to. Defaults to "3600s". 
            Pandas alias for time period, e.g. "1H" for hourly, "1D" for daily.

    Returns:
        pd.DataFrame: Resampled DataFrame
    """

    variables = variable_defaults.copy()
    variables["inlet_height_change"] = {"resample_method": "first"}
    variables["time_difference"] = {"resample_method": "first"}

    df = df.copy()

    # If mf_count isn't there, add it with ones
    if "mf_count" not in df.columns:
        df["mf_count"] = 1

    # First resample the variability. We'll add it back in later
    df_variability = resample_variability(df, [resample_period])

    # Resample the data
    agg_dict = define_agg_dict(variables, resample_period, df.columns,
                            no_time = True)
    df_resample = df.resample(resample_period,
                            closed="left", label="left").agg(agg_dict)

    # Add the variability back in
    df_resample["mf_variability"] = df_variability

    # If there are any Nans in the instrument_type column, replace with -1 (UNDEFINED)
    if "instrument_type" in df.columns:
        if df_resample["instrument_type"].isnull().any():
            df_resample["instrument_type"] = df_resample["instrument_type"].fillna(-1)

    # Change the final period to the number of seconds between the last time point and the end of the slice
    df_resample.at[df_resample.index[-1], "sampling_period"] = int((last_timestamp - df_resample.index[-1]).total_seconds())

    # Overwrite the rest of the sampling periods with the difference between the time points
    # This is because the resample method doesn't calculate the sampling period correctly for months
    time_diffs = df_resample.index.to_series().diff().dt.total_seconds()
    df_resample.loc[df_resample.index[:-1], "sampling_period"] = time_diffs.iloc[1:].values.astype(int)
    
    return df_resample


def grouper(df, inlet_height_change_indices,
            inlet_height_change_times,
            inlet_height_change_times_delta,
            variable_defaults, resample_period="3600s"):
    """Group the dataset by inlet height or resample to a regular time interval, 
    if the inlet height changes more quickly than the resample period

    Args:
        df (pd.DataFrame): DataFrame created from xarray dataset
        variable_defaults (dict): Variable defaults
        resample_period (str, optional): Period to resample to. Defaults to "3600s". 
            Pandas alias for time period, e.g. "1H" for hourly, "1D" for daily.

    Returns:
        pd.DataFrame: Grouped/resampled DataFrame
    """

    # Store dtypes for later
    dtypes = df.dtypes
    if not "mf_variability" in dtypes:
        dtypes["mf_variability"] = "float32"
    if not "mf_count" in dtypes:
        dtypes["mf_count"] = "int32"
    if "inlet_height_change" in dtypes:
        dtypes = dtypes.drop(["inlet_height_change"])
    if "time_difference" in dtypes:
        dtypes = dtypes.drop(["time_difference"])

    variables = variable_defaults.copy()
    variables["inlet_height_change"] = {"resample_method": "first"}
    variables["time_difference"] = {"resample_method": "first"}

    # Create a unique label for each of these time periods
    # This will be used to group the data
    inlet_height_change = np.repeat(np.arange(len(inlet_height_change_indices)-1), 
                                np.diff(inlet_height_change_indices))
    df["inlet_height_change"] = inlet_height_change

    # Create a column to store the time difference between each time period
    time_difference_values = np.repeat(inlet_height_change_times_delta, np.diff(inlet_height_change_indices))
    df["time_difference"] = time_difference_values

    # Create the dataframes to either resample or group
    df_to_resample = df[df["time_difference"] > pd.Timedelta(resample_period)].copy()
    df_to_group = df[df["time_difference"] <= pd.Timedelta(resample_period)].copy()

    dfs = []

    # Grouping
    # =========

    if not df_to_group.empty:

        # If mf_count isn't there, add it with ones
        if "mf_count" not in df_to_group.columns:
            df_to_group["mf_count"] = 1

        # First resample the variability. We'll add it back in later
        df_variability = resample_variability(df_to_group, ["inlet_height_change"],
                                              grouping_method="groupby",
                                              grouping_kwargs={})

        # Move time index to column
        df_to_group.reset_index(inplace=True)

        # Group the data
        agg_dict = define_agg_dict(variables, resample_period, df_to_group.columns)
        df_grouped = df_to_group.groupby("inlet_height_change").agg(agg_dict, axis=0)

        # Remove inlet_height_change and time_difference columns
        df_grouped.set_index("time", inplace=True)
        # Replace sampling period with time difference in seconds
        df_grouped["sampling_period"] = df_grouped["time_difference"].dt.total_seconds()

        # Add the variability back in
        df_grouped["mf_variability"] = df_variability

        # Remove unneeded columns
        df_grouped = df_grouped.drop(columns=["inlet_height_change", "time_difference"])

        dfs.append(df_grouped)

    # Resampling
    # ==========

    if not df_to_resample.empty:

        # Resample the data that needs resampling
        # But first, we need to group by inlet height change
        df_to_resample_grouped = df_to_resample.groupby("inlet_height_change")

        # Loop through the groups and resample
        for name, group in df_to_resample_grouped:
            df_avg = resampler(group, variable_defaults,
                               group.index[0] + pd.Timedelta(group["time_difference"].iloc[0], unit="s"),
                               resample_period=resample_period)
            if not df_avg.empty:
                dfs.append(df_avg.drop(columns=["inlet_height_change", "time_difference"]))

    # Ensure all dtypes are the same and that there are no NaNs in columns that are going to be converted to int
    for i in range(len(dfs)):
        # If there are any nans in int columns, replace with -999
        for col in dfs[i].select_dtypes(include=["int8", "int16", "int32", "int64"]).columns:
            if dfs[i][col].isnull().any():
                dfs[i][col] = dfs[i][col].fillna(-999)

        # If there are any nans in columns that are going to be converted to int, replace with -999
        for col in dtypes.index:
            if "int" in str(dtypes[col]):
                if dfs[i][col].isnull().any():
                    dfs[i][col] = dfs[i][col].fillna(-999)

        # Convert dtypes
        dfs[i] = dfs[i].astype(dtypes)

    # Concatenate and sort
    df_avg = pd.concat(dfs, axis=0).sort_index()

    # If there are any Nans in the instrument_type column, replace with -1 (UNDEFINED)
    if "instrument_type" in df.columns:
        if df_avg["instrument_type"].isnull().any():
            df_avg["instrument_type"] = df_avg["instrument_type"].fillna(-1)

    return df_avg


def find_inlet_height_changes(ds):
    """Find changes in inlet height

    Args:
        ds (xarray.Dataset): Dataset

    Returns:
        np.ndarray, pd.DatetimeIndex, pd.TimedeltaIndex: Indices of changes in inlet height,
            times of changes in inlet height, time deltas between changes in inlet height
    """

    inlet_height_change_times = [ds.time.values[0]]
    inlet_height_change_times_delta = []

    # Identify changes in inlet_height
    inlet_height_changes = np.where(np.diff(ds.inlet_height.values) != 0)[0] + 1

    # Extract change times
    inlet_height_change_times = ds.time.values[inlet_height_changes]

    # Include the first time value
    inlet_height_change_times = np.insert(inlet_height_change_times, 0, ds.time.values[0])
    inlet_height_changes = np.insert(inlet_height_changes, 0, 0)

    # Include the last time value
    last_time_value = ds.time.values[-1] + np.timedelta64(int(ds.sampling_period.values[-1]), 's')
    inlet_height_change_times = np.append(inlet_height_change_times,
                                        last_time_value)
    inlet_height_changes = np.append(inlet_height_changes, len(ds.time))

    # Calculate time deltas
    inlet_height_change_times_delta = np.diff(inlet_height_change_times)

    # Convert to pandas datetime and timedelta
    inlet_height_change_times = pd.to_datetime(inlet_height_change_times)
    inlet_height_change_times_delta = pd.to_timedelta(inlet_height_change_times_delta)

    return inlet_height_changes, inlet_height_change_times, inlet_height_change_times_delta


def resample(ds,
            resample_period = "3600s",
            resample_threshold = "600s",
            ignore_inlet = False
            ):
    """Resample the dataset to a regular time interval
    
    Args:
        ds (xarray.Dataset): Dataset
        resample_period (str, optional): Period to resample to. Defaults to "3600s". 
            Pandas alias for time period, e.g. "1H" for hourly, "1D" for daily.
        resample_threshold (str, optional): Threshold for resampling, in seconds. Defaults to 600s.
            If the median time difference is greater than this threshold, the dataset is not resampled.
            So, if the threshold is 600s and the resample_period is 3600s and 1-minute data is provided, 
            the dataset is resampled to houry. If 20-minute data is provided, the dataset is not resampled.
            Pandas alias for time period, e.g. "600s" for 10 minutes.
        ignore_inlet (bool, optional): If True, ignore changes in inlet height. Defaults to False.

    Returns:
        xarray.Dataset: Resampled dataset
    """

    # Read variables.json
    with open_data_file("variables.json", this_repo=True) as f:
        variable_defaults = json.load(f)

    # Read variable defaults for non-public data to find how to resample these variables
    with open_data_file("variables_not_public.json", this_repo=True) as f:
        variable_np_defaults = json.load(f)
    variable_defaults.update(variable_np_defaults)
    
    # check if median time difference is less than minimum_averaging_period
    if pd.to_timedelta(ds.time.diff("time").median().values) < \
            pd.to_timedelta(resample_threshold):
        
        # Remove duplicate time points, which seem to be there when inlets switch sometimes
        ds = ds.drop_duplicates("time", keep = "first")

        # Create new dataset to store resampled data
        ds_out = ds.isel(time=0)

        # If not considering inlet changes, resample
        if ignore_inlet:
                print("... resampling")
                df = resampler(ds.to_dataframe(),
                               variable_defaults,
                               ds.time.values[-1] + pd.Timedelta(ds.sampling_period.values[-1], unit="s"),
                               resample_period=resample_period)
                comment_str = f"Resampled to {resample_period}."

        else:

            # Find changes in inlet height
            inlet_height_changes, inlet_height_change_times, inlet_height_change_times_delta = find_inlet_height_changes(ds)

            resample_or_group = ""
            
            # There are no changes in inlet height
            if inlet_height_change_times_delta.median() == pd.to_timedelta(0):
                resample_or_group = "resample"
            else:
                # If the median time difference is less than the resample period, group
                if inlet_height_change_times_delta.median() < pd.to_timedelta(resample_period):
                    resample_or_group = "group"
                else:
                    resample_or_group = "resample"

            if resample_or_group == "resample":
                print("... resampling")
                df = resampler(ds.to_dataframe(), variable_defaults, inlet_height_change_times[-1], resample_period=resample_period)
                comment_str = f"Resampled to {resample_period}."

            else:
                print("... grouping inlets and averaging/resampling")
                df = grouper(ds.to_dataframe(),
                            inlet_height_changes,
                            inlet_height_change_times,
                            inlet_height_change_times_delta,
                            variable_defaults, resample_period=resample_period)
                comment_str = f"Grouped by inlet height and/or resampled to {resample_period}."

        # Expand dimensions to include time
        # This step removes time attributes, so need to store and replace
        time_attrs = ds.time.attrs
        ds_out = ds_out.expand_dims(time=df.index)
        ds_out.time.attrs = time_attrs

        variables = list(ds.variables)
        variables.remove("time")

        for var in variables:
            ds_out[var] = xr.DataArray(df[var].values, dims=["time"],
                                    coords={"time": ds_out.time})
            ds_out[var].attrs = ds[var].attrs.copy()
        
        ds_out.time.attrs = ds.time.attrs.copy()

        ds_out["mf_variability"] = xr.DataArray(df["mf_variability"].values, dims=["time"],
                                            coords={"time": ds_out.time})
        ds_out["mf_variability"].attrs = variable_defaults["mf_variability"]["attrs"].copy()
        ds_out["mf_variability"].attrs["units"] = ds["mf"].attrs["units"]
        # Copy either "calibration_scale" or "scale" attribute from "mf" variable
        if "calibration_scale" in ds["mf"].attrs:
            ds_out["mf_variability"].attrs["calibration_scale"] = ds["mf"].attrs["calibration_scale"]
        elif "scale" in ds["mf"].attrs:
            ds_out["mf_variability"].attrs["calibration_scale"] = ds["mf"].attrs["scale"]
        else:
            raise ValueError("No calibration scale found for mole fraction variable.")

        ds_out["mf_count"] = xr.DataArray(df["mf_count"], dims=["time"],
                                coords={"time": ds_out.time})
        ds_out["mf_count"].attrs = variable_defaults["mf_count"]["attrs"].copy()
        ds_out["mf_count"].attrs["units"] = ""

        if "comment" not in ds.attrs:
            ds_out.attrs["comment"] = comment_str
        else:
            ds_out.attrs["comment"] = comment_append(ds_out.attrs["comment"], comment_str)
        if "comment" not in ds.time.attrs:
            ds_out.time.attrs["comment"] = comment_str
        else:
            ds_out.time.attrs["comment"] = comment_append(ds_out.time.attrs["comment"], comment_str)

        return ds_out

    else:

        return ds


def monthly_baseline(ds, ds_baseline):
    '''Calculate monthly baseline mole fractions

    Args:
        ds (xr.Dataset): Dataset
        ds_baseline (xr.Dataset): Baseline dataset

    Returns:
        xr.Dataset: Dataset with monthly baseline mole fractions
    '''

    # Select baseline points
    ds_baseline_points = ds.where(ds_baseline.baseline == 1, drop=True)

    # Remove any baseline points where the mole fraction is NaN
    ds_baseline_points = ds_baseline_points.where(~np.isnan(ds_baseline_points.mf), drop=True)

    # Calculate monthly mean
    # If there are no baseline points, return an empty dataset with same attributes
    if len(ds_baseline_points.time) == 0:
        ds_monthly = ds.isel(time=[])
        ds_monthly["mf_variability"] = ds.mf.isel(time=[])
        ds_monthly["mf_variability"].attrs["units"] = ds.mf.attrs["units"]
        ds_monthly["mf_repeatability"] = ds.mf.isel(time=[])
        ds_monthly["mf_repeatability"].attrs["units"] = ds.mf.attrs["units"]
    else:
        ds_monthly = resample(ds_baseline_points,
                              resample_period="1MS",
                              resample_threshold="1000D",
                              ignore_inlet=True) # Big number so that everything gets resampled

    # Relabel some attributes
    ds_monthly["mf_variability"].attrs["long_name"] = "Monthly standard deviation of baseline mole fractions"
    ds_monthly["mf_repeatability"].attrs["long_name"] = "Monthly standard error in mean of baseline mole fractions"
    
    # Copy global attributes
    ds_monthly.attrs = ds.attrs.copy()

    # Add baseline flag
    ds_monthly.attrs["baseline_flag"] = ds_baseline.attrs["baseline_flag"]

    # Change frequency
    ds_monthly.attrs["frequency"] = "monthly baseline"

    # Run variable formatting
    ds_monthly = format_variables(ds_monthly,
                                  attribute_override={"mf_variability": ds_monthly["mf_variability"].attrs,
                                                      "mf_repeatability": ds_monthly["mf_repeatability"].attrs})

    return ds_monthly


def scale_convert(ds, scale_new_in):
    """Convert mole fraction from one scale to another

    Args:
        ds (xarray.Dataset): Dataset containing mole fractions
        scale_new_in (str): New scale to convert to. If None, no conversion is applied.
            If "defaults", the default scale for the species is used from the scale_defaults.csv file
            If "defaults_individual", the default scale for the species is used from the scale_defaults_individual.csv file. 
            You can set any other default file by setting to the path to something like "defaults_<suffix>", 
            which will look for a file called scale_defaults_<suffix>.csv
        
    Returns:
        ndarray,float: Mole fraction in new scale
    """

    # If no conversion required, return original dataset
    if scale_new_in == None:
        return ds

    # Find species
    species = ds.attrs["species"]

    # Get default scale, if needed
    if "default" in scale_new_in:
        scale_new = calibration_scale_default(ds.attrs["network"],
                                              format_species(species),
                                              scale_defaults_file=scale_new_in)
    else:
        scale_new = scale_new_in

    # If scales are the same, return original dataset
    if ds.attrs["calibration_scale"] == scale_new:
        return ds
    
    scale_original = ds.attrs["calibration_scale"]
    
    # Make a deep copy of the dataset
    ds_out = ds.copy(deep=True)

    # Apply conversion factor
    ds_out.mf.values = convert_scale(ds.mf, species, scale_original, scale_new).values

    # Update attributes
    ds_out.attrs["calibration_scale"] = scale_new
    for var in ds_out.variables:
        if "calibration_scale" in ds_out[var].attrs:
            ds_out[var].attrs["calibration_scale"] = scale_new
        if "scale" in ds_out[var].attrs:
            ds_out[var].attrs["scale"] = scale_new

    return ds_out
