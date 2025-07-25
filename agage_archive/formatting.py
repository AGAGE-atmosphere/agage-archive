import json
import numpy as np
import xarray as xr
from datetime import datetime
import warnings

from agage_archive import __version__ as code_version
from agage_archive.config import open_data_file, data_file_path
from agage_archive.util import is_number, lookup_username
from agage_archive.definitions import instrument_type_definition, nc4_types


def format_attributes_global_instrument(ds,
                                instrument,
                                return_attributes=False):
    '''Add global attributes for instrument

    Args:
        ds (xr.Dataset): Dataset
        instrument (str): Instrument
    '''

    attrs = ds.attrs.copy()

    if "instrument" not in ds.attrs.keys():
        attrs["instrument"] = instrument
    if "instrument_date" not in ds.attrs.keys():
        attrs["instrument_date"] = f"{ds.time[0].dt.strftime('%Y-%m-%d').values}"
    if "instrument_comment" not in ds.attrs.keys():
        attrs["instrument_comment"] = ""

    if return_attributes:
        return {key: attrs[key] for key in attrs if ("instrument" in key) and (key != "instrument_type")}
    else:
        ds_out = ds.copy(deep=True)
        ds_out.attrs = attrs.copy()
        return ds_out


def format_attributes_global_instruments(ds,
                                        instruments = [],
                                        return_attributes=False):
    '''Combine instrument details in global attributes

    Args:
        ds (xr.Dataset): Dataset
        instruments (list): List of instrument dictionaries containing the keys "instrument", "instrument_date" and "instrument_comment"
        
    Returns:
        xr.Dataset: Dataset with updated global attributes
    '''

    # If no instruments specified, check if they are in the dataset attributes
    if len(instruments) == 0:
        if "instrument" in ds.attrs:
            instruments = [{key: ds.attrs[key] for key in ds.attrs if ("instrument" in key) and \
                            (key != "instrument_type") and (key != "instrument_selection")}]
        else:
            raise ValueError("No instrument* attributes specified and none found in dataset attributes. " + \
                             "As a minimum, set keyword instrument = [{'instrument': 'INSTRUMENT_NAME'}]")

    # If only one instrument, make sure formatted properly and return
    elif len(instruments) == 1:
        return format_attributes_global_instrument(ds, instruments[0]["instrument"],
                                                return_attributes=return_attributes)

    attrs = {}

    # Remove instrument attributes so that we can repopulate them
    for attr in ds.attrs:
        if "instrument" not in attr:
            attrs[attr] = ds.attrs[attr]
        elif attr == "instrument_type" or attr == "instrument_selection":
            attrs[attr] = ds.attrs[attr]

    dates = []
    suffixes = []

    for instrument in instruments:
        suffix = ["",]

        has_instrument = False
        has_instrument_date = False

        for key, value in instrument.items():
            # For now, just get the date of the first instrument, as it makes sorting easier
            # There can be multiple, but it's probably not a big deal if they aren't in exactly the right order
            if key == "instrument_date":
                dates.append(value)
                has_instrument_date = True
            
            if is_number(key.split("instrument_")[-1]):
                # Store numerical suffix, prepending with underscore
                suffix.append("_" + key.split("instrument_")[-1])
                has_instrument = True

            if key == "instrument":
                has_instrument = True
                
        suffixes.append(suffix)

        if not has_instrument:
            raise ValueError("No instrument attribute found")
        if not has_instrument_date:
            raise ValueError("No instrument_date attribute found")
        
    #Sort index by date
    idx = np.argsort(dates)[::-1]
    # Apply this sort order to instruments dictionary
    instruments_sorted = [instruments[i] for i in idx]
    suffixes_sorted = [suffixes[i] for i in idx]

    instrument_count = 0

    # Loop through instruments in reverse
    for instrument, suffix in zip(instruments_sorted, suffixes_sorted):

        for n in suffix:

            # Relabel instrument number
            if instrument_count == 0:
                suffix_new = ""
            else:
                suffix_new = "_" + str(instrument_count)

            for attr in ["instrument", "instrument_date", "instrument_comment"]:
                if attr + n in instrument.keys():
                    attrs[attr + suffix_new] = instrument[attr + n]
                else:
                    print("WARNING: No " + attr + " found for instrument " + n + ". Setting to empty string")
                    attrs[attr + suffix_new] = ""
        
            instrument_count += 1

    if return_attributes:
        return {key: attrs[key] for key in attrs if ("instrument" in key) and (key != "instrument_type")}
    else:
        ds_out = ds.copy(deep=True)
        ds_out.attrs = attrs.copy()
        return ds_out


def format_dataset(ds,
                variable_translate={},
                instruments = [],
                species = None,
                units = None,
                calibration_scale = None):
    '''Format attributes, variables and encoding

    Args:
        ds (xr.Dataset): Dataset
    
    Returns:
        xr.Dataset: Dataset with updated global attributes
    '''

    ds = format_attributes(ds,
                        instruments = instruments,
                        species = species,
                        units = units,
                        calibration_scale = calibration_scale)
    
    ds = format_variables(ds,
                        variable_translate = variable_translate,
                        species = species,
                        units = units,
                        calibration_scale = calibration_scale)

    #TODO: Format comment string?

    return ds


def format_variables(ds,
                    variable_translate={},
                    species = None,
                    units = None,
                    calibration_scale = None,
                    attribute_override = {}):
    '''Standardise variable names and units

    Things to note:
        - The variable names are standardised according to the variables.json file
        - The units are standardised according to the unit_translator dictionary in definitions.py
        - The calibration scale is standardised according to the scale_translator dictionary in definitions.py
        - Variable attributes are taken from the variables.json file, and can be overridden using the attribute_override dictionary
        - The time comment attribute is carried over from the original dataset, to retain any information on resampling, etc.

    Args:
        ds (xr.Dataset): Dataset
        variable_translate (dict, optional): Dictionary of variable translations. Defaults to {}.
        species (str, optional): Species name. Defaults to None, in which case it is looked up in the dataset attributes.
        units (str, optional): Units. Defaults to None, in which case it is looked up in the dataset attributes.
        calibration_scale (str, optional): Calibration scale. Defaults to None, in which case it is looked up in the dataset attributes.
        attribute_override (dict, optional): Dictionary of attribute overrides. Defaults to {}.

    Returns:
        xr.Dataset: Dataset with standardised variable names and units

    Raises:
        ValueError: If variable not found in dataset

    '''

    network = ds.attrs["network"]

    with open_data_file("variables.json", this_repo=True) as f:
        variables = json.load(f)
    
    with open_data_file("standard_names.json", this_repo=True) as f:
        standard_names=json.load(f)

    # Some attributes that we want to keep without changing
    attrs = ds.attrs.copy()
    if "comment" in ds.time.attrs:
        time_comment = ds.time.attrs["comment"]
    else:
        time_comment = variables["time"]["attrs"]["comment"]

    # Units shouldn't be in attrs (CF convention), so try to find in dataset
    if units is None:
        if "units" in ds.mf.attrs:
            units = ds.mf.attrs["units"]
        else:
            raise ValueError("No units specified and none found in dataset attributes. Specify in function call.")
        
    vars_out = {}

    # Loop through standard variable names
    variables_standardise = [v for v in variables if v != "time"]

    for var in variables_standardise:

        # Do we need to translate any variable names?
        if var in variable_translate:
            var_ds = variable_translate[var]
        else:
            var_ds = var

        # Convert type to that contained in variables.json
        typ = np.__getattribute__(nc4_types[variables[var]["encoding"]["dtype"]])

        # Try to convert variable to new type. If there are missing values, choose an appropriate missing value for the type
        # If the variable is a float, we still want to use NaNs
        if nc4_types[variables[var]["encoding"]["dtype"]][0] != "f":
            if "_FillValue" in variables[var]["encoding"]:
                missing_value = variables[var]["encoding"]["_FillValue"]
            else:
                missing_value = np.nan
        else:
            missing_value = np.nan
        
        # This if statement is here to handle optional variables that are not in the dataset
        if var_ds in ds.variables:
            var_temp = ds[var_ds].values.copy()
        else:
            # If the variable is not in the dataset, check if it is optional
            # If it is optional, set it to the default value
            # If it is not optional, raise an error
            if variables[var]["optional"] == "False":
                if "default" in variables[var]:
                    # If the variable is optional and has a default value, use that
                    # Setting to a float to avoid issues with NaNs, type conversion happens later
                    var_temp = np.full(ds.time.shape, float(variables[var]["default"]))
                else:
                    raise ValueError(f"Variable {var_ds} not found in dataset and has no default value. " + \
                                    "Use variable_translate to map to a different variable name, " + \
                                    "or set a default value in variables.json.")
            else:
                # This variable is optional and not present, so we can skip it
                continue

        # Error on warnings, in case casting to type causes problems
        warnings.simplefilter("error")

        var_temp[np.isnan(var_temp)] = missing_value
        vars_out[var] = ("time", var_temp.copy().astype(typ))

        warnings.simplefilter("default")

    # Create new dataset
    ds = xr.Dataset(vars_out,
                    coords = {"time": ds.time.copy()}, 
                    attrs=attrs)

    # Add variable attributes and encoding
    for var in ds.variables:
        ds[var].attrs = variables[var]["attrs"]
        ds[var].encoding = variables[var]["encoding"]

        # for mole fractions, replace % with species name in long_name
        if "mf" in var and var != "mf_count":
            ds[var].attrs["long_name"] = ds[var].attrs["long_name"].replace("%",
                                                        lookup_locals_and_attrs("species", locals(), attrs))
        
            # standard names need to found from the standard_names.json for CF compliance. 
            # if not in there (e.g. trichloroethylene the only example currently), no standard_name attribute assigned.
            if "standard_name" in ds[var].attrs:
                if lookup_locals_and_attrs("species", locals(), attrs) in standard_names.keys():
                    ds[var].attrs["standard_name"]=ds[var].attrs["standard_name"].replace("%", 
                                                                        standard_names[lookup_locals_and_attrs("species",
                                                                                                                locals(),
                                                                                                                attrs)])

            ds[var].attrs["units"] = lookup_locals_and_attrs("units", locals(), attrs)
            if "calibration_scale" in ds[var].attrs:
                ds[var].attrs["calibration_scale"] = lookup_locals_and_attrs("calibration_scale", locals(), attrs)

    # Copy time comment attribute
    ds.time.attrs["comment"] = time_comment

    # If instrument_type variable is in file, make sure comment is formatted properly
    if "instrument_type" in ds.variables:
        instrument_number, instrument_number_string = instrument_type_definition(network)
        ds.instrument_type.attrs["comment"] = instrument_number_string

    # If there are any attribute overrides, apply them
    for var in attribute_override:
        if var in ds.variables:
            for attr in attribute_override[var]:
                ds[var].attrs[attr] = attribute_override[var][attr]
        else:
            raise ValueError(f"Variable {var} not found in dataset. Can't override attributes.")

    return ds


def format_attributes(ds, instruments = [],
                    network = None,
                    species = None,
                    calibration_scale = None,
                    site = True,
                    extra_attributes = {}):
    '''Format attributes

    Note that many of the above arguments don't appear to be used,
    but they can be accessed in the lookup_locals_and_attrs function

    Args:
        ds (xr.Dataset): Dataset
        instruments (list, optional): List of instrument dictionaries containing the keys "instrument", "instrument_date" and "instrument_comment"
        network (str, optional): Network name. Defaults to None, in which case it is looked up in the dataset attributes.
        species (str, optional): Species name. Defaults to None, in which case it is looked up in the dataset attributes.
        units (str, optional): Units. Defaults to None, in which case it is looked up in the dataset attributes.
        calibration_scale (str, optional): Calibration scale. Defaults to None, in which case it is looked up in the dataset attributes.
        site (bool, optional): Look for site-specific attributes.
        extra_attributes (dict, optional): Extra attributes to add to the dataset. Defaults to {}. Keys must be present in one of the attributes.json files.

    Returns:
        xr.Dataset: Dataset with formatted attributes
    '''

    with open_data_file("attributes.json", this_repo=True) as f:
        attributes_default = json.load(f)

    if network is None:
        if "network" in ds.attrs:
            network_attrs = ds.attrs["network"]
        else:
            raise ValueError("No network specified and none found in dataset attributes. Specify in function call.")
    else:
        ds.attrs["network"] = network
        network_attrs = network

    with open_data_file("attributes.json", network=network_attrs) as f:
        attributes_network = json.load(f)

    # Combine default and network attributes
    # Allow default attributes to be overwritten by network attributes
    for attr in attributes_network:
        attributes_default[attr] = attributes_network[attr]
    
    # Add site attributes, if available
    if site:
        if data_file_path("attributes_site.json", network=network_attrs).exists():
            with open_data_file("attributes_site.json", network=network_attrs) as f:
                attributes_sites = json.load(f)
            if ds.attrs["site_code"] in attributes_sites:
                # Combine default and site attributes
                # Allow default attributes to be overwritten by site attributes
                attributes_site = attributes_sites[ds.attrs["site_code"]]
                for attr in attributes_site:
                    attributes_default[attr] = attributes_site[attr]

                if "instrument" in attributes_site:
                    instruments = [{"instrument": attributes_site["instrument"],
                                    "instrument_date": attributes_site["instrument_date"],
                                    "instrument_comment": attributes_site["instrument_comment"]}]

    attrs = {}

    for attr in attributes_default:

        if "instrument" in attr and attr != "instrument_type" and attr != "instrument_selection":
            # Update instrument attributes
            attrs.update(format_attributes_global_instruments(ds,
                                                        instruments,
                                                        return_attributes=True))

        elif attr == "file_created_by":
            # Get username
            attrs[attr] = lookup_username()

        elif attr == "file_created":
            # Get current time
            attrs[attr] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        elif attr == "start_date":
            # Get start date
            attrs[attr] = f"{ds.time[0].dt.strftime('%Y-%m-%d %H:%M:%S').values}"
        
        elif attr == "end_date":
            # Get end date
            attrs[attr] = f"{ds.time[-1].dt.strftime('%Y-%m-%d %H:%M:%S').values}"
        
        elif attr == "processing_code_version":
            # Get code version
            attrs[attr] = code_version

        else:
            # Set default
            attrs[attr] = attributes_default[attr]

            # If attribute is in dataset, and not empty, overwrite
            if attr in ds.attrs:
                if ds.attrs[attr] != "":
                    if isinstance(ds.attrs[attr], bytes):
                        att = ds.attrs[attr].decode("utf-8")
                    else:
                        att = ds.attrs[attr]
                    
                    #There are some attributes that aren't encoded properly
                    # e.g., the Å in Ny-Ålesund. This seems to fix it
                    if isinstance(att, str):
                        attrs[attr] = att.encode("utf-8", "surrogateescape").decode("UTF-8")
                    else:
                        attrs[attr] = att
            
            # If attribute is in extra_attributes, overwrite
            if attr in extra_attributes:
                attrs[attr] = extra_attributes[attr]

    # Format certain key attributes, and determine if they have been set as keywords
    for v in ["species", "calibration_scale", "network"]:
        attrs[v] = lookup_locals_and_attrs(v, locals(), ds.attrs.copy())

    # Append to a site-specific instrument-specific attribute, if present
    if data_file_path("attributes_site_species_instrument.json", network=network_attrs).exists():
        with open_data_file("attributes_site_species_instrument.json", network=network_attrs) as f:
            attributes_site_species_instrument = json.load(f)
        if attrs["site_code"] in attributes_site_species_instrument:
            if attrs["species"] in attributes_site_species_instrument[attrs["site_code"]]:
                if attrs["instrument"] in attributes_site_species_instrument[attrs["site_code"]][attrs["species"]]:
                    attributes_site_species_instrument = attributes_site_species_instrument[attrs["site_code"]][attrs["species"]][attrs["instrument"]]
                    for attr in attributes_site_species_instrument:
                        attrs[attr] += attributes_site_species_instrument[attr]

    ds_out = ds.copy(deep=True)
    ds_out.attrs = attrs.copy()

    return ds_out


def format_species(species):
    '''Format species name

    Args:
        species (str): Species name

    Returns:
        str: Formatted species name
    '''

    from agage_archive.definitions import species_translator

    if species.lower() in species_translator:
        return species_translator[species.lower()]
    else:
        return species.lower()


def format_species_flask(species):
    """Species name translation for GCWerks flask data

    Args:
        species (str): species string
    """

    from agage_archive.definitions import species_translator_flask

    if format_species(species) in species_translator_flask:
        return species_translator_flask[format_species(species)]
    else:
        return format_species(species).upper()
        
     
def format_units(units):
    '''Format units

    Args:
        units (str): Units

    Returns:
        str: Formatted units
    '''

    from agage_archive.definitions import unit_translator

    if units in unit_translator:
        return unit_translator[units]
    else:
        return units


def format_calibration_scale(scale):
    '''Format scale

    Args:
        scale (str): Scale

    Returns:
        str: Formatted scale
    '''

    from agage_archive.definitions import scale_translator

    if scale in scale_translator:
        return scale_translator[scale]
    else:
        return scale


def format_network(network):
    """Format network name

    Args:
        network (str): Network name

    Returns:
        str: Formatted network name
    """

    return network.lower()


def lookup_locals_and_attrs(v, local, attrs):
    '''Look up variable in locals and attrs, and format the output

    Args:
        v (str): Variable name
        local (dict): Dictionary of local variables
        attrs (dict): Dictionary of attributes

    Returns:
        str: Variable name
    '''

    #TODO: this assumes that the variable is in locals
    if local[v] is None:
        # If not set as keyword, check if it is in dataset attributes
        if v not in attrs:
            raise ValueError(f"{v} not set and not found in dataset attributes")
        else:
            # If in dataset attributes, format it
            return eval(f"format_{v}('{attrs[v]}')")
    else:
        # If set, format it
        return eval(f"format_{v}('{local[v]}')")


def comment_append(existing_comment, comment):
    '''Append comment to existing comment

    Args:
        existing_comment (str): Existing comment
        comment (str): Comment to append

    Returns:
        str: Comment
    '''

    if existing_comment == "":
        return comment
    else:
        if existing_comment[-1] == ".":
            return existing_comment + " " + comment
        else:
            return existing_comment + ". " + comment