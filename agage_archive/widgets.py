import xarray as xr
from IPython.display import clear_output, display
import ipywidgets as widgets
from glob import glob
from pathlib import Path

from agage_archive.config import Paths, data_file_list, open_data_file, is_jupyterlab_session
from agage_archive.visualise import plot_datasets, variables


# Global dictionary to store filenames associated with instrument/site string displayed in dropdown
instrument_site_filenames = {}

# Global list to store variable names that can be plotted
variable_names = list(variables.keys())


def file_search_species(network, file_type, species):
    """ Search for files containing species
    
    Args:
        network (str): Network
        file_type (str): File type ("high-frequency", "monthly-baseline" or "individual-instruments")
        species (str): Species to search for

    Returns:
        list: List of files containing species        
    """
    
    paths = Paths(network, errors="ignore_inputs")

    if file_type == "high-frequency":
        pattern = f"{species}/*.nc"
    elif file_type == "monthly-baseline":
        pattern = f"{species}/monthly-baseline/*.nc"
    elif file_type == "individual-instruments":
        pattern = f"{species}/individual-instruments/*.nc"
    else:
        raise ValueError("File type must be 'high-frequency', 'monthly-baseline' or 'individual-instruments'")

    files = data_file_list(network,
                           sub_path=f"{paths.output_path}",
                           pattern=pattern,
                           errors="ignore_inputs",
                           sub_directories=False)[2]

    return sorted(files)


def instruments_sites(files):
    """ Get networks and sites from files

    Args:
        files (list): List of files

    Returns:
        tuple: Tuple of lists containing networks and sites
    """

    instruments = []
    sites = []

    for file in files:
        filename = file.split('/')[-1]
        instruments.append(filename.split('_')[0])
        sites.append(filename.split('_')[1])

    return instruments, sites


def update_instrument_site(species,
                        file_type,
                        network,
                        instrument_site_dropdown):
    """ Update instrument and site dropdown

    Args:
        species (str): Species
        file_type (str): File type ("high-frequency", "monthly-baseline" or "individual-instruments")
        network (str): Network
        instrument_site_dropdown (ipywidgets.Dropdown): Dropdown widget
    """
    def find_duplicates(options):
        """ Find duplicates in options list
        """
        seen = set()
        duplicates = set()
        for option in options:
            if option in seen:
                duplicates.add(option)
            else:
                seen.add(option)
        
        error_str = "Duplicate options found in instrument and site combinations:"

        if duplicates:
            for duplicate in duplicates:
                error_str += f"\n{duplicate}"
            raise ValueError(error_str)

    global instrument_site_filenames

    # Clear contents of instrument_site_filenames
    instrument_site_filenames.clear()

    files = file_search_species(network, file_type, species)

    instruments, sites = instruments_sites(files)

    options = []
    options_files = []

    for f, i, s in zip(files, instruments, sites):
        options.append(f"{s}, {i}")
        options_files.append(f)

    # Check for duplicates and raise an error if found
    find_duplicates(options)
    
    # Populate dictionary with instrument/site strings as keys and filenames as values
    for option in sorted(options):
        instrument_site_filenames[option] = options_files[options.index(option)]

    # Update dropdown widget if passed as kwarg, otherwise return options list
    if instrument_site_dropdown:
        instrument_site_dropdown.options = instrument_site_filenames.keys()
    else:
        return instrument_site_filenames.keys()


def get_filenames(instrument_sites):
    """ Get filenames from species and network/site
    
    Args:
        instrument_sites (list): List of instrument/site strings

    Returns:
        list: List of filenames
    """

    global instrument_site_filenames

    return [instrument_site_filenames[instrument_site] for instrument_site in instrument_sites]


def load_datasets(network, filenames):
    """ Load datasets from filenames

    Args:
        network (str): Network
        filenames (list): List of filenames

    Returns:
        list: List of datasets
    """

    paths = Paths(network, errors="ignore_inputs")

    datasets = []
    for filename in filenames:
        with open_data_file(filename, network, paths.output_path, errors="ignore_inputs") as f:
            with xr.open_dataset(f) as ds:
                ds_species = ds.load()

        datasets.append(ds_species)

    return datasets


def plot_to_output(sender, network, species, instrument_site, variable,
                   output_widget, mode="lines"):
    """ Plot to output widget

    Args:
        sender (ipywidgets.Button): Button widget
        network (str): Network
        species (str): Species
        network_site (str): Network and site
        output_widget (ipywidgets.Output): Output widget
        mode (str) : how to plot the data. Defaults to "line", but can be set to "markers" if desired

    """

    if not instrument_site:
        with output_widget:
            clear_output(True)
            print("Please select a network and site") 

    filenames = get_filenames(instrument_site)

    datasets = load_datasets(network, filenames)

    renderer = is_jupyterlab_session()

    with output_widget:
        clear_output()
        print(f"Plotting {species} {variable} for {instrument_site}... please wait...")
        clear_output(True)
        fig = plot_datasets(datasets, variable=variable, mode=mode)
        fig.show(renderer=renderer)


def show_netcdf_info(sender, network, instrument_site, 
                    output_widget):
    """ Show NetCDF info to output widget

    Args:
        sender (ipywidgets.Button): Button widget
        species (str): Species
        network_site (str): Network and site
        output_widget (ipywidgets.Output): Output widget
    """

    if not instrument_site:
        with output_widget:
            clear_output(True)
            print("Please select a network and site") 

    filenames = get_filenames(instrument_site)
    datasets = load_datasets(network, filenames)

    with output_widget:
        clear_output()
        for filename, dataset in zip(filenames, datasets):
            print(filename)
            print(dataset.info())
            print("-----------------------------------------")
            print("")


def dashboard(network,
            file_types = ["high-frequency", "monthly-baseline", "individual-instruments"],
            mode="lines"):
    """ Create dashboard for visualising data

    Args:
        network (str): Network
        file_types (list): List of file types to visualise. Defaults to ["high-frequency", "monthly-baseline", "individual-instruments"]
        mode (str) : how to plot the data. Defaults to "line", but can be set to "markers" if desired
    """
    
    paths = Paths(network, errors="ignore_inputs")    
    
    # Get species names from the output directory structure
    species = []
    for f in data_file_list(network, paths.output_path, errors="ignore_inputs")[2]:
        if "/" in f:
            species.append(f.split("/")[0])
        else:
            continue

    if len(species) == 0:
        raise ValueError("No files found in the output directory")

    species = sorted(set(species))

    # Create dropdown widget
    species_dropdown = widgets.Dropdown(
        options=species,
        description='Species:',
        disabled=False,
        default=species[0]
        )

    # Create file_type widget
    file_type_dropdown = widgets.Dropdown(
        options=file_types,
        description='File type:',
        disabled=False,
        default=file_types[0]
    )

    # Selection widget for network and site
    instrument_site = widgets.SelectMultiple(
        options=update_instrument_site(species[0],
                                    file_types[0],
                                    network,
                                    None),
        description='Site, instrument:',
        disabled=False,
        indent=True,
        style={'description_width': 'initial'}
    )

    # Selection widget for variable
    variable_dropdown = widgets.Dropdown(
        options=variable_names,
        description='Variable:',
        disabled=False,
        default=variable_names[0]
    )

    # Plotting button
    plot_button = widgets.Button(description="Plot")

    # Output widget
    output = widgets.Output()
    output_netcdf = widgets.Output()

    # Widget to store output dataset
    output_dataset = widgets.Output()

    # Update network and site dropdown when species is changed
    species_dropdown.observe(lambda change:
                            update_instrument_site(change["new"],
                                                file_type_dropdown.value,
                                                network,
                                                instrument_site),
                            names="value")

    file_type_dropdown.observe(lambda change:
                            update_instrument_site(species_dropdown.value,
                                            change["new"],
                                            network,
                                            instrument_site),
                            names="value")

    # Plot to output when button is clicked
    plot_button.on_click(lambda x: plot_to_output(x, network,
                                                species_dropdown.value,
                                                instrument_site.value,
                                                variable_dropdown.value,
                                                output,
                                                mode=mode))
    plot_button.on_click(lambda x: show_netcdf_info(x, network,
                                                    instrument_site.value,
                                                    output_netcdf))

    display(species_dropdown)
    display(file_type_dropdown)
    display(instrument_site)
    display(variable_dropdown)
    display(plot_button)
    display(output)
    display(output_netcdf)

