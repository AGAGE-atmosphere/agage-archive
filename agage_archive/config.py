from pathlib import Path as _Path
import tarfile
from zipfile import ZipFile
import yaml
from fnmatch import fnmatch, filter
import psutil
from shutil import copy, rmtree
import os


class Paths():

    def __init__(self,
                network = "",
                this_repo = False,
                errors = "ignore",
                site = ""):
        """Class to store paths to data folders
        
        Args:
            network (str, optional): Network name. Defaults to "".
            this_repo (bool, optional): If True, look for the root and data folder within this repository (no config).
                If False, will look for the root and data folders and config file in the working directory.
                Defaults to False.
            errors (str, optional): If "raise", raise FileNotFoundError if file not found. 
                If "ignore", return path.
                If "ignore_inputs", ignore errors in input paths. 
                Defaults to "raise".

        Raises:
            FileNotFoundError: If config file doesn't exist
            FileNotFoundError: If folder or zip archive doesn't exist
            FileNotFoundError: If folder or zip archive is not a folder or zip archive   
        """

        # Get repository root
        # Do this by finding the location of the .git folder in the working directory
        # and then going up one level
        if not this_repo:
            working_directory = _Path.cwd()
            while True:
                if (working_directory / ".git").exists():
                    break
                else:
                    working_directory = working_directory.parent
                    if working_directory == _Path("/"):
                        raise FileNotFoundError("Can't find repository root")
        else:
            working_directory = _Path(__file__).parent.parent

        # Within working directory find package folder 
        # by looking for folder name with "_archive" in it, and __init__.py
        for pth in working_directory.glob("*_archive"):
            if (pth / "__init__.py").exists():
                self.root = pth
                break
        else:
            raise FileNotFoundError("Can't find package folder. Make sure your package has '_archive' in the folder name and __init__.py")

        self.data = self.root.parent / "data"

        # If this_repo is set, exit, to avoid config file confusion
        if this_repo:
            return

        # Check if config file exists
        self.config_file = self.root / "config.yaml"
        if not self.config_file.exists():
            if errors == "ignore":
                return
            raise FileNotFoundError(
                "Config file not found. Try running config.setup first")

        # Read config file
        with open(self.config_file) as f:
            config = yaml.safe_load(f)
        
        self.user = config["user"]["name"]

        # If network is not set, exit
        if not network:
            return

        # Check if network exists in config file
        if not network in config["paths"].keys():
            if errors == "ignore":
                return
            else:
                raise KeyError(f"Network {network} not found in config file")

        # Read all sub-paths associated with network and INPUTS
        for key, value in config["paths"][network].items():

            # If key is either of the output paths, go to next key
            if "output_path" in key:
                continue

            # If a site is set, it means that the value is a dictionary because species data are in a sub-directory
            if isinstance(value, dict):
                if not site:
                    if errors == "raise":
                        print(f"WARNING: Site not set for {key}... skipping")
                else:
                    if site not in value.keys():
                        if errors == "raise":
                            print(f"WARNING: Site {site} not found in {key}... skipping")
                    else:
                        self.__setattr__(key, value[site])
            else:
                self.__setattr__(key, value)
            
            # Test that path exists
            if errors == "raise" or errors == "ignore_outputs":
                if isinstance(value, dict):
                    if not site:
                        print(f"WARNING: Site not set for {key}... skipping")
                        continue
                    else:
                        if site not in value.keys():
                            print(f"WARNING: Site {site} not found in {key}... skipping")
                            continue
                        full_path = self.data / network / value[site]
                else:
                    full_path = self.data / network / value

                if not full_path.exists():
                    raise FileNotFoundError(f"Folder or zip archive {full_path} doesn't exist")
                if not (full_path.is_dir() or full_path.suffix == ".zip" or full_path.suffix == ".gz"):
                    raise FileNotFoundError(f"{full_path} is not a folder or zip archive")

        # Don't need to do the remaining checks if errors is set to ignore_outputs
        if "output_path" not in config["paths"][network]:
            if errors == "raise" or errors == "ignore_inputs":
                raise KeyError(f"Output path not found in config file")
            else:
                return
            
        # Set OUTPUT path
        self.output_path = config["paths"][network]["output_path"]
        
        # Test that output path exists
        if errors == "raise" or errors == "ignore_inputs":
            full_path = self.data / network / self.output_path
            if not (full_path).exists():
                raise FileNotFoundError(f"Folder or zip archive {full_path} doesn't exist")
            if not (full_path.is_dir() or full_path.suffix == ".zip"):
                raise FileNotFoundError(f"{full_path} is not a folder or zip archive")


def setup(network = ""):
    """ Setup the config.yml file for the agage_archive package.
    """

    #TODO: this_repo could cause confusion in the case that someone uses this script
    # to set up a config file in another repository
    # However, setting it for now, so that it doesn't look for config file before it's created
#    paths = Paths(this_repo=True)
    paths = Paths(errors="ignore")

    header = '''# Use this file to store configuration settings
# All paths are relative to the network subfolder in the data directory
# If you need to put data files elsewhere, you'll need to use symlinks
---
'''

    config = {}

    # User name
    usr = input("Name (press enter for system ID):") or ""
    config["user"] = {"name": usr}

    if not network:
        config["paths"] = {
            "agage_test":
                {
                    "md_path": "data-nc",
                    "optical_path": "data-optical-nc",
                    "gcms_path": "data-gcms-nc",
                    "gcms_flask_path": "data-gcms-flask-nc",
                    "ale_path": "ale",
                    "gage_path": "gage",
                    "magnum_path": "data-gcms-magnum.tar.gz",
                    "output_path": "output",
                },
            "agage":
                {
                    "md_path": "data-nc",
                    "optical_path": "data-optical-nc",
                    "gcms_path": "data-gcms-nc",
                    "gcms_flask_path": "data-gcms-flask-nc",
                    "ale_path": "ale_gage_sio1993/ale",
                    "gage_path": "ale_gage_sio1993/gage",
                    "magnum_path": "data-gcms-magnum.tar.gz",
                    "output_path": "agage-public-archive.zip",
            }
        }
    else:
        config["paths"] = {
            network:
                {
                    "md_path": "",
                    "optical_path": "",
                    "gcms_path": "",
                    "gcms_flask_path": "",
                    "output_path": "output",
                }
        }

    with open(paths.root / 'config.yaml', 'w') as configfile:
        # Write header lines
        configfile.write(header)
        # Dump config dictionary as yaml
        yaml.dump(config, configfile,
                  default_flow_style=False,
                  sort_keys=False)
        
    print(f"Config file written to {paths.root / 'config.yaml'}")
    print("Config file has been populated with default sub-paths relative to data/network. " + \
          "If you want to move the data elsewhere, manually modify the sub-paths in the config file. " + \
          "If the files need to be outside of the data directory, use symlinks.")


def data_file_list(network = "",
                   sub_path = "",
                   pattern = "*",
                   ignore_hidden = True,
                   errors="ignore",
                   sub_directories = True,
                   site = ""):
    """List files in data directory. Structure is data/network/sub_path
    sub_path can be a zip archive

    Args:
        network (str, optional): Network. Defaults to "".
        sub_path (str, optional): Sub-path. Defaults to "".
        pattern (str, optional): Pattern to match. Defaults to "*".
        ignore_hidden (bool, optional): Ignore hidden files. Defaults to True.
        errors (str, optional): See options in Paths class. Defaults to "raise".
        sub_directories (bool, optional): If False, will remove sub-directories. Defaults to True.

    Returns:
        tuple: Tuple containing network, sub-path and list of files
    """

    def return_sub_path(full_path):
        pth = ""
        for p in full_path.parts[::-1]:
            if (p == "data") or (p == network):
                break
            else:
                pth = p + "/" + pth
        # remove trailing slash if it exists
        if pth.endswith("/"):
            pth = pth[:-1]
        return pth

    def remove_sub_directories(files):
        nslash = [f.count("/") for f in files]
        max_slash = min(nslash)
        return [f for f in files if f.count("/") == max_slash]

    pth = data_file_path("", network=network, sub_path=sub_path, errors=errors, site=site)

    if pth.suffix == ".zip":
        
        # If zip archive doesn't exist, return empty list
        if not pth.exists() and "ignore" in errors:
            return network, return_sub_path(pth), []
        
        with ZipFile(pth, "r") as z:
            files = []
            for f in z.filelist:
                if fnmatch(f.filename, pattern) and not (ignore_hidden and f.filename.startswith(".")):
                    files.append(f.filename)
            if not sub_directories:
                files = remove_sub_directories(files)
            return network, return_sub_path(pth), files
    else:
        files = []
        # This is written this way to give the same output as the zip archive
        for f in pth.glob("**/*"):
            if fnmatch(str(f), "*" + pattern) and not (ignore_hidden and f.name.startswith(".")):
                # Append everything in file path after pth
                if f.is_dir():
                    files.append(str(f.relative_to(pth)) + "/")
                else:
                    files.append(str(f.relative_to(pth)))
        if not sub_directories:
            files = remove_sub_directories(files)
        return network, return_sub_path(pth), files


def data_file_path(filename,
                   network = "",
                   sub_path = "",
                   this_repo = False,
                   errors = "ignore",
                   site = ""):
    """Get path to data file. Structure is data/network/sub_path
    sub_path can be a zip archive, in which case the path to the zip archive is returned

    Note that by default, this function is only for input data.
    Output data is handled by output_path (which is a wrapper around this function)
    
    Args:
        filename (str): Filename
        network (str, optional): Network. Defaults to "".
        sub_path (str, optional): Sub-path. Defaults to ""
        this_repo (bool, optional): If True, look for the root and data folder within this repository (no config).
            If False, will look for the root and data folders and config file in the working directory.
        errors (str, optional): If "raise", raise FileNotFoundError if file not found. If "ignore", return path

    Raises:
        FileNotFoundError: Can't find file

    Returns:
        pathlib.Path: Path to file
    """

    paths = Paths(network,
                  this_repo=this_repo,
                  errors=errors,
                  site = site)

    if network:
        pth = paths.data / network
    else:
        pth = paths.data

    if sub_path:
        pth = pth / sub_path
    
    if not pth.exists() and errors == "raise":
        raise FileNotFoundError(f"Can't find path {pth}")

    if pth.suffix == ".zip":
        # If filename is empty, user is just asking to return completed directory
        if filename == "":
            return pth
        # Otherwise, check if filename is in zip archive
        with ZipFile(pth, "r") as z:
            for f in z.filelist:
                if f.filename == filename:
                    return pth
            if errors == "raise":
                raise FileNotFoundError(f"Can't find {filename} in {pth}")
    else:
        return pth / filename


def open_data_file(filename,
                   network = "",
                   sub_path = "",
                   verbose = False,
                   this_repo = False,
                   errors = "ignore",
                   site = ""):
    """Open data file. Structure is data/network/sub_path
    sub_path can be a zip archive

    Args:
        filename (str): Filename
        network (str, optional): Network. Defaults to "".
        sub_path (str, optional): Sub-path. Defaults to "". Can be a zip archive or directory
        verbose (bool, optional): Print verbose output. Defaults to False.
        this_repo (bool, optional): If True, look for the root and data folder within this repository (no config).
            If False, will look for the root and data folders and config file in the working directory.

    Raises:
        FileNotFoundError: Can't find file

    Returns:
        file: File object
    """

    pth = data_file_path("", network=network,
                         sub_path=sub_path,
                         this_repo=this_repo,
                         errors=errors,
                         site=site)
    
    if verbose:
        print(f"... opening {pth / filename}")

    if pth.suffix == ".zip":
        with ZipFile(pth, "r") as z:
            return z.open(filter(z.namelist(), filename)[0])
    elif "tar.gz" in filename:
        return tarfile.open(pth / filename, "r:gz")
    else:
        return (pth / filename).open("rb")


def output_path(network,
                species,
                site,
                instrument,
                extra = "",
                extra_archive = "",
                version="",
                errors="ignore_inputs",
                network_out = ""):
    '''Determine output path and filename

    Args:
        network (str): Network
        species (str): Species
        site (str): Site
        instrument (str): Instrument
        extra (str, optional): Extra string to add to filename. Defaults to "".
        extra_archive (str, optional): Extra string to add to archive filename or folder. Defaults to "".
        version (str, optional): Version number. Defaults to "".
        errors (str, optional): How to handle errors if path doesn't exist. Defaults to "raise".
        network_out (str, optional): Network for filename. Defaults to "".

    Raises:
        FileNotFoundError: Can't find output path

    Returns:
        pathlib.Path: Path to output directory
        str: Filename
    '''

    # Get paths. Ignore errors since outputs may not exist at this stage
    paths = Paths(network, errors="ignore")

    version_str = f"{version.replace(' ','')}" if version else ""

    if extra:
        if extra[-1] == "-":
            pass
        elif extra[-1] == "_":
            extra = extra[:-1] + "-"
        else:
            extra = extra + "-"
    
    if extra_archive:
        archive_path = archive_suffix(paths.output_path,
                                      extra_archive)
    else:
        archive_path = paths.output_path

    # Can tweak data_file_path to get the output path
    output_path = data_file_path("", network = network,
                                sub_path = archive_path,
                                errors=errors)
    
    # Create filename
    if network_out:
        network_str = network_out.lower()
    else:
        network_str = network.lower()

    if instrument:
        instrument_str = f"-{instrument}"
    else:
        instrument_str = ""

    filename = f"{network_str}{instrument_str}_{site.lower()}_{species}_{extra}{version_str}.nc"

    return output_path, filename


def copy_to_archive(src_file, network):
    """Copy file to archive. Structure is data/network/sub_path
    sub_path can be a zip archive

    Args:
        src_file (str): Source file
        network (str, optional): Network. Defaults to "".

    Raises:
        FileNotFoundError: Can't find file

    Returns:
        file: File object
    """

    archive_path, _ = output_path(network, "_", "_", "_",
                                errors = "ignore_inputs")

    if archive_path.suffix == ".zip":
        with ZipFile(archive_path, "a") as z:
            z.write(src_file, arcname=src_file.name)
    else:
        # Copy file into pth directory
        copy(src_file, archive_path / src_file.name)


def is_jupyterlab_session():
    """Check whether we are in a Jupyter-Lab session.
    Taken from:
    https://stackoverflow.com/questions/57173235/how-to-detect-whether-in-jupyter-notebook-or-lab
    """

    # inspect parent process for any signs of being a jupyter lab server
    parent = psutil.Process().parent()
    if parent.name() == "jupyter-lab":
        return "jupyterlab"
    
    keys = (
        "JUPYTERHUB_API_KEY",
        "JPY_API_TOKEN",
        "JUPYTERHUB_API_TOKEN",
    )
    env = parent.environ()
    if any(k in env for k in keys):
        return "jupyterlab"

    return "notebook"


def archive_suffix(path, suffix):
    """Insert a suffix into the archive name before the file extension or folder name.
    Args:
        name (str): The original archive name, e.g. "archive.zip" or "folder/"
        suffix (str): The suffix to insert, e.g. "-csv"
    Returns:
        str: The modified archive name with the suffix inserted
    """

    # If name is a Path object, convert it to string
    if isinstance(path, os.PathLike):
        name = str(path)
        isPath = True
    else:
        name = path
        isPath = False

    if ".zip" in name:
        # If the name is a zip file, we need to insert the suffix before the .zip
        parts = name.split(".zip")
        name = f"{parts[0]}{suffix}.zip"
    else:
        # If the name is not a zip file, assume it's a folder, which may have a trailing slash
        parts = name.rsplit("/", 1)
        if len(parts) == 1:
            # No slash found, just append the suffix
            name = f"{parts[0]}{suffix}"
        else:
            # Insert the suffix before the last part (the folder name)
            name = f"{parts[0]}{suffix}/"

    # If the original path was a Path object, convert back to Path
    if isPath:
        return Paths().root / name
    else:
        return name


def delete_archive(network, archive_suffix_string=""):
    """Delete all files in output directory before running

    Args:
        network (str): Network for output filenames
        archive_suffix_string (str, optional): Suffix to add to archive name. Defaults to "".
    """

    path = Paths(network, errors="ignore")

    try:
        out_pth, _ = output_path(network, "_", "_", "_",
                                errors="ignore_inputs",
                                extra_archive=archive_suffix_string)
    except FileNotFoundError:
        print(f"Output directory or archive for does not exist, continuing")
        return

    # Find all files in output directory
    _, _, files = data_file_list(network=network,
                                sub_path=archive_suffix(path.output_path, archive_suffix_string),
                                errors="ignore")

    print(f'Deleting all files in {out_pth}')

    # If out_pth is a zip file, delete it
    if out_pth.suffix == ".zip" and out_pth.exists():
        out_pth.unlink()
    else:
        # For safety that out_pth is in data/network directory
        if out_pth.parents[1] == path.data and out_pth.parents[0].name == network:
            pass
        else:
            raise ValueError(f"{out_pth} must be in a data/network directory")

        # Delete all files in output directory
        for f in files:
            pth = out_pth / f
            if pth.is_file():
                pth.unlink()
            elif pth.is_dir():
                rmtree(pth)


def create_empty_archive(network, archive_suffix_string=""):
    """Create an empty output zip file or folders

    Args:
        network (str): Network for output filenames
        archive_suffix_string (str, optional): Suffix to add to archive name. Defaults to "".
    """

    out_pth, _ = output_path(network, "_", "_", "_",
                            errors="ignore",
                            extra_archive=archive_suffix_string)

    if out_pth.suffix == ".zip" and not out_pth.exists():
        with ZipFile(out_pth, "w") as f:
            pass
    elif out_pth.is_dir():
        out_pth.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    setup()