from pathlib import Path
import yaml
import json

from agage_archive.config import Paths, data_file_path, \
    open_data_file, data_file_list, output_path


repo_path = Path(__file__).resolve().parents[1]


def test_config_exists():
    """ Check that the configuration file exists
    This is just so to help diagnose this as a problem if subsequent tests fail
    """

    config_file = repo_path / "agage_archive" / "config.yaml"

    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found at {config_file}. Create one before running tests")

    # Read the configuration file
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    # Check that agage-test network is in the configuration file
    if not "agage_test" in config["paths"].keys():
        raise KeyError("agage_test network not found in configuration file. Must exist to run tests")


def test_paths():

    # Check that "this_repo" option returns the repository path
    path_this_repo = Paths(this_repo=True)
    assert path_this_repo.root == repo_path / "agage_archive"
    assert path_this_repo.data == repo_path / "data"

    # Read the configuration file for agage_test network
    config_file = repo_path / "agage_archive" / "config.yaml"
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    # Check that some of the agage_test paths are as expected
    path = Paths("agage_test")
    assert path.output_path == \
              config["paths"]["agage_test"]["output_path"]

    # If we try to retrieve a path that doesn't exist, it should raise a KeyError
    try:
        path = Paths("non_existent_network", errors = "raise")
    except KeyError:
        pass
    else:
        raise AssertionError("KeyError not raised for non-existent network")

    # If we try to retrieve a path that doesn't exist with errors ignored, it should not error
    path = Paths("non_existent_network", errors="ignore")

    # Test for path
    path = Paths("agage_test", errors="ignore")
    assert path.output_path == \
              config["paths"]["agage_test"]["output_path"]


def test_data_file_path():

    assert data_file_path("test.txt", "agage_test", "path_test_files").exists()

    # This should just return the zip file path, but checks for the existance of the file internally
    assert data_file_path("test_top_level.txt", "agage_test", "path_test_files/A.zip").exists()

    # This should just return the zip file path, but checks for the existance of the file internally
    assert data_file_path("B/C.txt", "agage_test", "path_test_files/A.zip").exists()

    # Test that we can find a required file in this repo
    assert data_file_path("attributes.json", this_repo=True).exists()


def test_open_data_file():

    # Test that the attributes json is openned correctly
    with open_data_file("attributes.json", this_repo=True) as f:
        attributes = json.load(f)
    assert "calibration_scale" in attributes.keys()
    assert attributes["species"] == ""

    # Test that we can open a file within the A.zip file
    with open_data_file("B/C.txt", "agage_test", "path_test_files/A.zip") as f:
        assert f.read().decode("utf-8") == "test"


def test_data_file_list():

    # Test that we can list files in a folder
    network, sub_path, files = data_file_list("agage_test", "path_test_files")
    assert network == "agage_test"
    assert sub_path == "path_test_files/"
    assert "test.txt" in files

    # Test that we can list files in a zip archive
    files = data_file_list("agage_test", "path_test_files/A.zip")[2]
    assert "test_top_level.txt" in files
    assert "B/C.txt" in files

    # Test that we can list files in a zip archive with pattern
    files = data_file_list("agage_test", "path_test_files/A.zip", pattern="*.txt")[2]
    assert "test_top_level.txt" in files
    assert "B/C.txt" in files

    # Test that we can list files within a subdirectory of a zip archive
    files_zip = data_file_list("agage_test", "path_test_files/A.zip", pattern="B/*.txt")[2]
    assert "test_top_level.txt" not in files_zip
    assert "B/C.txt" in files_zip

    # Test that we get the same output but from an unzipped directory
    files = data_file_list("agage_test", "path_test_files/A", "B/*.txt")[2]
    assert "test_top_level.txt" not in files
    assert "B/C.txt" in files


def test_output_path():

    # Read the configuration file for agage_test network
    config_file = repo_path / "agage_archive" / "config.yaml"
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    # Check that some of the agage_test paths are as expected
    # First check for an output file
    path = Paths("agage_test", errors="ignore")
    out_path_true = path.data / "agage_test" / \
        config["paths"]["agage_test"]["output_path"]

    out_path, filename = output_path("agage_test",
                                    "cfc-11", "THD", "gcms-medusa",
                                    extra="testing", version="v1",
                                    errors="ignore")
    assert out_path == out_path_true
    assert filename == "agage_test-gcms-medusa_thd_cfc-11_testing-v1.nc"

