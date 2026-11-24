from zipfile import ZipFile

import pytest

from agage_archive.config import output_path, archive_zip_root, load_json


pytest.importorskip("ipywidgets")


# Members of a small archive, at archive-relative paths (as written before nesting)
ARCHIVE_MEMBERS = [
    "README.md",
    "ch4/agage_mhd_ch4_testv1.nc",
    "ch4/agage_cgo_ch4_testv1.nc",
    "ch4/individual-instruments/agage-picarro_mhd_ch4_testv1.nc",
    "ch4/monthly-baseline/agage_mhd_ch4_monthly-baseline_testv1.nc",
    "n2o/agage_mhd_n2o_testv1.nc",
]


@pytest.fixture(params=["flat", "nested"])
def output_zip(request, monkeypatch):
    """Redirect agage_test output to a zip, either flat or nested under a version folder."""

    from tests.test_archive import _patch_config_output, NETWORK

    _patch_config_output(monkeypatch, "output.zip")

    archive, _ = output_path(NETWORK, "_", "_", "_", errors="ignore")
    if archive.exists():
        archive.unlink()

    # Create the archive first: load_json checks that the output path exists
    ZipFile(archive, "w").close()

    prefix = ""
    if request.param == "nested":
        version = load_json("attributes.json", network=NETWORK).get("version", "")
        prefix = archive_zip_root(archive, version) + "/"

    with ZipFile(archive, "w") as z:
        for member in ARCHIVE_MEMBERS:
            z.writestr(prefix + member, "data")

    yield NETWORK, prefix

    if archive.exists():
        archive.unlink()


def test_output_species(output_zip):
    """Species come from the folders inside the archive, not its top-level version folder."""

    from agage_archive.widgets import output_species

    network, _ = output_zip

    assert output_species(network) == ["ch4", "n2o"]


@pytest.mark.parametrize("file_type, expected", [
    ("high-frequency", {"cgo, agage": "ch4/agage_cgo_ch4_testv1.nc",
                        "mhd, agage": "ch4/agage_mhd_ch4_testv1.nc"}),
    ("individual-instruments", {"mhd, agage-picarro":
                                "ch4/individual-instruments/agage-picarro_mhd_ch4_testv1.nc"}),
    ("monthly-baseline", {"mhd, agage":
                          "ch4/monthly-baseline/agage_mhd_ch4_monthly-baseline_testv1.nc"}),
])
def test_update_instrument_site(output_zip, file_type, expected):
    """Only the selected species' files are offered, and they point at real zip members."""

    from agage_archive import widgets

    network, prefix = output_zip

    options = widgets.update_instrument_site("ch4", file_type, network, None)

    assert list(options) == sorted(expected)
    assert widgets.instrument_site_filenames == {k: prefix + v for k, v in expected.items()}
