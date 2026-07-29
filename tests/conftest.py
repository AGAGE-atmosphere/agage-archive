import shutil

import pytest

from agage_archive.config import data_file_path


@pytest.fixture
def clean_output():
    """Empty the agage_test output directory, and empty it again afterwards.

    Several tests write into data/agage_test/output. Without this fixture they leave
    files behind for each other to trip over, which makes the suite order-dependent.
    Anything a test needs to assert on should be read before it returns.

    Yields:
        pathlib.Path: Path to the (empty) output directory.
    """

    pth = data_file_path("", network="agage_test", sub_path="output", errors="ignore")

    def empty():
        if not pth.exists():
            pth.mkdir(parents=True, exist_ok=True)
            return
        for f in sorted(pth.iterdir()):
            if f.name.startswith("."):
                # Keep .gitignore and friends
                continue
            if f.is_dir():
                shutil.rmtree(f)
            else:
                f.unlink()

    empty()
    yield pth
    empty()
