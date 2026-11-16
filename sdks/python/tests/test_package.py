"""The package metadata and the runtime version stay in step."""

from importlib.metadata import version

import rillkit


def test_runtime_version_matches_installed_metadata():
    assert rillkit.__version__ == version("rillkit")
