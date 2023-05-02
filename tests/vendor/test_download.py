import pytest  # noqa
import pathlib

from libsys_airflow.plugins.vendor.download import download


@pytest.fixture
def hook(mocker):
    mock_hook = mocker.patch("airflow.providers.ftp.hooks.ftp.FTPHook")
    mock_hook.list_directory.return_value = [
        "3820230411.mrc",
        "3820230412.mrc",
        "3820230412.xxx",
    ]
    return mock_hook


@pytest.fixture
def download_path(tmp_path):
    pathlib.Path(f"{tmp_path}/3820230412.mrc").touch()
    return str(tmp_path)


def test_download(hook, download_path):
    download(hook, "oclc", download_path, ".+\.mrc")

    assert hook.list_directory.call_count == 1
    assert hook.list_directory.called_with("oclc")
    assert hook.retrieve_file.call_count == 1
    assert hook.retrieve_file.called_with(
        "3820230411.mrc", f"{download_path}/3820230411.mrc"
    )
