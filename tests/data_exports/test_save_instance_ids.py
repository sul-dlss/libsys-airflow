import pandas as pd
import pathlib
import pytest

from libsys_airflow.plugins.data_exports.instance_ids import save_ids_to_fs, save_ids
from libsys_airflow.plugins.data_exports.apps.data_export_upload_view import (
    upload_data_export_ids,
)

from unittest.mock import MagicMock


@pytest.fixture
def mock_task_instance():
    mock_task_instance = MagicMock()

    mock_task_instance.xcom_pull = mock_xcom_pull

    return mock_task_instance


@pytest.fixture
def mock_ti_param_only():
    mock_ti_param = MagicMock()

    mock_ti_param.xcom_pull = mock_xcom_param

    return mock_ti_param


def mock_xcom_pull(**kwargs):
    return {
        "new": [
            'ecab8fc2-5a84-4a6e-a8a5-536fd37fd242',
            '942b117a-9d10-48fa-bf4d-75f042e20fe5',
        ],
        "updates": [
            '4e66ce0d-4a1d-41dc-8b35-0914df20c7fb',
            'fe2e581f-9767-442a-ae3c-a421ac655fe2',
        ],
        "deletes": [
            '336971cd-2ea1-4ad2-af86-22ae7c0a95ae',
            '4e66ce0d-4a1d-41dc-8b35-0914df20c7fb',
        ],
    }


def mock_xcom_param(**kwargs):
    return {
        "new": [],
        "updates": [],
        "deletes": [],
        "fetch_folio_record_ids": False,
        "saved_record_ids_kind": "updates",
    }


def test_save_ids_to_fs(tmp_path, mock_task_instance):
    save_path = save_ids_to_fs(
        airflow=tmp_path, task_instance=mock_task_instance, vendor="oclc"
    )

    file_list = []
    assert "new" in save_path[0]
    assert "updates" in save_path[1]
    assert "deletes" in save_path[2]

    for _, path in enumerate(save_path):
        file = pathlib.Path(path)
        assert file.exists()
        with file.open('r') as fo:
            for row in fo:
                file_list.append(row)

    assert file_list == [
        'ecab8fc2-5a84-4a6e-a8a5-536fd37fd242\n',
        '942b117a-9d10-48fa-bf4d-75f042e20fe5\n',
        '4e66ce0d-4a1d-41dc-8b35-0914df20c7fb\n',
        'fe2e581f-9767-442a-ae3c-a421ac655fe2\n',
        '336971cd-2ea1-4ad2-af86-22ae7c0a95ae\n',
        '4e66ce0d-4a1d-41dc-8b35-0914df20c7fb\n',
    ]


def test_save_ids_to_fs_given_kind(tmp_path, mock_ti_param_only):
    mock_uploaded_data = [
        'ecab8fc2-5a84-4a6e-a8a5-536fd37fd242\n',
        '942b117a-9d10-48fa-bf4d-75f042e20fe5\n',
    ]
    save_ids(airflow=tmp_path, vendor="pod", data=mock_uploaded_data, kind="updates")

    save_path = save_ids_to_fs(
        airflow=tmp_path,
        task_instance=mock_ti_param_only,
        vendor="pod",
        record_id_kind="updates",
    )

    import re

    filepath = re.compile(
        rf"{tmp_path}/data-export-files/pod/instanceids/updates/[0-9]+\.csv"
    )
    assert re.match(filepath, save_path[0])


def test_upload_data_export_file_ids_one_column():
    data = {
        'Name': ['Joe', 'Shel', 'Ger'],
        'Age': [20, 21, 19],
        'Height': [6.1, 5.9, 6.0],
    }
    df = pd.DataFrame(data)

    with pytest.raises(ValueError, match="ID file has more than one column."):
        upload_data_export_ids(df, 'gobi', 'updates')


def test_upload_data_export_file_not_uuid():
    data = {'UUID': ['Joe', 'Shel', 'Ger']}
    df = pd.DataFrame(data)

    with pytest.raises(ValueError, match="Joe is not a UUID."):
        upload_data_export_ids(df, 'gobi', 'deletes')
