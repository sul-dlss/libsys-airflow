import csv
import pandas as pd
import pathlib
import pytest

from libsys_airflow.plugins.data_exports.instance_ids import save_ids_to_fs
from libsys_airflow.plugins.data_exports.data_export_upload_view import (
    upload_data_export_ids,
)

from unittest.mock import MagicMock


@pytest.fixture
def mock_task_instance():
    mock_task_instance = MagicMock()

    mock_task_instance.xcom_pull = mock_xcom_pull

    return mock_task_instance


def mock_xcom_pull(**kwargs):
    return [
        [],
        [
            ['4e66ce0d-4a1d-41dc-8b35-0914df20c7fb'],
            ['fe2e581f-9767-442a-ae3c-a421ac655fe2'],
        ],
        [],
    ]


def test_save_ids_to_fs(tmp_path, mock_task_instance):
    save_path = save_ids_to_fs(
        airflow=tmp_path, task_instance=mock_task_instance, vendor="oclc"
    )

    file = pathlib.Path(save_path)
    assert file.exists()

    with file.open('r') as fo:
        id_list = list(row for row in csv.reader(fo))

    assert id_list[0][1] == "['fe2e581f-9767-442a-ae3c-a421ac655fe2']"


def test_upload_data_export_file_ids_one_column():
    data = {
        'Name': ['Joe', 'Shel', 'Ger'],
        'Age': [20, 21, 19],
        'Height': [6.1, 5.9, 6.0],
    }
    df = pd.DataFrame(data)

    with pytest.raises(ValueError, match="ID file has more than one column."):
        upload_data_export_ids(df, 'gobi')


def test_upload_data_export_file_not_uuid():
    data = {'UUID': ['Joe', 'Shel', 'Ger']}
    df = pd.DataFrame(data)

    with pytest.raises(ValueError, match="Joe is not a UUID."):
        upload_data_export_ids(df, 'gobi')
