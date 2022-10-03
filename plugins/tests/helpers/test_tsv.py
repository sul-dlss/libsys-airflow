import pandas as pd
import pydantic
import pytest

from plugins.folio.helpers.tsv import (
    _merge_notes,
    transform_move_tsvs,
)

from plugins.tests.mocks import mock_file_system  # noqa
from pytest_mock import MockerFixture


@pytest.fixture
def mock_dag_run(mocker: MockerFixture):
    dag_run = mocker.stub(name="dag_run")
    dag_run.run_id = "manual_2022-02-24"
    return dag_run


class MockDagRun(pydantic.BaseModel):
    run_id: str = "manual_2022-09-30T22:03:42"


# Mock xcom messages dict
messages = {}


# Mock xcoms
def mock_xcom_push(*args, **kwargs):
    key = kwargs["key"]
    value = kwargs["value"]
    messages[key] = value


def mock_xcom_pull(*args, **kwargs):
    task_id = kwargs["task_ids"]
    key = kwargs["key"]
    if task_id in messages:
        if key in messages[task_id]:
            return messages[task_id][key]
    return "unknown"


class MockTaskInstance(pydantic.BaseModel):
    xcom_pull = mock_xcom_pull
    xcom_push = mock_xcom_push


def test_merge_notes_circnotes(mock_file_system):  # noqa

    circ_path = mock_file_system[1] / "test.sample2.circnote.tsv"

    circ_notes_df = pd.DataFrame(
        [{"BARCODE": "36105033974929  ", "CIRCNOTE": "pen marks 6/5/19cc"}]
    )

    circ_notes_df.to_csv(circ_path, sep="\t", index=False)

    notes_df = _merge_notes(circ_path)

    note_row = notes_df.loc[notes_df["BARCODE"] == "36105033974929"]
    assert note_row["note"].item() == "pen marks 6/5/19cc"
    assert note_row["NOTE_TYPE"].item() == "CIRCNOTE"


def test_merge_notes_techstaff(mock_file_system):  # noqa
    techstaff_path = mock_file_system[1] / "test.sample2.techstaff.tsv"

    techstaff_df = pd.DataFrame(
        [
            {
                "BARCODE": "36105031890341",
                "TECHSTAFF": "rf:370.4 .J65 no.15 c.3, hbr 6/1/06",
            }
        ]
    )

    techstaff_df.to_csv(techstaff_path, sep="\t", index=False)

    notes_df = _merge_notes(techstaff_path)

    note_row = notes_df.loc[notes_df["BARCODE"] == "36105031890341"]
    assert note_row["note"].item() == "rf:370.4 .J65 no.15 c.3, hbr 6/1/06"
    assert note_row["NOTE_TYPE"].item() == "TECHSTAFF"


def test_transform_move_tsvs(mock_file_system, mock_dag_run):  # noqa
    airflow_path = mock_file_system[0]
    source_dir = mock_file_system[1]

    # mock sample tsv
    symphony_tsv = source_dir / "sample.tsv"
    symphony_tsv.write_text(
        """CATKEY\tFORMAT\tCALL_NUMBER_TYPE\tBARCODE\tLIBRARY\tITEM_TYPE
123456\tMARC\tLC 12345\t45677  \tHOOVER\tNONCIRC
789012\tMARC\tLC 67890\t12345  \tGREEN\tSTKS-MONO"""
    )

    symphony_notes_tsv = source_dir / "sample.public.tsv"
    symphony_notes_tsv.write_text("BARCODE\tPUBLIC\n45677 \tAvailable for checkout")

    # mock sample CIRCNOTE tsv
    symphony_circnotes_tsv = source_dir / "sample.circnote.tsv"
    symphony_circnotes_tsv.write_text(
        "BARCODE\tCIRCNOTE\n45677 \tpencil marks 7/28/18cc"
    )

    column_transforms = [
        ("CATKEY", lambda x: f"a{x}"),
        ("BARCODE", lambda x: x.strip()),
    ]

    libraries = ["HOOVER", "HV-ARCHIVE"]

    data_prep = airflow_path / "migration/data_preparation/"

    data_prep.mkdir(parents=True)

    # Mocks successful upstream task
    global messages
    messages["bib-files-group"] = {
        "tsv-files": [
            str(symphony_notes_tsv),
            str(symphony_circnotes_tsv),
        ],
        "tsv-base": str(symphony_tsv),
    }

    transform_move_tsvs(
        airflow=airflow_path,
        column_transforms=column_transforms,
        libraries=libraries,
        task_instance=MockTaskInstance(),
        source="symphony",
        tsv_stem="sample",
        dag_run=mock_dag_run,
    )
    tsv_directory = (
        airflow_path / f"migration/iterations/{mock_dag_run.run_id}/source_data/items"
    )
    sample_tsv = tsv_directory / "sample.tsv"

    with open(sample_tsv, "r") as f:
        sample_lines = f.readlines()

    assert (
        sample_lines[1]
        == "a123456\tMARC\tLC 12345\t45677\tHOOVER\tNONCIRC MARC HOOVER\n"
    )
    assert sample_lines[2] == "a789012\tMARC\tLC 67890\t12345\tGREEN\tSTKS-MONO\n"

    messages = {}


def test_transform_move_tsvs_doesnt_exist(mock_file_system, mock_dag_run):  # noqa
    airflow_path = mock_file_system[0]

    data_prep = airflow_path / "migration/data_preparation/"

    data_prep.mkdir(parents=True)

    with pytest.raises(FileNotFoundError, match="No such file or directory"):
        transform_move_tsvs(
            airflow=airflow_path,
            source="symphony",
            task_instance=MockTaskInstance(),
            tsv_stem="sample",
            dag_run=mock_dag_run,
        )
