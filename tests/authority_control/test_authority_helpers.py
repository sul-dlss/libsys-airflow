import pandas as pd

import pymarc
import pytest


from libsys_airflow.plugins.authority_control.helpers import (
    archive_csv_files,
    batch_csv,
    clean_csv_file,
    clean_up,
    create_batches,
    delete_authorities,
    find_authority_by_001,
    trigger_load_record_dag,
)


@pytest.fixture
def mock_folio_client(mocker):
    def mock_delete(*args, **kwargs):
        result = {}
        if args[0].endswith("05f594c41bb6"):
            raise ValueError("404: Not Found")
        return result

    def mock_get(*args, **kwargs):
        query = kwargs.get("query")
        natural_id = query.split("==")[-1]
        result = {}
        match natural_id:

            case "n79008406":
                return [{"id": "edb30998-7f1d-47f1-a072-328e8c094557"}]

            case "n79010634":
                raise ValueError("Server Error retieving naturalID")

            case "n79015478":
                result = [
                    {"id": "2ca1f846-55b4-478a-abab-a16fbfca8aa9"},
                    {"id": "472a48e6-c195-4f02-822b-4ae86a84f661"},
                ]

            case _:
                result = []

        return result

    mock_client = mocker.MagicMock()
    mock_client.folio_get = mock_get
    mock_client.folio_delete = mock_delete
    return mock_client


def test_archive_csv_files(tmp_path, caplog):
    authorities_uploads_path = tmp_path / "authorities/uploads/"
    authorities_uploads_path.mkdir(parents=True)

    original_csv = authorities_uploads_path / "test.csv"
    original_csv.touch()
    updated_csv = authorities_uploads_path / "update-1765926805.csv"
    updated_csv.touch()
    batch1_csv = authorities_uploads_path / "update-1765926805-01.csv"
    batch1_csv.touch()
    batch2_csv = authorities_uploads_path / "update-1765926805-02.csv"
    archive_csv_files(
        airflow=tmp_path,
        csv_files=[
            str(original_csv),
            str(updated_csv),
            str(batch1_csv),
            str(batch2_csv),
        ],
    )
    assert original_csv.exists() is False
    assert updated_csv.exists() is False
    assert batch1_csv.exists() is False
    assert f"{batch2_csv} does not exist, cannot archive" in caplog.text

    archive_updated_csv = tmp_path / "authorities/archive/uploads/update-1765926805.csv"
    assert archive_updated_csv.exists()


def test_batch_csv(tmp_path):

    test_file = tmp_path / "update-1765921205-test.csv"
    with test_file.open("w+") as fo:
        fo.write("001s\n")
        for i in range(2_078):
            fo.write(f"n{i:08d}\n")

    batches = batch_csv(file=str(test_file.absolute()))
    assert len(batches) == 5
    second_batch_df = pd.read_csv(batches[1])
    assert len(second_batch_df) == 500
    assert second_batch_df['001s'].iloc[0] == "n00000500"
    assert second_batch_df['001s'].iloc[-1] == "n00000999"
    last_batch_df = pd.read_csv(batches[-1])
    assert len(last_batch_df) == 78
    assert last_batch_df['001s'].iloc[0] == "n00002000"
    assert last_batch_df['001s'].iloc[-1] == "n00002077"


def test_clean_csv_file(tmp_path):
    uploads_path = tmp_path / "authorities/uploads"
    uploads_path.mkdir(parents=True)

    test_csv_path = uploads_path / "test.csv"

    with test_csv_path.open("w+") as fo:
        for field001_value in ["001s", "n\\79000500\\", "n 79045242", "n79044348"]:
            fo.write(f"{field001_value}\n")

    updated_csv_file = clean_csv_file(
        airflow=tmp_path, file=str(test_csv_path.absolute())
    )
    assert updated_csv_file.endswith("-test.csv")

    updated_001s_df = pd.read_csv(updated_csv_file)

    updated_001s = updated_001s_df.to_dict(orient='records')

    assert len(updated_001s) == 3
    assert updated_001s[0]['001s'] == "n79000500"
    assert updated_001s[1]['001s'] == "n79045242"


def test_clean_up(mocker, tmp_path):
    authority_marc_file = tmp_path / "authority.mrc"
    authority_marc_file.touch()

    clean_up(str(authority_marc_file), airflow=str(tmp_path))

    assert authority_marc_file.exists() is False
    assert (tmp_path / "authorities/archive/authority.mrc").exists()


def test_create_batches(tmp_path):
    authority_marc_file = tmp_path / "authority.mrc"
    with authority_marc_file.open("wb") as fo:
        writer = pymarc.MARCWriter(fo)
        for i in range(50_100):
            record = pymarc.Record()
            record.add_field(
                pymarc.Field(
                    tag="245",
                    indicators=["0", "0"],
                    subfields=[pymarc.Subfield(code="a", value=f"Test Record {i}")],
                )
            )
            writer.write(record)

    batches = create_batches(str(authority_marc_file), airflow=str(tmp_path))

    assert len(batches) == 6
    assert (tmp_path / "authorities/authority_1.mrc").exists()
    assert (tmp_path / "authorities/authority_2.mrc").exists()


def test_delete_authorities(mock_folio_client):
    deletes = [
        "2e8dd0b8-075e-4e86-b2bd-ef804870e5e0",
        "e7329957-40d5-4f9c-b705-05f594c41bb6",
    ]
    output = delete_authorities(deletes=deletes, client=mock_folio_client)
    assert output["deleted"] == 1
    assert output["errors"][0].startswith("e7329957-40d5-4f9c-b705-05f594c41bb6: 404")


def test_find_authority_by_001(tmp_path, mock_folio_client):
    update_csv_file = tmp_path / "update-765921405-test.csv"
    with update_csv_file.open("w+") as fo:
        fo.write("001s\n")
        for row in ["n79008406", "n79010634", "n79042815", "n79015478"]:
            fo.write(f"{row}\n")

    results = find_authority_by_001(
        client=mock_folio_client, file=str(update_csv_file.absolute())
    )
    assert results["deletes"] == ['edb30998-7f1d-47f1-a072-328e8c094557']
    assert results["errors"][0].startswith("n79010634: Server Error")
    assert results["missing"] == ["n79042815"]
    assert results["multiples"][0].startswith("n79015478: 2ca1f846-55b4")
    assert results["multiples"][0].endswith("-4ae86a84f661")


def test_trigger_load_record_dag(mocker):
    operator = trigger_load_record_dag("authority.mrc", "authority_profile")
    assert operator.task_id == "trigger_load_record_dag"
