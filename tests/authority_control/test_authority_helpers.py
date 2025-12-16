import pandas as pd

import pymarc
import pytest


from libsys_airflow.plugins.authority_control.helpers import (
    batch_csv,
    clean_csv_file,
    clean_up,
    create_batches,
    find_authority_by_001,
    trigger_load_record_dag,
)


@pytest.fixture
def mock_folio_client(mocker):
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
    return mock_client


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
    test_001s_df = pd.DataFrame(
        [{"001": "n\\79000500\\"}, {"001": "n 79045242"}, {"001": "n79044348"}]
    )

    uploads_path = tmp_path / "authorities/uploads"
    uploads_path.mkdir(parents=True)

    test_csv_path = uploads_path / "test.csv"

    test_001s_df.to_csv(test_csv_path)

    updated_csv_file = clean_csv_file(airflow=tmp_path, file="test.csv")
    assert updated_csv_file.endswith("-test.csv")

    updated_001s_df = pd.read_csv(updated_csv_file)

    updated_001s = updated_001s_df.to_dict(orient='records')

    assert len(updated_001s) == 3
    assert updated_001s[0]['001'] == "n79000500"
    assert updated_001s[1]['001'] == "n79045242"


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
