import pandas as pd

import pymarc
import pytest  # noqa


from libsys_airflow.plugins.authority_control.helpers import (
    clean_csv_file,
    clean_up,
    create_batches,
    trigger_load_record_dag,
)


def test_clean_csv_file(tmp_path):
    test_001s_df = pd.DataFrame(
        [{"001": "n\\79000500\\"}, {"001": "n 79045242"}, {"001": "n79044348"}]
    )

    uploads_path = tmp_path / "authorities/uploads"
    uploads_path.mkdir(parents=True)

    test_csv_path = uploads_path / "test.csv"

    test_001s_df.to_csv(test_csv_path)

    updated_csv_file = clean_csv_file(airflow=tmp_path, file="test.csv")

    assert updated_csv_file.endswith("updated-test.csv")

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


def test_trigger_load_record_dag(mocker):
    operator = trigger_load_record_dag("authority.mrc", "authority_profile")
    assert operator.task_id == "trigger_load_record_dag"
