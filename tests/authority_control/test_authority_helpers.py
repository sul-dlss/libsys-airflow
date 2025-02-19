import pymarc
import pytest  # noqa


from libsys_airflow.plugins.authority_control.helpers import (
    clean_up,
    create_batches,
    trigger_load_record_dag,
)


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

    assert len(batches) == 2
    assert (tmp_path / "authorities/batch_1.mrc").exists()
    assert (tmp_path / "authorities/batch_2.mrc").exists()


def test_trigger_load_record_dag(mocker):
    operator = trigger_load_record_dag("authority.mrc", "authority_profile")
    assert operator.task_id == "trigger_load_record_dag"
