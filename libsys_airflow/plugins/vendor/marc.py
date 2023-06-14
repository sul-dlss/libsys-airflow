import logging
import pathlib
from typing import Optional

import pymarc
import magic
from pydantic import BaseModel, Field

from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from libsys_airflow.plugins.vendor.models import VendorFile, FileStatus
from libsys_airflow.plugins.vendor.file_status import record_status_from_context

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class MarcSubfield(BaseModel):
    code: str
    value: str


class MarcField(BaseModel):
    tag: str
    indicator1: Optional[str] = None
    indicator2: Optional[str] = None
    subfields: list[MarcSubfield] = []


class AddField(MarcField):
    unless: Optional[MarcField]


class ChangeField(BaseModel):
    from_: MarcField = Field(alias='from')
    to: MarcField


def record_processed_filename(context):
    vendor_interface_uuid = context["params"]["vendor_interface_uuid"]
    filename = context["params"]["filename"]
    processed_filename = context['ti'].xcom_pull(key='marc_filename')

    logger.info(f"Recording processed filename for {filename}: {processed_filename}")

    pg_hook = PostgresHook("vendor_loads")
    with Session(pg_hook.get_sqlalchemy_engine()) as session:
        vendor_file = VendorFile.load(vendor_interface_uuid, filename, session)
        vendor_file.processed_filename = processed_filename
        session.commit()


def record_processing_error(context):
    record_status_from_context(context, FileStatus.processing_error)


def record_processed(context):
    record_status_from_context(context, FileStatus.processed)


@task(
    multiple_outputs=True,
    on_success_callback=record_processed_filename,
    on_failure_callback=record_processing_error,
)
def process_marc_task(
    download_path: str,
    filename: str,
    remove_fields: Optional[list[str]] = None,
    change_fields: Optional[list[dict]] = None,
    add_fields: Optional[list[dict]] = None,
) -> dict:
    """
    Applies changes to MARC records.

    change_fields example: [
        { from: { tag: "520" }, to: { tag: "920" },
        { from: { tag: "528", indicator1: "a", indicator2: "b" },  to: { tag: "928", indicator1: "", indicator2: " " }
    ]
    add_fields example: [
            { tag: "910", indicator1: '2', subfields: [{code: "a", value: "MARCit"}] },
            { tag: "590", subfields: [{code: "a", value: "MARCit brief record"}], unless: { tag: "035", subfields: [{code: "a", value: "OCoLC"}]} },
        ]
    """
    marc_path = pathlib.Path(download_path) / filename
    if not is_marc(marc_path):
        logger.info(f"Skipping filtering fields from {marc_path}")
        return {"records_count": 0, "marc_filename": filename}
    change_fields_models = None
    if change_fields:
        change_fields_models = _to_change_fields_models(change_fields)
    add_fields_models = None
    if add_fields:
        add_fields_models = _to_add_fields_models(add_fields)
    return process_marc(
        pathlib.Path(marc_path), remove_fields, change_fields_models, add_fields_models
    )


def process_marc(
    marc_path: pathlib.Path,
    remove_fields: Optional[list[str]] = None,
    change_fields: Optional[list[ChangeField]] = None,
    add_fields: Optional[list[AddField]] = None,
) -> dict:
    logger.info(f"Processing from {marc_path}")
    records = []
    with marc_path.open("rb") as fo:
        reader = _marc_reader(fo, to_unicode=False)
        for record in reader:
            if record is None:
                logger.info(
                    f"Error reading MARC. Current chunk: {reader.current_chunk}. Error: {reader.current_exception}"
                )
            else:
                record = _marc8_to_unicode(record)
                if remove_fields:
                    record.remove_fields(*remove_fields)
                if change_fields:
                    _change_fields(record, change_fields)
                if add_fields:
                    _add_fields(record, add_fields)
                records.append(record)

    new_marc_path = marc_path.with_stem(f"{marc_path.stem}-processed")
    _write_records(records, new_marc_path)

    logger.info(f"Finished processing from {marc_path}")
    return {"records_count": len(records), "marc_filename": new_marc_path.name}


def is_marc(path: pathlib.Path):
    return magic.from_file(str(path), mime=True) == "application/marc"


def _marc_reader(file, to_unicode=True):
    return pymarc.MARCReader(
        file, to_unicode=to_unicode, permissive=True, utf8_handling="replace"
    )


@task(on_failure_callback=record_processing_error, on_success_callback=record_processed)
def batch_task(download_path: str, filename: str) -> list[str]:
    """
    Splits a MARC file into batches.
    """
    marc_path = pathlib.Path(download_path) / filename
    if not is_marc(marc_path):
        logger.info(f"Skipping batching {filename}")
        return [filename]
    max_records = Variable.get("MAX_ENTITIES", 500)
    return batch(download_path, filename, max_records)


def batch(download_path: str, filename: str, max_records: int) -> list[str]:
    """
    Splits a MARC file into batches.
    """
    records = []
    index = 1
    batch_filenames = []
    with open(pathlib.Path(download_path) / filename, "rb") as fo:
        reader = _marc_reader(fo)
        for record in reader:
            if record is None:
                logger.info(
                    f"Error reading MARC. Current chunk: {reader.current_chunk}. Error: {reader.current_exception} "
                )
            else:
                records.append(record)
                if len(records) == max_records:
                    records, index = _new_batch(
                        download_path, filename, index, batch_filenames, records
                    )
    if len(records) > 0:
        _new_batch(download_path, filename, index, batch_filenames, records)
    logger.info(f"Finished batching {filename} into {index} files")
    return batch_filenames


def _new_batch(download_path, filename, index, batch_filenames, records):
    batch_filename = _batch_filename(filename, index)
    batch_filenames.append(batch_filename)
    _write_records(records, pathlib.Path(download_path) / batch_filename)
    return [], index + 1


def _batch_filename(filename, index):
    file_path = pathlib.Path(filename)
    return f"{file_path.stem}_{index}{file_path.suffix}"


def _write_records(records, marc_path):
    logger.info(f"Writing {len(records)} records to {marc_path}")
    with marc_path.open("wb") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for record in records:
            record.force_utf8 = True
            marc_writer.write(record)


def _to_change_fields_models(change_fields):
    return [ChangeField(**change) for change in change_fields]


def _to_add_fields_models(add_fields):
    return [AddField(**add) for add in add_fields]


def _change_fields(record, change_fields):
    for change in change_fields:
        for field in record.get_fields(change.from_.tag):
            if _change_field_match(field, change.from_):
                if field.is_control_field():
                    # This assumes that control fields will be moved to normal fields
                    record.add_ordered_field(
                        pymarc.Field(
                            tag=change.to.tag,
                            indicators=[
                                change.to.indicator1 or " ",
                                change.to.indicator2 or " ",
                            ],
                            subfields=[
                                # Always add value to subfield a.
                                pymarc.Subfield(code='a', value=field.value())
                            ],
                        )
                    )
                    record.remove_field(field)
                else:
                    field.tag = change.to.tag
                    if change.to.indicator1 and change.to.indicator1 != "":
                        field.indicator1 = change.to.indicator1
                    if change.to.indicator2 and change.to.indicator2 != "":
                        field.indicator2 = change.to.indicator2
                    record.remove_field(field)
                    record.add_ordered_field(field)


def _change_field_match(field: pymarc.field.Field, match_field: MarcField):
    if field.tag != match_field.tag:
        return False
    if (
        not field.is_control_field()
        and match_field.indicator1
        and match_field.indicator1 != ""
        and field.indicator1 != match_field.indicator1
    ):
        return False
    if (
        not field.is_control_field()
        and match_field.indicator2
        and match_field.indicator2 != ""
        and field.indicator2 != match_field.indicator2
    ):
        return False
    return True


def _add_fields(record, add_fields):
    for add_field in add_fields:
        if _skip(record, add_field.unless):
            continue
        field = pymarc.field.Field(
            tag=add_field.tag,
            indicators=[add_field.indicator1 or ' ', add_field.indicator2 or ' '],
        )
        for subfield in add_field.subfields:
            field.add_subfield(subfield.code, subfield.value)
        record.add_ordered_field(field)


def _skip(record, unless):
    if unless and _has_matching_field(record, unless):
        return True
    return False


def _has_matching_field(record: pymarc.Record, field: MarcField):
    for check_field in record.get_fields(field.tag):
        if _field_match(field, check_field):
            return True
    return False


def _field_match(field: MarcField, check_field: pymarc.Field):
    if field.indicator1 and check_field.indicators[0] != field.indicator1:
        return False
    if field.indicator2 and check_field.indicators[1] != field.indicator2:
        return False
    for subfield in field.subfields:
        check_subfield_value = ''.join(check_field.get_subfields(subfield.code))
        if not check_subfield_value or check_subfield_value != subfield.value:
            return False
    return True


def _marc8_to_unicode(record: pymarc.Record) -> pymarc.Record:
    """
    Handle MARC records that are encoded as MARC8 but have incorrect bit
    set for utf-8 encoding in leader or have mixed MARC8 and UTF-8 encodings
    in different fields
    """
    modified_leader = record.leader[0:9] + " " + record.leader[10:]
    record.leader = modified_leader
    raw_marc = record.as_marc()
    return pymarc.Record(data=raw_marc, to_unicode=True)
