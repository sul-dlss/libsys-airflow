import logging
import pathlib

import pandas as pd

logger = logging.getLogger(__name__)


def _apply_transforms(df, column_transforms):
    for transform in column_transforms:
        column = transform[0]
        if column in df:
            function = transform[1]
            df[column] = df[column].apply(function)
    return df


def _merge_notes(note_path: pathlib.Path):
    notes_df = pd.read_csv(note_path, sep="\t", dtype=object)

    if len(notes_df) < 1:
        logging.info(f"{notes_df} is empty")
        return

    match note_path.name.split(".")[-2]:
        case "circnote":
            column_name = "CIRCNOTE"
            notes_df["NOTE_TYPE"] = column_name

        case "circstaff":
            column_name = "CIRCSTAFF"
            notes_df["NOTE_TYPE"] = "CIRCNOTE"

        case "hvshelfloc":
            column_name = "HVSHELFLOC"
            notes_df["NOTE_TYPE"] = column_name

        case "public":
            column_name = "PUBLIC"
            notes_df["NOTE_TYPE"] = column_name

        case "techstaff":
            column_name = "TECHSTAFF"
            notes_df["NOTE_TYPE"] = column_name

    notes_df = notes_df.rename(columns={column_name: "note"})

    notes_df["BARCODE"] = notes_df["BARCODE"].apply(
        lambda x: x.strip() if isinstance(x, str) else x
    )

    return notes_df


def _processes_tsv(**kwargs):
    tsv_base = kwargs["tsv_base"]
    tsv_notes = kwargs["tsv_notes"]
    airflow = kwargs["airflow"]
    column_transforms = kwargs["column_transforms"]
    dag_run_id = kwargs["dag_run_id"]

    items_dir = pathlib.Path(
        f"{airflow}/migration/iterations/{dag_run_id}/source_data/items/"
    )

    tsv_base_df = pd.read_csv(tsv_base, sep="\t", dtype=object)
    tsv_base_df = _apply_transforms(tsv_base_df, column_transforms)
    new_tsv_base_path = items_dir / tsv_base.name

    tsv_base_df.to_csv(new_tsv_base_path, sep="\t", index=False)

    all_notes = []
    # Iterate on tsv notes and merge into the tsv_notes DF
    for tsv_note_path in tsv_notes:
        logging.info(f"Starting merge of {tsv_note_path}")
        note_df = _merge_notes(tsv_note_path)
        if note_df is not None:
            all_notes.append(note_df)
            logging.info(f"Merged {len(note_df)} notes into items tsv")
        tsv_note_path.unlink()

    if len(all_notes) > 0:
        tsv_notes_df = pd.concat(all_notes)
        tsv_notes_name_parts = tsv_base.name.split(".")
        tsv_notes_name_parts.insert(-1, "notes")

        tsv_notes_name = ".".join(tsv_notes_name_parts)

        new_tsv_notes_path = pathlib.Path(
            f"{airflow}/migration/iterations/{dag_run_id}/source_data/items/{tsv_notes_name}"
        )

        tsv_notes_df.to_csv(new_tsv_notes_path, sep="\t", index=False)

        return new_tsv_notes_path


unwanted_item_cat1 = [
    "BUSCORPRPT",
    "BW-CHILD",
    "BW-PARENT",
    "CATEVAL",
    "EEM",
    "M-MARCADIA",
    "PC-FALLSEM",
    "PC-FQTR",
    "PC-F-SPSEM",
    "PC-FWSQTRS"
    "PC-PERM",
    "PC-SPQTR",
    "PC-SPSEM",
    "PC-SUQTR",
    "PC-WQTR",
    "RECYCLE",
    "SALPROB1",
    "SALPROB2",
    "TEAMS"
]


def transform_move_tsvs(*args, **kwargs):
    airflow = kwargs.get("airflow", "/opt/airflow")
    dag = kwargs["dag_run"]
    column_transforms = kwargs.get("column_transforms", [])
    task_instance = kwargs["task_instance"]

    tsv_notes_files = task_instance.xcom_pull(
        task_ids="bib-files-group", key="tsv-files"
    )
    tsv_base_file = task_instance.xcom_pull(task_ids="bib-files-group", key="tsv-base")

    tsv_notes = [pathlib.Path(filename) for filename in tsv_notes_files]
    tsv_base = pathlib.Path(tsv_base_file)

    notes_path = _processes_tsv(
        tsv_base=tsv_base,
        tsv_notes=tsv_notes,
        airflow=airflow,
        column_transforms=column_transforms,
        dag_run_id=dag.run_id,
    )

    # Delete tsv base
    tsv_base.unlink()

    if notes_path is not None:
        task_instance.xcom_push(key="tsv-notes", value=str(notes_path))

    return tsv_base.name
