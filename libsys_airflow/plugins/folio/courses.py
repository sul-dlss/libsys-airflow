import logging

from s3path import S3Path
from pathlib import Path

from airflow.decorators import task
from airflow.models import Variable
from airflow.exceptions import AirflowException

from attrs import define
from cattrs import Converter
import csv

from folioclient import FolioClient

logger = logging.getLogger(__name__)


@define
class Term:
    id: str
    name: str

    @property
    def term_name(self) -> str:
        name = self.name
        name = (
            name.replace(" ", "")
            .replace("20", "")
            .replace("SUL", "")
            .replace("Law", "")
            .replace("Lane", "")
            .replace("Fall", "F")
            .replace("Winter", "W")
            .replace("Spring", "Sp")
            .replace("Summer", "Su")
        )
        return name


@define
class CourseListing:
    id: str
    termId: str
    termObject: Term


@define
class Course:
    id: str
    courseNumber: str
    courseListingObject: CourseListing

    @property
    def course_number(self) -> str:
        cn = self.courseNumber
        cn = cn.upper().replace(" ", "-")
        return cn


def models_converter():
    converter = Converter()
    return converter


def _folio_client():
    try:
        return FolioClient(
            Variable.get("OKAPI_URL"),
            "sul",
            Variable.get("FOLIO_USER"),
            Variable.get("FOLIO_PASSWORD"),
        )
    except ValueError as error:
        logger.error(error)
        raise


def _term_names(logical_date) -> list:
    month = logical_date.month
    year = logical_date.year
    match month:
        case 1 | 2 | 3:
            return [f"Winter {year}", f"Spring {year}"]
        case 4 | 5 | 6:
            return [f"Spring {year}", f"Summer {year}"]
        case 7 | 8 | 9:
            return [f"Summer {year}", f"Fall {year}"]
        case _:
            return [f"Fall {year}", f"Winter {year + 1}"]


def _term_ids(folio_query: str, folio_client: FolioClient) -> list:
    """
    Returns all term Ids given query parameter
    """
    terms = folio_client.folio_get_all(
        "/coursereserves/terms", key="terms", query=folio_query, limit=500
    )
    return [row.get("id") for row in terms]


def _courses(folio_query: str, folio_client: FolioClient) -> list:
    """
    Returns all courses given query parameter
    """
    courses = folio_client.folio_get_all(
        "/coursereserves/courses", key="courses", query=folio_query, limit=500
    )
    return [row for row in courses]


def _transform_courses(course: dict, converter: Converter) -> Course:
    return converter.structure(course, Course)


@task
def current_next_term_ids(**kwargs) -> list:
    """
    Get term UUIDs with name Fall/Winter/Spring/Summer YYYY
    based on airflow DAG run's logical date month and year
    """
    folio_client = _folio_client()
    dag_run = kwargs["dag_run"]
    dag_run_date = dag_run.logical_date
    term_names = _term_names(dag_run_date)
    query = f"""?query=(name all "{term_names[0]}") or (name all "{term_names[1]}")"""
    logger.info(f"Getting term UUIDs for {term_names[0]} and {term_names[1]}.")

    term_ids = _term_ids(query, folio_client)
    if len(term_ids) == 0:
        logger.warning(
            f"No terms with term names {term_names[0]} and {term_names[1]} in FOLIO. Downstream tasks will be skiped."
        )
    return term_ids


@task(max_active_tis_per_dag=5)
def generate_course_reserves_data(term_id: str) -> dict:
    """
    Retrieves courses with course listing term ID
    and transforms to format for Canvas course reserves
    """
    folio_client = _folio_client()
    query = f"""?query=courseListing.termId=={term_id}"""
    logger.info(f"Getting courses for term {term_id}.")
    courses = _courses(query, folio_client)
    course_data: list = []
    converter = models_converter()
    for row in courses:
        course_object = _transform_courses(row, converter)
        id = course_object.id
        term_course_num = f"{course_object.courseListingObject.termObject.term_name}-{course_object.course_number}"
        url = f"https://searchworks.stanford.edu/catalog?f%5Bcourses_folio_id_ssim%5D%5B%5D={id}"
        course_data.append([term_course_num, url])

    return {term_id: course_data}


@task
def generate_course_reserves_file(course_data: dict, airflow="/opt/airflow") -> str:
    course_reserves_dir = Path(airflow) / "data-export-files/course-reserves"
    course_reserves_dir.mkdir(exist_ok=True, parents=True)
    course_reserves_file = course_reserves_dir / "course-reserves.tsv"
    with course_reserves_file.open("w") as fo:
        logger.info(f"Empty file {str(course_reserves_file)} created.")
        fo.close()

    for row in course_data:
        for term_id, data in row.items():
            if len(data) > 1:
                logger.info(
                    f"Writing to course reserves file {str(course_reserves_file)}"
                )
                with course_reserves_file.open("a", newline="") as fo:
                    filewriter = csv.writer(fo, delimiter="|")
                    filewriter.writerows(data)

            else:
                logger.warning(f"No new course data for term {term_id}.")

    return str(course_reserves_file)


@task
def upload_to_s3(file: str) -> bool:
    bucket = Variable.get("FOLIO_AWS_BUCKET", "folio-data-export-prod")
    s3_dir = S3Path(f"/{bucket}/data-export-files/course-reserves")
    s3_dir.mkdir(exist_ok=True, parents=True)
    s3_file_path = s3_dir / "course-reserves.tsv"
    local_path = Path(file)
    try:
        S3Path(s3_file_path).write_text(local_path.read_text())
        return True
    except Exception as e:
        logger.warning(e)
        raise AirflowException("Failed to upload file to S3 bucket.")
