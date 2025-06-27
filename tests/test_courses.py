import pytest  # noqa
import csv

from datetime import datetime
from unittest.mock import MagicMock

from libsys_airflow.plugins.folio.courses import (
    _term_names,
    _transform_courses,
    _courses,
    models_converter,
    Course,
    current_next_term_ids,
    generate_course_reserves_data,
    generate_course_reserves_file,
)


terms = [
    {
        "id": "295843bd-fe5a-4c53-a9ce-e96a8cd7bcc8",
        "name": "Law Fall 2024",
    },
    {
        "id": "21798258-dae5-4455-b02b-666ac3a78302",
        "name": "Lane Fall 2024",
    },
    {
        "id": "cd1256a7-c1c9-4296-8e04-808992109c76",
        "name": "SUL Fall 2024",
    },
    {
        "id": "96cd02b3-82ab-45c5-b320-5eec5a2873e1",
        "name": "Law Winter 2025",
    },
    {
        "id": "f3fffd89-5646-49bf-a3e6-631851b794c3",
        "name": "Lane Winter 2025",
    },
    {
        "id": "bf5eae19-db90-44ae-aca6-216f8191417f",
        "name": "SUL Winter 2025",
    },
]

sul_course_dict = {
    "id": "ab9e0fd0-f835-4082-8b9b-f29d19228507",
    "name": "The Global Mughal World",
    "departmentId": "db91179f-b20c-4756-a2f8-15c3eb2eaf59",
    "departmentObject": {
        "id": "db91179f-b20c-4756-a2f8-15c3eb2eaf59",
        "name": "ARTHIST - Art & Art History",
    },
    "courseListingId": "55ea7b87-ae3d-49c3-a703-def12d46c249",
    "courseListingObject": {
        "id": "55ea7b87-ae3d-49c3-a703-def12d46c249",
        "locationId": "198f1e45-948d-4d30-9437-f93240beb659",
        "locationObject": {
            "id": "198f1e45-948d-4d30-9437-f93240beb659",
            "name": "Art Reserves",
            "code": "ART-CRES",
            "discoveryDisplayName": "On reserve: Ask at Art circulation desk ",
            "isActive": True,
        },
        "termId": "cd1256a7-c1c9-4296-8e04-808992109c76",
        "termObject": {
            "id": "cd1256a7-c1c9-4296-8e04-808992109c76",
            "name": "SUL Fall 2024",
            "startDate": "2024-09-16T07:00:00.000Z",
            "endDate": "2024-12-13T08:00:00.000Z",
        },
        "courseTypeId": "39b137f2-2259-4b5d-b3b7-996b2719c5be",
        "courseTypeObject": {
            "id": "39b137f2-2259-4b5d-b3b7-996b2719c5be",
            "name": "In Person",
        },
        "instructorObjects": [
            {
                "id": "a52a918b-752e-42ef-b483-ca7c490885e2",
                "userId": "228a461b-7100-4fc2-98c4-e975e8d26726",
                "courseListingId": "55ea7b87-ae3d-49c3-a703-def12d46c249",
            }
        ],
    },
    "courseNumber": "ARTHIST-123-01",
}

lane_course_dict = {
    "id": "97667743-6b10-4ad8-9821-d3a2c89e4de0",
    "name": "Basic Cardiac Life Support for Healthcare Professionals",
    "departmentId": "4cc80c84-aeed-4213-9499-0e19af204128",
    "departmentObject": {},
    "courseListingId": "520f37d5-c12e-4d48-9c58-0ee103881fc3",
    "courseListingObject": {
        "id": "520f37d5-c12e-4d48-9c58-0ee103881fc3",
        "locationId": "4952c89e-98ac-4601-a1ec-7dcbe1cfa4b4",
        "locationObject": {},
        "termId": "21798258-dae5-4455-b02b-666ac3a78302",
        "termObject": {
            "id": "21798258-dae5-4455-b02b-666ac3a78302",
            "name": "Lane Fall 2024",
            "startDate": "2024-08-15T07:00:00.000Z",
            "endDate": "2024-12-13T08:00:00.000Z",
        },
        "courseTypeId": "39b137f2-2259-4b5d-b3b7-996b2719c5be",
        "courseTypeObject": {},
        "instructorObjects": [],
    },
    "courseNumber": "EMED 201",
}

courses = [sul_course_dict, lane_course_dict]


@pytest.fixture
def mock_dag_run(mocker):
    dag_run = mocker.stub(name="dag_run")
    dag_run.run_id = "scheduled__2024-10-23"
    dag_run.logical_date = datetime.strptime(
        "2024-10-23T09:00:00+00:00", "%Y-%m-%dT%H:%M:%S%z"
    )

    return dag_run


@pytest.fixture
def mock_failed_dag_run(mocker):
    dag_run = mocker.stub(name="dag_run")
    dag_run.run_id = "scheduled__2025-06-23"
    dag_run.logical_date = datetime.strptime(
        "2025-06-23T09:00:00+00:00", "%Y-%m-%dT%H:%M:%S%z"
    )

    return dag_run


@pytest.fixture
def mock_folio_client():
    def mock_get_all(*args, **kwargs):
        # Terms
        if args[0].startswith("/coursereserves/terms"):
            if kwargs["query"].startswith(
                '?query=(name all "Fall 2024") or (name all "Winter 2025")'
            ):
                return terms
            else:
                return []

        # Courses
        if args[0].startswith("/coursereserves/courses"):
            if (
                kwargs["query"]
                == '?query=courseListing.termId==cd1256a7-c1c9-4296-8e04-808992109c76'
            ):
                return [sul_course_dict]
            else:
                return courses

    mock_client = MagicMock()
    mock_client.folio_get_all = mock_get_all
    return mock_client


def test_term_names():
    term_names = _term_names(
        datetime.strptime("2025-06-23T09:00:00+00:00", "%Y-%m-%dT%H:%M:%S%z")
    )
    assert term_names[0] == "Spring 2025"
    assert term_names[1] == "Summer 2025"


def test_current_next_term_ids(mocker, mock_folio_client, mock_dag_run, caplog):
    mocker.patch(
        "libsys_airflow.plugins.folio.courses._folio_client",
        return_value=mock_folio_client,
    )
    term_ids = current_next_term_ids.function(dag_run=mock_dag_run)
    assert len(term_ids) == 6
    assert term_ids[0] == "295843bd-fe5a-4c53-a9ce-e96a8cd7bcc8"
    assert "Getting term UUIDs for Fall 2024 and Winter 2025." in caplog.text


def test_no_term_ids(mocker, mock_folio_client, mock_failed_dag_run, caplog):
    mocker.patch(
        "libsys_airflow.plugins.folio.courses._folio_client",
        return_value=mock_folio_client,
    )
    term_ids = current_next_term_ids.function(dag_run=mock_failed_dag_run)
    assert len(term_ids) == 0
    assert "Getting term UUIDs for Spring 2025 and Summer 2025." in caplog.text
    assert (
        "No terms with term names Spring 2025 and Summer 2025 in FOLIO. Downstream tasks will be skiped."
        in caplog.text
    )


def test_courses(mocker, mock_folio_client):
    mocker.patch(
        "libsys_airflow.plugins.folio.courses._folio_client",
        return_value=mock_folio_client,
    )
    query = "?query=courseListing.termId==cd1256a7-c1c9-4296-8e04-808992109c76"
    courses = _courses(query, mock_folio_client)
    assert len(courses) == 1


def test_generate_course_reserves_data(mocker, mock_folio_client):
    mocker.patch(
        "libsys_airflow.plugins.folio.courses._folio_client",
        return_value=mock_folio_client,
    )
    course_data = generate_course_reserves_data.function(term_id="abc-123")
    file_data = course_data["data"]
    assert file_data[0][0] == "F24-ARTHIST-123-01"
    assert file_data[0][1].endswith("ab9e0fd0-f835-4082-8b9b-f29d19228507")
    assert file_data[1][0] == "F24-EMED-201"
    assert file_data[1][1].endswith("97667743-6b10-4ad8-9821-d3a2c89e4de0")


def test_transform_courses():
    converter = models_converter()
    course_object = _transform_courses(sul_course_dict, converter)
    assert isinstance(course_object, Course)
    assert course_object.course_number == "ARTHIST-123-01"
    assert course_object.courseListingObject.termObject.term_name == "F24"
    another_course_object = _transform_courses(lane_course_dict, converter)
    assert isinstance(another_course_object, Course)
    assert another_course_object.course_number == "EMED-201"


def test_generate_course_reserves_file(mocker, tmp_path):
    file_data = {
        "data": [
            [
                "F24-ARTHIST-123-01",
                "https://searchworks.stanford.edu/catalog?f%5Bcourses_folio_id_ssim%5D%5B%5D=ab9e0fd0-f835-4082-8b9b-f29d19228507",
            ],
            [
                "F24-EMED-201",
                "https://searchworks.stanford.edu/catalog?f%5Bcourses_folio_id_ssim%5D%5B%5D=97667743-6b10-4ad8-9821-d3a2c89e4de0",
            ],
        ]
    }
    mocker.patch("libsys_airflow.plugins.folio.courses.S3Path", return_value=tmp_path)
    save_path = generate_course_reserves_file.function(file_data)
    assert save_path == f"{tmp_path}/course-reserves.tsv"
    saved_data = []
    with open(save_path, "r", newline="") as tsvfile:
        reader = csv.reader(tsvfile, delimiter="|")

        for row in reader:
            saved_data.append(row)

    assert saved_data[0][0] == "F24-ARTHIST-123-01"
    assert saved_data[0][1].endswith("ab9e0fd0-f835-4082-8b9b-f29d19228507")
    assert saved_data[1][0] == "F24-EMED-201"
    assert saved_data[1][1].endswith("97667743-6b10-4ad8-9821-d3a2c89e4de0")
