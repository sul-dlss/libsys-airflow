import pytest
import requests

from pytest_mock import MockerFixture

from mocks import (  # noqa
    mock_dag_run,
    mock_file_system,
    MockFOLIOClient,
    MockTaskInstance
)

import mocks

from libsys_airflow.plugins.folio.circ_rules import (
    friendly_report,
    friendly_batch_report,
    generate_report,
    generate_batch_report,
    generate_urls,
    generate_batch_urls,
    policy_report,
    policy_batch_report,
    retrieve_policies,
    retrieve_batch_policies,
    setup_rules,
    setup_batch_rules,
)

# Mock xcom messages dict
setup_circ_rules = {
    "setup-circ-rules": {
        "patron_group_id": "503a81cd-6c26-400f-b620-14c08943697c",
        "loan_type_id": "2b94c631-fca9-4892-a730-03ee529ffe27",
        "material_type_id": "fd6c6515-d470-4561-9c32-3e3290d4ca98",
        "location_id": "758258bc-ecc1-41b8-abca-f7b610822ffd",
    }
}

setup_batch_circ_rules = {
    "setup-circ-rules": {
        "total": 3,
        "material_type_id0": "dd0bf600-dbd9-44ab-9ff2-e2a61a6539f1",
        "material_type_id1": "1a54b431-2e4f-452d-9cae-9cee66c9a892",
        "material_type_id2": "fd6c6515-d470-4561-9c32-3e3290d4ca98",
        "loan_type_id0": "a1dc1ce3-d56f-4d8a-b498-d5d674ccc845",
        "loan_type_id1": "e8b311a6-3b21-43f2-a269-dd9310cb2d0e",
        "loan_type_id2": "2b94c631-fca9-4892-a730-03ee529ffe27",
        "location_id0": "53cf956f-c1df-410b-8bea-27f712cca7c0",
        "location_id1": "fcd64ce1-6995-48f0-840e-89ffa2288371",
        "location_id2": "fcd64ce1-6995-48f0-840e-89ffa2288371",
        "location_id3": "b241764c-1466-4e1d-a028-1a3684a5da87",
        "patron_group_id0": "503a81cd-6c26-400f-b620-14c08943697c",
        "patron_group_id1": "bdc2b6d4-5ceb-4a12-ab46-249b9a68473e",
        "patron_group_id2": "ad0bc554-d5bc-463c-85d1-5562127ae91b",
    }
}

mocks.messages = setup_circ_rules.copy()


@pytest.fixture
def mock_requests(monkeypatch, mocker: MockerFixture):  # noqa
    def mock_group():
        return {"usergroups": [{"group": "faculty"}]}

    def mock_loan_types():
        return {"loantypes": [{"name": "4-hour loan type"}]}

    def mock_material_types():
        return {"mtypes": [{"name": "sound recording"}]}

    def mock_locations():
        return {
            "locations": [
                {
                    "name": "Green Library",
                    "libraryId": "5d78803e-ca04-4b4a-aeae-2c63b924518b",
                }
            ]
        }

    def mock_location_units():
        return {"loclibs": [{"code": "BUSINESS"}]}

    def mock_overdue_fine_policy():
        return """{ "overdueFinePolicyId" : "c39fa210-ed40-48f4-bde5-82177b95bfb9", "appliedRuleConditions" : { "materialTypeMatch" : false, "loanTypeMatch" : false, "patronGroupMatch" : false } }"""

    def mock_overdue_fine_policy_all():
        return """{ "circulationRuleMatches" : [ { "overduePolicyId" : "c39fa210-ed40-48f4-bde5-82177b95bfb9", "circulationRuleLine" : 132 }, { "overduePolicyId" : "814ffb76-82d0-49a6-8391-d801b7b34541", "circulationRuleLine" : 2 } ] }"""

    def mock_request_policy():
        return {
            "requestPolicies": [
                {
                    "id": "8a58b9d6-855d-49bb-9a16-8b409e590dfe",
                    "name": "No requests allowed",
                },
                {"id": "1a376854-636f-44e6-8405-65987ea90e95", "name": "Allow paging"},
            ]
        }

    def mock_get(*args, **kwargs):
        get_response = mocker.stub(name="get_result")
        get_response.status_code = 200
        url = args[0]
        json_func = lambda: {}  # noqa
        if "groups" in url:
            json_func = mock_group
        if "loan-types" in url:
            json_func = mock_loan_types
        if "material-types" in url:
            json_func = mock_material_types
        if "locations" in url:
            json_func = mock_locations
        if "location-units" in url:
            json_func = mock_location_units
        if "request-policy-storage" in url:
            json_func = mock_request_policy
        if "overdue-fine-policy" in url:
            get_response.text = mock_overdue_fine_policy()
        if "overdue-fine-policy-all" in url:
            get_response.text = mock_overdue_fine_policy_all()

        get_response.json = json_func
        return get_response

    monkeypatch.setattr(requests, "get", mock_get)


@pytest.fixture
def mock_valid_current_context(monkeypatch, mocker: MockerFixture):
    def mock_get_current_context():
        context = mocker.stub(name="context")
        context.get = lambda arg: {
            "material_type_id": "fd6c6515-d470-4561-9c32-3e3290d4ca98",
            "loan_type_id": "2b94c631-fca9-4892-a730-03ee529ffe27",
            "location_id": "fcd64ce1-6995-48f0-840e-89ffa2288371",
            "patron_group_id": "ad0bc554-d5bc-463c-85d1-5562127ae91b",
        }
        return context

    monkeypatch.setattr(
        "libsys_airflow.plugins.folio.circ_rules.get_current_context", mock_get_current_context
    )


@pytest.fixture
def mock_missing_current_context(monkeypatch, mocker: MockerFixture):
    def mock_get_current_context():
        context = mocker.stub(name="context")
        context.get = lambda arg: {
            "material_type_id": None,
            "loan_type_id": "2b94c631-fca9-4892-a730-03ee529ffe27",
            "location_id": "fcd64ce1-6995-48f0-840e-89ffa2288371",
            "patron_group_id": "ad0bc554-d5bc-463c-85d1-5562127ae91b",
        }
        return context

    monkeypatch.setattr(
        "libsys_airflow.plugins.folio.circ_rules.get_current_context", mock_get_current_context
    )


@pytest.fixture
def mock_batch_current_context(monkeypatch, mocker: MockerFixture):
    def mock_get_current_context():
        context = mocker.stub(name="context")
        context.get = lambda arg: {
            "scenarios": """{
                "patron_group_id":{"0":"503a81cd-6c26-400f-b620-14c08943697c","1":"bdc2b6d4-5ceb-4a12-ab46-249b9a68473e"},
                "material_type_id":{"0":"dd0bf600-dbd9-44ab-9ff2-e2a61a6539f1","1":"1a54b431-2e4f-452d-9cae-9cee66c9a892"}
            }"""
        }
        return context

    monkeypatch.setattr(
        "libsys_airflow.plugins.folio.circ_rules.get_current_context", mock_get_current_context
    )


def test_friendly_report(mock_dag_run, mock_requests):  # noqa
    mocks.messages = setup_circ_rules.copy()
    task_id = "friendly-report-group.friendly-report"
    task_instance = MockTaskInstance(task_id=task_id)
    friendly_report(folio_client=MockFOLIOClient(), task_instance=task_instance)

    assert mocks.messages[task_id]["patron_group"] == "faculty"
    assert mocks.messages[task_id]["loan_type"] == "4-hour loan type"
    assert mocks.messages[task_id]["material_type"] == "sound recording"
    assert mocks.messages[task_id]["libraryName"] == "Green Library"
    assert mocks.messages[task_id]["location"] == "BUSINESS"
    mocks.messages = {}


def test_friendly_batch_report(mock_dag_run, mock_requests):  # noqa
    # Setup xcom messages for batch testing
    mocks.messages = setup_batch_circ_rules.copy()

    task_id = "friendly-report-group.friendly-report"
    friendly_batch_report(
        folio_client=MockFOLIOClient(), task_instance=MockTaskInstance(task_id=task_id)
    )

    assert mocks.messages[task_id]["patron_group0"] == "faculty"
    assert mocks.messages[task_id]["material_type2"] == "sound recording"
    assert mocks.messages[task_id]["libraryName2"] == "Green Library"

    # Resets xcom messages for remaining tests
    mocks.messages = {}


def test_generate_report(mock_dag_run, tmp_path):  # noqa
    mocks.messages = setup_batch_circ_rules.copy()

    mocks.messages["friendly-report-group.friendly-report"] = {"libraryName": "Art"}

    mocks.messages["friendly-report-group.loan-policy-test"] = {
        "winning-policy": "Winning policy is No loan - 2"
    }

    report_filepath = tmp_path / f"{mock_dag_run.run_id}.json"
    task_instance = MockTaskInstance(task_id="generate-final-report")

    record = generate_report(
        dag_run=mock_dag_run, task_instance=task_instance, circ_dir=tmp_path
    )
    assert report_filepath.exists()
    assert record["Library Name"] == "Art"
    assert record["loan"] == "Winning policy is No loan - 2"
    mocks.messages = setup_circ_rules.copy()


def test_generate_batch_report(mock_dag_run, tmp_path, caplog):  # noqa
    mocks.messages = setup_batch_circ_rules.copy()

    generate_batch_report(
        task_instance=MockTaskInstance(task_id="generate-final-report"),
        dag_run=mock_dag_run,
        circ_dir=tmp_path,
    )

    assert "Finished Generating Batch Report for 3 batches" in caplog.text
    mocks.messages = setup_circ_rules.copy()


def test_generate_urls():
    task_id = "retrieve-policies-group.loan-generate-urls"
    task_instance = MockTaskInstance(task_id=task_id)
    generate_urls(
        task_instance=task_instance, folio_client=MockFOLIOClient(), policy_type="loan"
    )
    assert (
        mocks.messages[task_id]["single-policy-url"]
        == "https://okapi.edu/circulation/rules/loan-policy?patron_type_id=503a81cd-6c26-400f-b620-14c08943697c&loan_type_id=2b94c631-fca9-4892-a730-03ee529ffe27&item_type_id=fd6c6515-d470-4561-9c32-3e3290d4ca98&location_id=758258bc-ecc1-41b8-abca-f7b610822ffd&limit=2000"
    )
    assert (
        mocks.messages[task_id]["all-policies-url"]
        == "https://okapi.edu/circulation/rules/loan-policy-all?patron_type_id=503a81cd-6c26-400f-b620-14c08943697c&loan_type_id=2b94c631-fca9-4892-a730-03ee529ffe27&item_type_id=fd6c6515-d470-4561-9c32-3e3290d4ca98&location_id=758258bc-ecc1-41b8-abca-f7b610822ffd&limit=2000"
    )

    mocks.messages = setup_circ_rules.copy()


def test_generate_batch_urls(caplog):
    mocks.messages = setup_batch_circ_rules.copy()
    task_id = "retrieve-policies-group.loan-generate-urls"
    generate_batch_urls(
        task_instance=MockTaskInstance(task_id=task_id),
        folio_client=MockFOLIOClient(),
        policy_type="loan",
    )

    assert "Finished generating urls for 3 batches" in caplog.text
    assert "all-policies-url1" in mocks.messages[task_id]
    assert "single-policy-url2" in mocks.messages[task_id]

    mocks.messages = setup_circ_rules.copy()


def test_policy_report(mock_requests):
    task_id = "friendly-report-group.request-policy-test"
    mocks.messages = setup_circ_rules.copy()
    mocks.messages["retrieve-policies-group.request-get-policies"] = {
        "single-policy": """{
            "requestPolicyId" : "8a58b9d6-855d-49bb-9a16-8b409e590dfe",
            "appliedRuleConditions" : {
                "materialTypeMatch" : false,
                "loanTypeMatch" : false,
                "patronGroupMatch" : false
            }
        }""",
        "all-policies": """{
            "circulationRuleMatches" : [
                {
                    "requestPolicyId" : "8a58b9d6-855d-49bb-9a16-8b409e590dfe",
                    "circulationRuleLine" : 132
                },
                { "requestPolicyId" : "8a58b9d6-855d-49bb-9a16-8b409e590dfe",
                  "circulationRuleLine" : 2 }
            ]
        }""",
    }

    task_instance = MockTaskInstance(task_id=task_id)
    policy_report(
        task_instance=task_instance,
        folio_client=MockFOLIOClient(),
        policy_type="request",
    )
    assert (
        mocks.messages[task_id]["winning-policy"]
        == "Winning policy is No requests allowed - 132"
    )
    assert len(mocks.messages[task_id]["losing-policies"]) == 0
    mocks.messages = setup_circ_rules.copy()


def test_policy_batch_report(mock_requests, caplog):
    mocks.messages = setup_batch_circ_rules.copy()
    mocks.messages["retrieve-policies-group.loan-get-policies"] = {
        "all-policies0": """{ "circulationRuleMatches": [] }""",
        "all-policies1": """{ "circulationRuleMatches": [] }""",
        "all-policies2": """{ "circulationRuleMatches": [] }""",
        "single-policy0": """{}""",
        "single-policy1": """{}""",
        "single-policy2": """{}""",
    }
    task_id = "friendly-report-group.request-policy-test"

    policy_batch_report(
        task_instance=MockTaskInstance(task_id=task_id),
        folio_client=MockFOLIOClient(),
        policy_type="loan",
    )

    assert "Finished generating policy reports for 3 batches" in caplog.text
    mocks.messages = {}


def test_retrieve_policies(mock_requests):
    mocks.messages = setup_batch_circ_rules.copy()

    mocks.messages["retrieve-policies-group.overdue-fine-generate-urls"] = {
        "single-policy-url": "https://okapi.edu/circulation/rules/overdue-fine-policy?patron_type_id=503a81cd-6c26-400f-b620-14c08943697c&loan_type_id=2b94c631-fca9-4892-a730-03ee529ffe27&item_type_id=fd6c6515-d470-4561-9c32-3e3290d4ca98&location_id=758258bc-ecc1-41b8-abca-f7b610822ffd",
        "all-policies-url": "https://okapi.edu/circulation/rules/overdue-fine-policy-all?patron_type_id=503a81cd-6c26-400f-b620-14c08943697c&loan_type_id=2b94c631-fca9-4892-a730-03ee529ffe27&item_type_id=fd6c6515-d470-4561-9c32-3e3290d4ca98&location_id=758258bc-ecc1-41b8-abca-f7b610822ffd",
    }

    task_id = "retrieve-policies-group.overdue-fine-get-policies"

    retrieve_policies(
        task_instance=MockTaskInstance(task_id=task_id),
        policy_type="overdue-fine",
        folio_client=MockFOLIOClient(),
    )

    assert "overdueFinePolicyId" in mocks.messages[task_id]["single-policy"]
    assert "overduePolicyId" in mocks.messages[task_id]["all-policies"]

    mocks.messages = setup_circ_rules.copy()


def test_retrieve_batch_policies(mock_requests, caplog):

    mocks.messages = setup_batch_circ_rules.copy()
    task_id = "retrieve-policies-group.overdue-fine-get-policies"

    mocks.messages["retrieve-policies-group.overdue-fine-generate-urls"] = {
        "all-policies-url0": "http://okapi/overdue-fine-policy-all/0",
        "all-policies-url1": "http://okapi/overdue-fine-policy-all/1",
        "all-policies-url2": "http://okapi/overdue-fine-policy-all/2",
        "single-policy-url0": "http://okapi/overdue-fine-policy/0",
        "single-policy-url1": "http://okapi/overdue-fine-policy/1",
        "single-policy-url2": "http://okapi/overdue-fine-policy/2",
    }

    retrieve_batch_policies(
        task_instance=MockTaskInstance(task_id=task_id),
        policy_type="overdue-fine",
        folio_client=MockFOLIOClient(),
    )

    assert "Finished retrieving policies for 3 batches" in caplog.text

    mocks.messages = setup_circ_rules.copy()


def test_setup_rules(mock_valid_current_context):
    mocks.messages = {}
    task_id = "setup-circ-rules"
    task_instance = MockTaskInstance(task_id=task_id)
    setup_rules(
        task_instance=task_instance,
    )

    assert len(mocks.messages[task_id]) == 4
    assert mocks.messages[task_id]["location_id"] == "fcd64ce1-6995-48f0-840e-89ffa2288371"

    mocks.messages = {}


def test_missing_setup_rules(mock_missing_current_context):
    task_id = "setup-circ-rules"
    task_instance = MockTaskInstance(task_id=task_id)
    with pytest.raises(ValueError, match="material_type_id value is None"):
        setup_rules(task_instance=task_instance)


def test_setup_batch_rules(mock_batch_current_context, caplog):
    task_id = "setup-circ-rules"
    task_instance = MockTaskInstance(task_id=task_id)
    setup_batch_rules(task_instance=task_instance)

    assert "Setup completed for 2 batches" in caplog.text


# Ensures messages are empty after finishing all tests
mocks.messages = {}
