import json
import logging
import pathlib
import urllib.parse

import requests

from airflow.operators.python import get_current_context
from jsonpath_ng.ext import parse

logger = logging.getLogger(__name__)

# For inconsistent naming and results in Okapi API
policy_lookup = {
    "lost-item": {
        "endpoint": "lost-item-fees-policies",
        "policy_key": "lostItemFeePolicies",
        "key": "lostItemPolicyId",
    },
    "notice": {
        "endpoint": "patron-notice-policy-storage/patron-notice-policies",
        "policy_key": "patronNoticePolicies",
    },
    "overdue-fine": {
        "endpoint": "overdue-fines-policies",
        "policy_key": "overdueFinePolicies",
        "all_policy_key": "overduePolicyId",
        "key": "overdueFinePolicyId",
    },
    "request": {"endpoint": "request-policy-storage/request-policies"},
}

policy_types = ["loan", "request", "notice", "overdue-fine", "lost-item"]


def _friendly_name(**kwargs):
    folio_client = kwargs["folio_client"]
    query = kwargs["query"]
    json_path = kwargs["json_path"]
    fallback = kwargs["fallback"]
    folio_result = requests.get(
        f"{folio_client.okapi_url}/{query}", headers=folio_client.okapi_headers
    )
    expression = parse(json_path)
    matches = expression.find(folio_result.json())
    if len(matches) > 0:
        return matches[0].value
    return fallback


def _library_location_names(**kwargs):
    folio_client = kwargs["folio_client"]
    instance = kwargs["task_instance"]
    location_id = kwargs["location_id"]
    library_location_result = requests.get(
        f"""{folio_client.okapi_url}/locations?query=(id=="{location_id}")""",
        headers=folio_client.okapi_headers,
    )
    library_location_payload = library_location_result.json()
    lib_name_expr = parse("$.locations[0].name")
    lib_name_match = lib_name_expr.find(library_location_payload)
    if len(lib_name_match) > 0:
        library_name = lib_name_match[0].value
    else:
        library_name = "Library not found"

    lib_id_expr = parse("$.locations[0].libraryId")
    lib_id_match = lib_id_expr.find(library_location_payload)
    logger.info(f"Library location payload {library_location_payload}")

    location_code = "Location not found"
    if len(lib_id_match) > 0:
        # Second call to
        library_id = lib_id_match[0].value
        location_units_results = requests.get(
            f"""{folio_client.okapi_url}/location-units/libraries?query=(id=="{library_id}")""",
            headers=folio_client.okapi_headers,
        )

        location_code_expr = parse("$.loclibs[0].code")
        location_code_matches = location_code_expr.find(location_units_results.json())
        if len(location_code_matches) > 0:
            location_code = location_code_matches[0].value

    instance.xcom_push(key="libraryName", value=library_name)
    instance.xcom_push(key="location", value=location_code)


def friendly_report(**kwargs):
    instance = kwargs["task_instance"]
    folio_client = kwargs["folio_client"]
    patron_type_id = instance.xcom_pull(
        task_ids="setup-circ-rules", key="patron_type_id"
    )
    loan_type_id = instance.xcom_pull(task_ids="setup-circ-rules", key="loan_type_id")
    item_type_id = instance.xcom_pull(task_ids="setup-circ-rules", key="item_type_id")
    location_id = instance.xcom_pull(task_ids="setup-circ-rules", key="location_id")

    # Patron Group Name
    instance.xcom_push(
        key="patron_group",
        value=_friendly_name(
            folio_client=folio_client,
            query=f"""groups?query=(id=="{patron_type_id}")""",
            json_path="$.usergroups[0].group",
            fallback="Patron group not found",
        ),
    )
    logger.info("Finished patron_group friendly name")

    # Loan type friendly name
    instance.xcom_push(
        key="loan_type",
        value=_friendly_name(
            folio_client=folio_client,
            query=f"""loan-types?query=(id=="{loan_type_id}")""",
            json_path="$.loantypes[0].name",
            fallback="Loan type not found",
        ),
    )
    logger.info("Finished Loan type friendly name")

    # Material type friendly name (API refers to it as item_type_id)
    instance.xcom_push(
        key="material_type",
        value=_friendly_name(
            folio_client=folio_client,
            query=f"""material-types?query=(id=="{item_type_id}")""",
            json_path="$.mtypes[0].name",
            fallback="Material type not found",
        ),
    )
    logger.info("Finished Material type friendly name")

    # Library and Location names
    _library_location_names(
        folio_client=folio_client, task_instance=instance, location_id=location_id
    )


def generate_report(**kwargs):
    instance = kwargs["task_instance"]
    dag = kwargs["dag_run"]
    circ_directory = kwargs.get("circ_dir", "/opt/airflow/circ")
    circ_path = pathlib.Path(circ_directory)
    report_path = circ_path / f"{dag.run_id}.json"

    # Generate Header
    record = {
        "Library Name": instance.xcom_pull(
            task_ids="friendly-report-group.friendly-report", key="libraryName"
        ),
        "Loan Type": instance.xcom_pull(
            task_ids="friendly-report-group.friendly-report", key="loan_type"
        ),
        "Location": instance.xcom_pull(
            task_ids="friendly-report-group.friendly-report", key="location"
        ),
        "Material Type": instance.xcom_pull(
            task_ids="friendly-report-group.friendly-report", key="material_type"
        ),
        "Patron Group": instance.xcom_pull(
            task_ids="friendly-report-group.friendly-report", key="patron_group"
        ),
    }

    for policy_type in policy_types:
        record[f"{policy_type} Winning Policy"] = instance.xcom_pull(
            task_ids=f"friendly-report-group.{policy_type}-policy-test",
            key="winning-policy",
        )

    with report_path.open("w+") as fo:
        json.dump(record, fo, indent=2)

    logging.info(f"Finished Generating Report at {report_path}")
    return record


def generate_urls(**kwargs):
    instance = kwargs["task_instance"]
    policy_type = kwargs["policy_type"]
    folio_client = kwargs["folio_client"]
    query_string = urllib.parse.urlencode(
        {
            "patron_type_id": instance.xcom_pull(
                task_ids="setup-circ-rules", key="patron_type_id"
            ),
            "loan_type_id": instance.xcom_pull(
                task_ids="setup-circ-rules", key="loan_type_id"
            ),
            "item_type_id": instance.xcom_pull(
                task_ids="setup-circ-rules", key="item_type_id"
            ),
            "location_id": instance.xcom_pull(
                task_ids="setup-circ-rules", key="location_id"
            ),
        }
    )
    base = f"{folio_client.okapi_url}/circulation/rules/{policy_type}-policy"
    instance.xcom_push(key="single-policy-url", value=f"{base}?{query_string}")
    instance.xcom_push(key="all-policies-url", value=f"{base}-all?{query_string}")


def policy_report(**kwargs):
    folio_client = kwargs["folio_client"]
    policy_type = kwargs["policy_type"]
    instance = kwargs["task_instance"]

    if policy_type in policy_lookup:
        endpoint = policy_lookup[policy_type]["endpoint"]
        policy_key = policy_lookup[policy_type].get(
            "policy_key", f"{policy_type}Policies"
        )
        policy_id = policy_lookup[policy_type].get("key", f"{policy_type}PolicyId")
        if "all_policy_key" in policy_lookup[policy_type]:
            all_policy_id = policy_lookup[policy_type]["all_policy_key"]
        else:
            all_policy_id = policy_id

    else:
        endpoint = f"{policy_type}-policy-storage/{policy_type}-policies"
        policy_key = f"{policy_type}Policies"
        policy_id = f"{policy_type}PolicyId"
        all_policy_id = policy_id

    policy_url = f"{folio_client.okapi_url}/{endpoint}"

    logger.info(f"Checking url {policy_url} for policy")

    policies_result = requests.get(policy_url, headers=folio_client.okapi_headers)

    policies = policies_result.json()

    single_policy = json.loads(
        instance.xcom_pull(
            task_ids=f"retrieve-policies-group.{policy_type}-get-policies",
            key="single-policy",
        )
    )

    all_policies = json.loads(
        instance.xcom_pull(
            task_ids=f"retrieve-policies-group.{policy_type}-get-policies",
            key="all-policies",
        )
    )

    winning_policy, losing_policies = None, []
    for circ_rule_match in all_policies["circulationRuleMatches"]:
        circ_policy_id = circ_rule_match[all_policy_id]
        for policy in policies[policy_key]:
            suffix = f"{policy['name']} - {circ_rule_match['circulationRuleLine']}"
            if circ_policy_id == policy["id"] and single_policy[policy_id]:
                winning_policy = f"Winning policy is {suffix}"
            else:
                losing_policies.append(f"Losing policy is {suffix}")
    instance.xcom_push(key="winning-policy", value=winning_policy)
    instance.xcom_push(key="losing-policies", value=losing_policies)
    logger.info("Finished Policy Report")


def retrieve_policies(**kwargs):
    instance = kwargs["task_instance"]
    policy_type = kwargs["policy_type"]
    folio_client = kwargs["folio_client"]

    task_id = f"retrieve-policies-group.{policy_type}-generate-urls"

    single_policy_url = instance.xcom_pull(task_ids=task_id, key="single-policy-url")

    all_policies_url = instance.xcom_pull(task_ids=task_id, key="all-policies-url")
    single_policy_result = requests.get(
        single_policy_url, headers=folio_client.okapi_headers
    )
    if single_policy_result.status_code == 200:
        instance.xcom_push(key="single-policy", value=single_policy_result.text)

    all_policies_result = requests.get(
        all_policies_url, headers=folio_client.okapi_headers
    )
    if all_policies_result.status_code == 200:
        instance.xcom_push(key="all-policies", value=all_policies_result.text)
    else:
        logger.error(f"Cannot retrieve {all_policies_url}\n{all_policies_result.text}")


def setup_rules(*args, **kwargs):
    instance = kwargs["task_instance"]
    context = get_current_context()
    params = context.get("params")
    for key, value in params.items():
        if value is None:
            raise ValueError(f"{key} value is None")
        instance.xcom_push(key=key, value=value)
