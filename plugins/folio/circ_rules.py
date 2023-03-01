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
    row_count = kwargs.get("row_count", "")
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

    location_code = "Location not found"
    if len(lib_id_match) > 0:
        # Second call to Okapi
        library_id = lib_id_match[0].value
        location_units_results = requests.get(
            f"""{folio_client.okapi_url}/location-units/libraries?query=(id=="{library_id}")""",
            headers=folio_client.okapi_headers,
        )
        location_code_expr = parse("$.loclibs[0].code")
        location_code_matches = location_code_expr.find(location_units_results.json())
        if len(location_code_matches) > 0:
            location_code = location_code_matches[0].value

    instance.xcom_push(key=f"libraryName{row_count}", value=library_name)
    instance.xcom_push(key=f"location{row_count}", value=location_code)


def friendly_report(**kwargs):
    instance = kwargs["task_instance"]
    folio_client = kwargs["folio_client"]
    row_count = kwargs.get("row_count", "")
    patron_group_id = instance.xcom_pull(
        task_ids="setup-circ-rules", key=f"patron_group_id{row_count}"
    )
    loan_type_id = instance.xcom_pull(
        task_ids="setup-circ-rules", key=f"loan_type_id{row_count}"
    )
    material_type_id = instance.xcom_pull(
        task_ids="setup-circ-rules", key=f"material_type_id{row_count}"
    )
    location_id = instance.xcom_pull(
        task_ids="setup-circ-rules", key=f"location_id{row_count}"
    )

    # Patron Group Name
    instance.xcom_push(
        key=f"patron_group{row_count}",
        value=_friendly_name(
            folio_client=folio_client,
            query=f"""groups?query=(id=="{patron_group_id}")""",
            json_path="$.usergroups[0].group",
            fallback="Patron group not found",
        ),
    )
    logger.info("Finished patron_group friendly name")

    # Loan type friendly name
    instance.xcom_push(
        key=f"loan_type{row_count}",
        value=_friendly_name(
            folio_client=folio_client,
            query=f"""loan-types?query=(id=="{loan_type_id}")""",
            json_path="$.loantypes[0].name",
            fallback="Loan type not found",
        ),
    )
    logger.info("Finished Loan type friendly name")

    # Material type friendly name
    instance.xcom_push(
        key=f"material_type{row_count}",
        value=_friendly_name(
            folio_client=folio_client,
            query=f"""material-types?query=(id=="{material_type_id}")""",
            json_path="$.mtypes[0].name",
            fallback="Material type not found",
        ),
    )
    logger.info("Finished Material type friendly name")

    # Library and Location names
    _library_location_names(
        folio_client=folio_client,
        task_instance=instance,
        location_id=location_id,
        row_count=row_count,
    )


def friendly_batch_report(**kwargs):
    instance = kwargs["task_instance"]
    total_rows = instance.xcom_pull(task_ids="setup-circ-rules", key="total")
    for i in range(int(total_rows)):
        friendly_report(row_count=i, **kwargs)
    logger.info(f"Finished friendly labels generation for {total_rows:,} batches")


def generate_report(**kwargs):
    instance = kwargs["task_instance"]
    dag = kwargs["dag_run"]
    circ_directory = kwargs.get("circ_dir", "/opt/airflow/circ")
    circ_path = pathlib.Path(circ_directory)
    row_count = kwargs.get("row_count", "")
    report_path = circ_path / f"{dag.run_id}.json"

    # Generate Header
    record = {
        "Library Name": instance.xcom_pull(
            task_ids="friendly-report-group.friendly-report",
            key=f"libraryName{row_count}",
        ),
        "Loan Type": instance.xcom_pull(
            task_ids="friendly-report-group.friendly-report",
            key=f"loan_type{row_count}",
        ),
        "Location": instance.xcom_pull(
            task_ids="friendly-report-group.friendly-report", key=f"location{row_count}"
        ),
        "Material Type": instance.xcom_pull(
            task_ids="friendly-report-group.friendly-report",
            key=f"material_type{row_count}",
        ),
        "Patron Group": instance.xcom_pull(
            task_ids="friendly-report-group.friendly-report",
            key=f"patron_group{row_count}",
        ),
    }

    for policy_type in policy_types:
        record[f"{policy_type}"] = instance.xcom_pull(
            task_ids=f"friendly-report-group.{policy_type}-policy-test",
            key=f"winning-policy{row_count}",
        )

    if isinstance(row_count, str):
        with report_path.open("w+", encoding="utf-8-sig") as fo:
            json.dump(record, fo, indent=2)

        logging.info(f"Finished Generating Report at {report_path}")
    return record


def generate_batch_report(**kwargs):
    instance = kwargs["task_instance"]
    dag = kwargs["dag_run"]
    circ_directory = kwargs.get("circ_dir", "/opt/airflow/circ")
    circ_path = pathlib.Path(circ_directory)
    total_rows = instance.xcom_pull(task_ids="setup-circ-rules", key="total")
    scenarios = []
    for i in range(int(total_rows)):
        record = generate_report(row_count=i, **kwargs)
        scenarios.append(record)

    with (circ_path / f"{dag.run_id}.json").open("w+", encoding="utf-8-sig") as fo:
        json.dump(scenarios, fo, indent=2)
    logger.info(f"Finished Generating Batch Report for {int(total_rows):,} batches")


def generate_urls(**kwargs):
    instance = kwargs["task_instance"]
    policy_type = kwargs["policy_type"]
    folio_client = kwargs["folio_client"]
    row_count = kwargs.get("row_count", "")
    query_string = urllib.parse.urlencode(
        {
            "patron_type_id": instance.xcom_pull(
                task_ids="setup-circ-rules", key=f"patron_group_id{row_count}"
            ),
            "loan_type_id": instance.xcom_pull(
                task_ids="setup-circ-rules", key=f"loan_type_id{row_count}"
            ),
            "item_type_id": instance.xcom_pull(
                task_ids="setup-circ-rules", key=f"material_type_id{row_count}"
            ),
            "location_id": instance.xcom_pull(
                task_ids="setup-circ-rules", key=f"location_id{row_count}"
            ),
            "limit": 2_000  # Should be plenty
        }
    )
    base = f"{folio_client.okapi_url}/circulation/rules/{policy_type}-policy"
    instance.xcom_push(
        key=f"single-policy-url{row_count}", value=f"{base}?{query_string}"
    )
    instance.xcom_push(
        key=f"all-policies-url{row_count}", value=f"{base}-all?{query_string}"
    )


def generate_batch_urls(**kwargs):
    instance = kwargs["task_instance"]
    total_rows = instance.xcom_pull(task_ids="setup-circ-rules", key="total")
    for i in range(int(total_rows)):
        generate_urls(row_count=i, **kwargs)
    logger.info(f"Finished generating urls for {total_rows:,} batches")


def policy_report(**kwargs):
    folio_client = kwargs["folio_client"]
    policy_type = kwargs["policy_type"]
    instance = kwargs["task_instance"]
    row_count = kwargs.get("row_count", "")

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

    policy_url = f"{folio_client.okapi_url}/{endpoint}?limit=2000"

    policies_result = requests.get(policy_url, headers=folio_client.okapi_headers)

    policies = policies_result.json()

    single_policy = json.loads(
        instance.xcom_pull(
            task_ids=f"retrieve-policies-group.{policy_type}-get-policies",
            key=f"single-policy{row_count}",
        )
    )

    all_policies = json.loads(
        instance.xcom_pull(
            task_ids=f"retrieve-policies-group.{policy_type}-get-policies",
            key=f"all-policies{row_count}",
        )
    )

    winning_policy, losing_policies = None, []
    for circ_rule_match in all_policies["circulationRuleMatches"]:
        circ_policy_id = circ_rule_match[all_policy_id]
        for policy in policies[policy_key]:
            suffix = f"{policy['name']} - {circ_rule_match['circulationRuleLine']}"
            if (
                circ_policy_id == policy["id"]
                and circ_policy_id == single_policy[policy_id]
            ):
                logger.info(f"Winning {circ_policy_id} {policy_id}")
                winning_policy = f"Winning policy is {suffix}"
            else:
                losing_policies.append(f"Losing policy is {suffix}")
    if winning_policy is None:
        for policy in policies:
            if policy.get('name', "").startswith("No"):
                winning_policy = policy['name']
    instance.xcom_push(key=f"winning-policy{row_count}", value=winning_policy)
    instance.xcom_push(key=f"losing-policies{row_count}", value=losing_policies)
    logger.info("Finished Policy Report")


def policy_batch_report(**kwargs):
    instance = kwargs["task_instance"]
    total_rows = instance.xcom_pull(task_ids="setup-circ-rules", key="total")
    for i in range(int(total_rows)):
        policy_report(row_count=i, **kwargs)
    logger.info(f"Finished generating policy reports for {total_rows:,} batches")


def retrieve_policies(**kwargs):
    instance = kwargs["task_instance"]
    policy_type = kwargs["policy_type"]
    folio_client = kwargs["folio_client"]
    row_count = kwargs.get("row_count", "")

    task_id = f"retrieve-policies-group.{policy_type}-generate-urls"

    single_policy_url = instance.xcom_pull(
        task_ids=task_id, key=f"single-policy-url{row_count}"
    )

    all_policies_url = instance.xcom_pull(
        task_ids=task_id, key=f"all-policies-url{row_count}"
    )

    single_policy_result = requests.get(
        single_policy_url, headers=folio_client.okapi_headers
    )

    if single_policy_result.status_code == 200:
        instance.xcom_push(
            key=f"single-policy{row_count}", value=single_policy_result.text
        )
    else:
        logger.error(f"Cannot retrieve {single_policy_url}\n{single_policy_result.text}")

    all_policies_result = requests.get(
        all_policies_url, headers=folio_client.okapi_headers
    )
    if all_policies_result.status_code == 200:
        instance.xcom_push(
            key=f"all-policies{row_count}", value=all_policies_result.text
        )
    else:
        logger.error(f"Cannot retrieve {all_policies_url}\n{all_policies_result.text}")


def retrieve_batch_policies(**kwargs):
    instance = kwargs["task_instance"]
    total_rows = instance.xcom_pull(task_ids="setup-circ-rules", key="total")
    for i in range(int(total_rows)):
        retrieve_policies(row_count=i, **kwargs)
    logger.info(f"Finished retrieving policies for {total_rows:,} batches")


def setup_batch_rules(*args, **kwargs):
    instance = kwargs["task_instance"]
    context = get_current_context()
    params = context.get("params")
    raw_scenarios = params["scenarios"]
    scenarios = json.loads(raw_scenarios)
    # Calculate total batches based on number of patron_group_id rows
    total = len(scenarios['patron_group_id'])
    instance.xcom_push(key="total", value=total)
    for policy_type, rows in scenarios.items():
        for row_count, uuid in rows.items():
            instance.xcom_push(key=f"{policy_type}{row_count}", value=uuid)

    logger.info(f"Setup completed for {total:,} batches")


def setup_rules(*args, **kwargs):
    instance = kwargs["task_instance"]
    context = get_current_context()
    params = context.get("params")
    for key, value in params.items():
        if value is None:
            raise ValueError(f"{key} value is None")
        instance.xcom_push(key=key, value=value)
