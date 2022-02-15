from airflow.models import Variable

def post_to_okapi(**kwargs):
    endpoint = kwargs.get("endpoint")
    jwt = kwargs["token"]

    records = kwargs["records"]
    payload_key = kwargs["payload_key"]

    tenant = "sul"
    okapi_url = Variable.get("OKAPI_URL")

    okapi_instance_url = f"{okapi_url}{endpoint}"

    headers = {
        "Content-type": "application/json",
        "user-agent": "FolioAirflow",
        "x-okapi-token": jwt,
        "x-okapi-tenant": tenant,
    }

    payload = {payload_key: records}

    new_record_result = requests.post(
        okapi_instance_url,
        headers=headers,
        json=payload,
    )

    logger.info(
        f"Result status code {new_record_result.status_code} for {len(records)} records"  # noqa
    )

    if new_record_result.status_code > 399:
        logger.error(new_record_result.text)
        raise ValueError(
            f"FOLIO POST Failed with error code:{new_record_result.status_code}"  # noqa
        )