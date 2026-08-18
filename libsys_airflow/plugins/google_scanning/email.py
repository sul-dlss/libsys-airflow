import logging

from jinja2 import Template

from airflow.sdk import task, Variable

from libsys_airflow.plugins.shared.utils import (
    dag_run_url,
    send_email_with_server_name,
)

logger = logging.getLogger(__name__)

GOOGLE_SCANNING_DEFAULT_EMAIL = "google-books-2026@lists.stanford.edu"


def _to_addresses(user_email: str | None) -> list[str]:
    to_emails = [Variable.get("EMAIL_DEVS"), GOOGLE_SCANNING_DEFAULT_EMAIL]
    if user_email:
        to_emails.append(user_email)
    return to_emails


_RESOLUTION_FAILURE_BLOCKS = """
        {% if instance_id_failures %}
        <h3>Barcodes that could not be resolved to an instance</h3>
        <ul>
        {% for failure in instance_id_failures %}
        <li>{{ failure.cart_name }} / {{ failure.barcode }}: {{ failure.reason }}</li>
        {% endfor %}
        </ul>
        {% endif %}
        {% if not_found_instance_ids %}
        <h3>Instances with no MARC record in SRS</h3>
        <ul>
        {% for instance_id in not_found_instance_ids %}
        <li>{{ instance_id }}</li>
        {% endfor %}
        </ul>
        {% endif %}
        """


def _confirmation_email_body(**kwargs) -> str:
    template = Template(
        """
        <h2>Google Scanning On-Campus Shipment</h2>
        <h3>DAG Run: <a href="{{ dag_run_url }}">{{ dag_run_id }}</a></h3>
        <p>Shipped {{ shipped_barcode_count }} barcode(s) from {{ shipped_carts|length }} cart(s):</p>
        <ul>
        {% for cart_name in shipped_carts %}
        <li>{{ cart_name }}</li>
        {% endfor %}
        </ul>
        <p>Files uploaded to Google Drive:</p>
        <ul>
        <li>{{ marc_xml_path }}</li>
        <li>{{ manifest_path }}</li>
        </ul>
        {% if skipped %}
        <h3>Barcodes skipped (already flagged missing or erroring during staging)</h3>
        <ul>
        {% for cart in skipped %}
        <li>{{ cart.cart_name }}
          <ul>
          {% for skip in cart.barcodes %}
          <li>{{ skip.barcode }}: {{ skip.reason }}</li>
          {% endfor %}
          </ul>
        </li>
        {% endfor %}
        </ul>
        {% endif %}
        """
        + _RESOLUTION_FAILURE_BLOCKS
    )
    return template.render(**kwargs)


@task
def shipment_confirmation_email(shipment_result: dict, **kwargs) -> None:
    """
    Sends the on-campus shipment confirmation email to the staff member who
    triggered the shipment (the dag conf's user_email) plus EMAIL_DEVS,
    summarizing what shipped and what was skipped or unresolved, and
    linking to the dag run. shipment_result is assembled by the DAG from
    barcodes_for_shipment/resolve_instance_ids (shipment.py),
    generate_shipment_marc (marc.py), and generate_manifest (manifest.py).
    """
    dag_run = kwargs["dag_run"]
    params = kwargs.get("params", {})
    to_emails = _to_addresses(params.get("user_email"))

    html_content = _confirmation_email_body(
        dag_run_url=dag_run_url(dag_run=dag_run),
        dag_run_id=dag_run.run_id,
        **shipment_result,
    )

    logger.info(f"Sending shipment confirmation email to {to_emails}")
    send_email_with_server_name(
        to=to_emails,
        subject="Google Scanning On-Campus Shipment Confirmation",
        html_content=html_content,
    )


def _failure_email_body(**kwargs) -> str:
    template = Template(
        """
        <h2>Google Scanning On-Campus Shipment Failed</h2>
        <h3>DAG Run: <a href="{{ dag_run_url }}">{{ dag_run_id }}</a></h3>
        <p>{{ reason }}</p>
        <p>Selected carts remain staged for retry.</p>
        """
    )
    return template.render(**kwargs)


def send_shipment_failure_email(reason: str, dag_run, user_email: str | None) -> None:
    """
    Sends the on-campus shipment failure notification to EMAIL_DEVS (and
    the triggering staff member, if known). Used both by the
    shipment_failure_email task below (Drive upload failures, handled by
    the check_upload branch in on_campus_shipment.py) and by that DAG's
    on-failure callback for tasks upstream of that branch (e.g. no
    barcodes resolving to an instance).
    """
    to_emails = _to_addresses(user_email)

    html_content = _failure_email_body(
        dag_run_url=dag_run_url(dag_run=dag_run),
        dag_run_id=dag_run.run_id,
        reason=reason,
    )

    logger.info(f"Sending shipment failure email to {to_emails}")
    send_email_with_server_name(
        to=to_emails,
        subject="Google Scanning On-Campus Shipment Failed",
        html_content=html_content,
    )


@task
def shipment_failure_email(reason: str, **kwargs) -> None:
    """
    Sends a failure notification when the on-campus shipment DAG fails
    after uploading to Drive was attempted. Selected carts are left in
    staged/ (not archived) so staff can retry once the issue's fixed.
    """
    dag_run = kwargs["dag_run"]
    params = kwargs.get("params", {})
    send_shipment_failure_email(reason, dag_run, params.get("user_email"))


def _sal3_confirmation_email_body(**kwargs) -> str:
    template = Template(
        """
        <h2>Google Scanning CaiaSoft SAL3 Shipment</h2>
        <h3>DAG Run: <a href="{{ dag_run_url }}">{{ dag_run_id }}</a></h3>
        <p>Shipped {{ shipped_barcode_count }} barcode(s) for {{ date }} from
        {{ shipment_numbers|length }} CaiaSoft shipment(s):</p>
        <ul>
        {% for shipment_number in shipment_numbers %}
        <li>{{ shipment_number }}</li>
        {% endfor %}
        </ul>
        <p>Files uploaded to Google Drive:</p>
        <ul>
        <li>{{ marc_xml_path }}</li>
        <li>{{ manifest_path }}</li>
        </ul>
        """
        + _RESOLUTION_FAILURE_BLOCKS
    )
    return template.render(**kwargs)


@task
def sal3_confirmation_email(sal3_result: dict, **kwargs) -> None:
    """
    Sends the CaiaSoft SAL3 shipment confirmation email to EMAIL_DEVS,
    summarizing the dispatched shipments processed and any resolution
    failures. sal3_result is assembled by the caiasoft_sal3 DAG from
    caiasoft_shipment.py, marc.py::generate_marc_for_instances, and
    manifest.py::generate_manifest.
    """
    dag_run = kwargs["dag_run"]
    to_emails = _to_addresses(None)

    html_content = _sal3_confirmation_email_body(
        dag_run_url=dag_run_url(dag_run=dag_run),
        dag_run_id=dag_run.run_id,
        **sal3_result,
    )

    logger.info(f"Sending SAL3 shipment confirmation email to {to_emails}")
    send_email_with_server_name(
        to=to_emails,
        subject="Google Scanning CaiaSoft SAL3 Shipment Confirmation",
        html_content=html_content,
    )


def _sal3_failure_email_body(**kwargs) -> str:
    template = Template(
        """
        <h2>Google Scanning CaiaSoft SAL3 Shipment Failed</h2>
        <h3>DAG Run: <a href="{{ dag_run_url }}">{{ dag_run_id }}</a></h3>
        <p>{{ reason }}</p>
        """
    )
    return template.render(**kwargs)


def send_sal3_failure_email(reason: str, dag_run) -> None:
    """
    Sends the CaiaSoft SAL3 shipment failure notification to EMAIL_DEVS.
    Used both by the sal3_failure_email task below (Drive upload failures)
    and by the caiasoft_sal3 DAG's on-failure callback for tasks upstream
    of the upload step.
    """
    to_emails = _to_addresses(None)

    html_content = _sal3_failure_email_body(
        dag_run_url=dag_run_url(dag_run=dag_run),
        dag_run_id=dag_run.run_id,
        reason=reason,
    )

    logger.info(f"Sending SAL3 shipment failure email to {to_emails}")
    send_email_with_server_name(
        to=to_emails,
        subject="Google Scanning CaiaSoft SAL3 Shipment Failed",
        html_content=html_content,
    )


@task
def sal3_failure_email(reason: str, **kwargs) -> None:
    """
    Sends a failure notification when the caiasoft_sal3 DAG fails after
    uploading to Drive was attempted.
    """
    dag_run = kwargs["dag_run"]
    send_sal3_failure_email(reason, dag_run)
