from jinja2 import Template

from airflow.models import Variable
from airflow.utils.email import send_email


def generate_mapping_email(instances: str, errors: str):
    """
    Generates email for mapping MARC to Instance
    """
    template = Template(
        """
        <h2>Summary Report for MARC-to-Instance Mapping</h2>
        <ul>
            <li><strong>Total Successful Instance Re-mapped:</strong> {{ instances }}</li>
            <li><strong>Total SRS Errors:</strong> {{ srs_errors }}</li>
        </ul>
    """
    )
    body = template.render(instances=instances, errors=errors)
    devs_to_email_addr = Variable.get("EMAIL_DEVS")
    send_email(
        to=[devs_to_email_addr],
        subject="Summary of MARC-to-Instance Mapping",
        html_content=body,
    )
