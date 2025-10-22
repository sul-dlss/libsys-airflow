import re

from airflow.sdk import Variable
from jinja2 import Template

from libsys_airflow.plugins.shared.utils import send_email_with_server_name


def email_to(**kwargs):
    fy_code = kwargs.get("fy_code", "")
    devs_email = Variable.get("EMAIL_DEVS", "sul-unicorn-devs@lists.stanford.edu")
    sul_email = Variable.get("EMAIL_ENC_SUL", "")
    law_email = Variable.get("EMAIL_ENC_LAW", "")
    lane_email = Variable.get("EMAIL_ENC_LANE", "")

    to_addresses = [
        devs_email,
    ]

    match Regex_equals(fy_code):
        case r'SUL+':
            to_addresses.append(sul_email)
        case r'LAW+':
            to_addresses.append(law_email)
        case r'LANE+':
            to_addresses.append(lane_email)

    return to_addresses


def email_log(**kwargs):
    fy_code = kwargs.get("fy_code", "")
    log_file = kwargs.get("log_file")
    to_addresses = email_to(fy_code=fy_code)

    with open(log_file, 'r') as fo:
        send_email_with_server_name(
            to=to_addresses,
            subject=subject(fy_code=fy_code),
            html_content=_email_body(fo),
        )


def _email_body(log):
    email_template = Template(
        """
     {{ log_content.replace('\n', '<br>') }}
    """
    )
    html_body = email_template.render(log_content=log.read())

    return html_body


def subject(**kwargs):
    fy_code = kwargs.get("fy_code", "")
    return f"Fix Encumbrances for {fy_code}"


class Regex_equals(str):
    "Override str.__eq__ to match a regex pattern."

    def __eq__(self, pattern):
        return re.match(pattern, self)
