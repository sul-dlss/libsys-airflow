from airflow.models import Variable
from airflow.utils.email import send_email
from jinja2 import Template


def email_log(**kwargs):
    library = kwargs.get("library", "")
    log_file = kwargs.get("log_file")
    devs_email = Variable.get("EMAIL_DEVS", "sul-unicorn-devs@lists.stanford.edu")
    sul_email = Variable.get("EMAIL_ENC_SUL", "")
    law_email = Variable.get("EMAIL_ENC_LAW", "")
    lane_email = Variable.get("EMAIL_ENC_LANE", "")

    to_addresses = [
        devs_email,
    ]

    match library:
        case "sul":
            to_addresses.append(sul_email)
        case "law":
            to_addresses.append(law_email)
        case "lane":
            to_addresses.append(lane_email)

    with open(log_file, 'r') as fo:
        send_email(
            to=to_addresses,
            subject=f"Fix Encumbrances for {library}",
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
