import pathlib

from airflow.configuration import conf
from onelogin.saml2.idp_metadata_parser import OneLogin_Saml2_IdPMetadataParser


def get_certs(saml_path: pathlib.Path) -> tuple:
    with (saml_path / "sp.crt").open() as fo:
        public_cert = fo.read()
    with (saml_path / "sp.key").open() as fo:
        private_key = fo.read()
    return public_cert, private_key


def generate_settings(**kwargs) -> dict:
    """
    Generate SAML Service Point Settings
    """
    airflow_url = kwargs.get("airflow_url")
    if not airflow_url:
        airflow_url = conf.get('webserver', 'base_url')
    airflow = kwargs.get("airflow", "/opt/airflow")
    idp_url = kwargs.get("idp_url", "https://login.stanford.edu/metadata.xml")

    bp_url = f"{airflow_url}/saml"
    stanford_idp = OneLogin_Saml2_IdPMetadataParser.parse_remote(idp_url)
    saml_config = pathlib.Path(airflow) / "saml"
    public_cert, private_key = get_certs(saml_config)

    bp_service_point = {
        "entityId": bp_url,
        "assertionConsumerService": {
            "url": f"{bp_url}?acs",
            "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST",
        },
        "singleLogoutService": {
            "url": f"{bp_url}?slo",
            "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect",
        },
        "x509cert": public_cert,
        "privateKey": private_key,
    }
    settings = {
        "strict": True,
        "debug": True,
        "sp": bp_service_point,
        "idp": stanford_idp["idp"],
    }
    return settings


def prepare_request(request) -> dict:
    return {
        "https": "on" if request.scheme.startswith("https") else "off",
        "http_host": conf.get('webserver', 'base_url'),
        "script_name": request.path,
        "get_data": request.args.copy(),
        "post_data": request.form.copy(),
    }
