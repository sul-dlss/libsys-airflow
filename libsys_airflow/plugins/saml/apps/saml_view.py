import pathlib


from flask import flash, make_response, request, redirect, session

from flask_appbuilder import expose, BaseView as AppBuilderBaseView

from onelogin.saml2.auth import OneLogin_Saml2_Auth
from onelogin.saml2.idp_metadata_parser import OneLogin_Saml2_IdPMetadataParser
from onelogin.saml2.utils import OneLogin_Saml2_Utils

from libsys_airflow.plugins.shared.folio_client import folio_client as get_folio_client
from libsys_airflow.plugins.saml.sso import prepare_request, generate_settings


class SamlView(AppBuilderBaseView):
    default_view = "saml_home"
    route_base = "/saml"

    @expose("/result")
    def saml_result(self):
        return f"""Headers: {request.headers}"""

    @expose("/")
    def saml_home(self):
        folio_client = get_folio_client()

        return self.render_template(
            "saml-sso/index.html",
            gateway_url=folio_client.gateway_url
        )
