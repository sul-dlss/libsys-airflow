from flask import request
from flask_appbuilder import expose, BaseView as AppBuilderBaseView

from onelogin.saml2.auth import OneLogin_Saml2_Auth

from libsys_airflow.plugins.saml.sso import generate_settings, prepare_request


class BulkCheckinsUploadView(AppBuilderBaseView):
    default_view = "bulk_checkins_home"
    route_base = "/bulk_checkins"

    @expose("/")
    def bulk_checkins_home(self):
        req = prepare_request(request)
        auth = OneLogin_Saml2_Auth(req, generate_settings())

        return self.render_template(
            "bulk-barcodes-checkins/index.html",
            is_authenticated=auth.is_authenticated(),
        )
