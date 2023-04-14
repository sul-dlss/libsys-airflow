from airflow.models import Variable

from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from flask import flash, request, redirect, Response
from folioclient import FolioClient

class DataManagementView(AppBuilderBaseView):
    default_view = "data_home"
    route_base = "/data_management"

    def __init__(self, *args, **kwargs):
        self.folio_client = kwargs.get("folio_client")
        if self.folio_client is None:
            self.folio_client = FolioClient(
                Variable.get("okapi_url"),
                "sul",
                Variable.get("folio_user"),
                Variable.get("folio_password")
            )
        super().__init__(*args, **kwargs)

    @expose("/")
    def data_home(self):
        return self.render_template("index.html")