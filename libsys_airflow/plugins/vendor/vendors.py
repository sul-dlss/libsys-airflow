import requests

from airflow.models import Variable

from flask_appbuilder import expose, BaseView as AppBuilderBaseView


class VendorManagementView(AppBuilderBaseView):
    default_view = "vendors_index"
    route_base = "/vendor"

    @expose("/")
    def vendors_index(self):
        return self.render_template("index.html")
