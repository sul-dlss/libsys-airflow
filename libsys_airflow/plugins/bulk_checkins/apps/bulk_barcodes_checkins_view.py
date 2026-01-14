from flask import flash, make_response, redirect, request, session
from flask_appbuilder import expose, BaseView as AppBuilderBaseView

from onelogin.saml2.auth import OneLogin_Saml2_Auth
from onelogin.saml2.utils import OneLogin_Saml2_Utils

from libsys_airflow.plugins.shared.sso import generate_settings


def init_saml_auth(request) -> tuple:
    req = {
        "https": "on" if request.scheme.startswith("https") else "off",
        "http_host": request.host,
        "script_name": request.path,
        "get_data": request.args.copy(),
        "post_data": request.form.copy(),
    }
    settings = generate_settings(request)
    auth = OneLogin_Saml2_Auth(req, settings)
    return auth, req


class BulkCheckinsUploadView(AppBuilderBaseView):
    default_view = "bulk_checkins_home"
    route_base = "/bulk_checkins"

    def handle_acs(self, auth):
        """
        Handles Assertion Consumer Service
        """
        request_id = None
        if "AuthNRequestID" in session:
            request_id = session["AuthNRequestID"]
        auth.process_response(request_id=request_id)
        errors = auth.get_errors()
        if len(errors) < 1:
            if "AuthNRequestID" in session:
                del session["AuthNRequestID"]
            session["samlUserdata"] = auth.get_attributes()
            session["samlNameId"] = auth.get_nameid()
            session["samlNameIdFormat"] = auth.get_nameid_format()
            session["samlNameIdNameQualifier"] = auth.get_nameid_nq()
            session["samlNameIdSPNameQualifier"] = auth.get_nameid_spnq()
            session["samlSessionIndex"] = auth.get_session_index()

    def handle_slo(self, auth):
        """
        Handles Single Logout
        """
        name_id = None
        name_id_format = None
        name_id_nq = None
        name_id_spnq = None
        session_index = None

        if "samlNameId" in session:
            name_id = session["samlNameId"]
        if "samlSessionIndex" in session:
            session_index = session["samlSessionIndex"]
        if "samlNameIdFormat" in session:
            name_id_format = session["samlNameIdFormat"]
        if "samlNameIdNameQualifier" in session:
            name_id_nq = session["samlNameIdNameQualifier"]
        if "samlNameIdSPNameQualifier" in session:
            name_id_spnq = session["samlNameIdSPNameQualifier"]

        return auth.logout(
            name_id=name_id,
            session_index=session_index,
            nq=name_id_nq,
            name_id_format=name_id_format,
            spnq=name_id_spnq,
        )

    @expose("/metadata/")
    def metadata(self):
        auth, _ = init_saml_auth(request)
        settings = auth.get_settings()
        metadata = settings.get_sp_metadata()
        errors = settings.validate_metadata(metadata)

        if len(errors) < 1:
            resp = make_response(metadata, 200)
            resp.headers["Content-Type"] = "text/xml"
        else:
            resp = make_response(", ".join(errors), 500)

        return resp

    @expose("/", methods=["GET", "POST"])
    def bulk_checkins_home(self):
        auth, req = init_saml_auth(request)
        attributes = {}

        match request.args:

            case {"sso": sso}:  # noqa
                return redirect(auth.login())

            case {"slo": slo}:  # noqa
                return redirect(self.handle_slo(auth))

            case {"acs": acs}:  # noqa
                self.handle_acs(auth)
                self_url = OneLogin_Saml2_Utils.get_self_url(req)
                if (
                    "RelayState" in request.form
                    and self_url != request.form["RelayState"]
                ):
                    return redirect(auth.redirect_to(request.form["RelayState"]))

            case {"sls": sls}:  # noqa
                errors, url = self.handle_sls()
                if len(errors) < 1:
                    if url is not None:
                        return redirect(url)
                    flash("Logged out of SSO")
                elif auth.get_settings().is_debug_action():
                    error_reason = auth.get_last_error_reason()
                    flash(f"Error: {error_reason}")

        if "samlUserdata" in session:
            if len(session["samlUserdata"]) > 0:
                attributes = session["samlUserdata"].items()

        return self.render_template(
            "bulk-barcodes-checkins/index.html",
            user_attributes=attributes,
            is_authenticated=auth.is_authenticated(),
        )
