import pathlib


from flask import flash, make_response, request, redirect, session

from flask_appbuilder import expose, BaseView as AppBuilderBaseView

from onelogin.saml2.auth import OneLogin_Saml2_Auth
from onelogin.saml2.idp_metadata_parser import OneLogin_Saml2_IdPMetadataParser
from onelogin.saml2.utils import OneLogin_Saml2_Utils


from libsys_airflow.plugins.saml.sso import prepare_request, generate_settings


class SamlView(AppBuilderBaseView):
    default_view = "saml_home"
    route_base = "/saml"

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
        req = prepare_request(request)
        pre_settings = generate_settings()
        auth = OneLogin_Saml2_Auth(req, pre_settings)
        settings = auth.get_settings()
        metadata = settings.get_sp_metadata()
        errors = settings.validate_metadata(metadata)

        if len(errors) < 1:
            resp = make_response(metadata, 200)
            resp.headers["Content-Type"] = "text/xml"
        else:
            resp = make_response(", ".join(errors), 500)

        return resp

    @expose("/")
    def saml_home(self):
        req = prepare_request(request)
        auth = OneLogin_Saml2_Auth(req, generate_settings())

        attributes = {}

        match request.args:

            case {"sso": sso}:  # noqa
                return_to = None
                if "bp" in request.args:
                    return_to = f"/{request.args['bp']}/"
                return redirect(auth.login(return_to=return_to))

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
            "saml/index.html",
            user_attributes=attributes,
            is_authenticated=auth.is_authenticated(),
        )
