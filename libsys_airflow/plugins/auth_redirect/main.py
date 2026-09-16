from airflow.plugins_manager import AirflowPlugin

from libsys_airflow.plugins.shared.login_redirect import LoginReturnMiddleware

login_return_middleware = {
    "name": "Login Return",
    "middleware": LoginReturnMiddleware,
}


class AuthRedirectPlugin(AirflowPlugin):
    """
    Returns a user to the plugin page they asked for after Keycloak logs them in.

    The middleware has to be registered here rather than on an
    individual app because it acts on Airflow's own login callback route.
    """

    name = "Auth Redirect"
    fastapi_root_middlewares = [login_return_middleware]
