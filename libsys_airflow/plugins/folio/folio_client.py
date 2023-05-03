import requests


class FolioClient:
    """
    Low-level client for Folio API.

    Based on https://github.com/FOLIO-FSE/FolioClient/blob/master/folioclient/FolioClient.py
    """

    def __init__(self, okapi_url, tenant_id, username, password):
        self.okapi_url = okapi_url
        self.tenant_id = tenant_id
        self.username = username
        self.password = password
        self.login()
        self.okapi_headers = {
            "x-okapi-token": self.okapi_token,
            "x-okapi-tenant": self.tenant_id,
        }

    def __repr__(self) -> str:
        return f"FolioClient for tenant {self.tenant_id} at {self.okapi_url} as {self.username}"

    def login(self):
        """Logs into FOLIO in order to get the okapi token"""
        payload = {"username": self.username, "password": self.password}
        headers = {"x-okapi-tenant": self.tenant_id}
        url = f"{self.okapi_url}/authn/login"
        resp = requests.post(url, json=payload, headers=headers)
        resp.raise_for_status()

        self.okapi_token = resp.headers.get("x-okapi-token")

    def get(self, path, params=None):
        """Performs a GET and turns it into a json object"""
        url = self.okapi_url + path
        resp = requests.get(url, headers=self.okapi_headers, params=params)
        return self._handle_response(resp)

    def put(self, path, payload):
        """Performs a PUT and turns it into a json object"""
        url = self.okapi_url + path
        resp = requests.put(url, headers=self.okapi_headers, json=payload)
        return self._handle_response(resp)

    def post(self, path, payload):
        """Performs a POST and turns it into a json object"""
        url = self.okapi_url + path
        resp = requests.post(url, headers=self.okapi_headers, json=payload)
        return self._handle_response(resp)

    def post_file(self, path, filepath, content_type="application/octet-stream"):
        """Performs a POST and returns the response object"""
        url = self.okapi_url + path
        with open(filepath, "rb") as f:
            payload = f.read()

        headers = self.okapi_headers.copy()
        headers["Content-Type"] = content_type

        resp = requests.post(url, headers=headers, data=payload)
        return self._handle_response(resp)

    def _handle_response(self, resp):
        resp.raise_for_status()
        if resp.status_code == 204:
            return None
        else:
            return resp.json()
