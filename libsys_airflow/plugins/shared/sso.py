from onelogin.saml2.idp_metadata_parser import OneLogin_Saml2_IdPMetadataParser


def generate_settings(
    request, idp_url="https://login.stanford.edu/metadata.xml"
) -> dict:
    bp_url = f"{request.scheme}://{request.host}{request.path}"
    stanford_idp = OneLogin_Saml2_IdPMetadataParser.parse_remote(idp_url)
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
    }
    settings = {
        "strict": True,
        "debug": True,
        "sp": bp_service_point,
        "idp": stanford_idp["idp"],
    }
    return settings
