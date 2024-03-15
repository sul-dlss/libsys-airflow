import logging

import httpx
import pymarc

from typing import List

logger = logging.getLogger(__name__)

OCLC_WORLDCAT_METADATA = "https://metadata.api.oclc.org/worldcat"


class OCLCAPIWrapper(object):
    """
    Helper class for transmitting MARC records to OCLC Worldcat
    API
    """

    def __init__(self, user, password):
        self.token = None
        self.__authenticate__(user, password)

    def __authenticate__(self, username, passphrase) -> None:
        auth_url = "https://oauth.oclc.org/token?grant_type=client_credentials&scope=WorldCatMetadataAP"
        try:
            result = httpx.post(url=auth_url, auth=(username, passphrase))
            logger.info("Retrieved API Access Token")
            self.token = result.json()["access_token"]
        except Exception as e:
            logger.error("Retrieved API Access Token")
            raise Exception("Unable to Retrieve Access Token", e)

    def new(self, marc_records: List[pymarc.Record]):
        pass

    def update(self, marc_records: List[pymarc.Record]):
        pass
