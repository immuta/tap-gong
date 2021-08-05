"""REST client handling, including GongStream base class."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from tap_gong.auth import GongAuthenticator


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class GongStream(RESTStream):
    """Gong stream class."""

    url_base = "https://api.gong.io"
    records_jsonpath = "$[*]"
    next_page_token_jsonpath = "$.records.cursor"

    # Streams that will make many calls may sleep
    # For this amount of time to avoid rate limits
    request_delay_seconds = 0.3

    @property
    def authenticator(self) -> GongAuthenticator:
        """Return a new authenticator object."""
        return GongAuthenticator.create_for_stream(self)

    def get_url_params(self, context, next_page_token) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if next_page_token:
            params["cursor"] = next_page_token
        return params
