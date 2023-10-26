"""REST client handling, including GongStream base class."""

from pathlib import Path
from typing import Any, Dict
import requests
import json
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import RetriableAPIError, FatalAPIError
from tap_gong.auth import GongAuthenticator
from tap_gong.utils import log_error

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class GongStream(RESTStream):
    """Gong stream class."""

    url_base = "https://api.gong.io"
    records_jsonpath = "$[*]"
    next_page_token_jsonpath = "$.records.cursor"

    # Streams that will make many calls may sleep
    # For this amount of time to avoid rate limits
    request_delay_seconds = 0.3
    tries = 0

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

    def response_error_message(self, response: requests.Response) -> str:
        """
            Method overridden to capture errors correctly based on gong API response.
        """
        # Unauthorized for path|Validate credentials failed|Missing mandatory header "Authorization"
        if response.status_code == 401:
            return 'The key or secret provided is incorrect.'
        # No users found corresponding to the provided filters
        if response.status_code == 404:
            return 'There was no data found for the given date range.'

        errors = None
        try:
            errors = json.loads(response.content.decode('utf-8')).get("errors", [])
        except:
            pass
        
        if errors is None or len(errors) == 0:
            msg = 'Unexpected error'
        else:
            msg = ', '.join(errors)
        
        return f'Import failed with following Gong error: {msg}'

    def _request(
        self, prepared_request: requests.PreparedRequest, context: dict
    ) -> requests.Response:
        """
        Override default Singer request to add error logging for Symon Import
        """
        try:
            self.tries += 1
            return super()._request(prepared_request, context)
        except RetriableAPIError as e:
            if self.tries == self.backoff_max_tries():
                log_error(e, self.config, self.logger, 'gong.GongApiError')
            raise
        except FatalAPIError as e:
            log_error(e, self.config, self.logger, 'gong.GongApiError')
            raise
