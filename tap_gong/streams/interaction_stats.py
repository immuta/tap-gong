from typing import Any, Dict, Optional, Union, List, Iterable
from datetime import datetime
import requests
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.exceptions import RetriableAPIError, FatalAPIError
from tap_gong.client import GongStream


class InteractionStatsStream(GongStream):
    name = "interaction_stats"
    schema = th.PropertiesList(
        th.Property("userEmailAddress", th.StringType),
        th.Property("userId", th.StringType),
        th.Property("personInteractionStats",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("name", th.StringType),
                            th.Property("value", th.NumberType)
                        )
                    )
                )
    ).to_dict()
    path = "/v2/stats/interaction"
    primary_keys = ["userId"]
    records_jsonpath = "$.peopleInteractionStats[*]"
    rest_method = "POST"
    next_page_token_jsonpath = "$.records.cursor"
    state_partitioning_keys = []

    def prepare_request_payload(self, context: Optional[dict], next_page_token: Optional[Any]) -> Optional[dict]:
        """Prepare the data payload for the REST API request."""
        request_body = {
            "cursor": next_page_token,
            "filter": {
                "fromDate": self.config.get("start_date", datetime.min.strftime("%Y-%m-%dT%H:%M:%SZ")),
                "toDate": self.config.get("end_date", datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))
            }
        }
        return request_body

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response.

        Checks for error status codes and wether they are fatal or retriable.

        In case an error is deemed transient and can be safely retried, then this
        method should raise an :class:`singer_sdk.exceptions.RetriableAPIError`.
        By default this applies to 5xx error codes, along with values set in:
        :attr:`~singer_sdk.RESTStream.extra_retry_statuses`

        In case an error is unrecoverable raises a
        :class:`singer_sdk.exceptions.FatalAPIError`. By default, this applies to
        4xx errors, excluding values found in:
        :attr:`~singer_sdk.RESTStream.extra_retry_statuses`

        Tap developers are encouraged to override this method if their APIs use HTTP
        status codes in non-conventional ways, or if they communicate errors
        differently (e.g. in the response body).

        .. image:: ../images/200.png

        Args:
            response: A `requests.Response`_ object.

        Raises:
            FatalAPIError: If the request is not retriable.
            RetriableAPIError: If the request is retriable.

        .. _requests.Response:
            https://requests.readthedocs.io/en/latest/api/#requests.Response
        """
        if (
                response.status_code in self.extra_retry_statuses
                or 500 <= response.status_code < 600
        ):
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)
        elif 400 <= response.status_code < 500:
            # 404 = nothing found for user - move on
            if response.status_code != 404:
                msg = self.response_error_message(response)
                raise FatalAPIError(msg)


