from typing import Any, Dict, Optional, Union, List, Iterable
import requests
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.exceptions import RetriableAPIError, FatalAPIError
from tap_gong.client import GongStream
from tap_gong import config_helper


class AggregatedActivityStream(GongStream):
    name = "aggregated_activity"
    schema = th.PropertiesList(
        th.Property("userEmailAddress", th.StringType),
        th.Property("userId", th.StringType),
        th.Property("userAggregateActivityStats",
                    th.ObjectType(
                        th.Property("callsAsHost", th.NumberType),
                        th.Property("callsGaveFeedback", th.NumberType),
                        th.Property("callsRequestedFeedback", th.NumberType),
                        th.Property("callsReceivedFeedback", th.NumberType),
                        th.Property("ownCallsListenedTo", th.NumberType),
                        th.Property("othersCallsListenedTo", th.NumberType),
                        th.Property("callsSharedInternally", th.NumberType),
                        th.Property("callsSharedExternally", th.NumberType),
                        th.Property("callsScorecardsFilled", th.NumberType),
                        th.Property("callsScorecardsReceived", th.NumberType),
                        th.Property("callsAttended", th.NumberType),
                        th.Property("callsCommentsGiven", th.NumberType),
                        th.Property("callsCommentsReceived", th.NumberType),
                        th.Property("callsMarkedAsFeedbackGiven", th.NumberType),
                        th.Property("callsMarkedAsFeedbackReceived", th.NumberType)
                    )
                    )
    ).to_dict()
    path = "/v2/stats/activity/aggregate"
    primary_keys = ["userId"]
    records_jsonpath = "$.usersAggregateActivityStats[*]"
    rest_method = "POST"
    next_page_token_jsonpath = "$.records.cursor"
    #parent_stream_type = UsersStream
    #ignore_parent_replication_key = False
    state_partitioning_keys = []

    # extras for retry
    retried = False
    modified_request = False

    def prepare_request_payload(self, context: Optional[dict], next_page_token: Optional[Any]) -> Optional[dict]:
        """Prepare the data payload for the REST API request."""
        stats_filter_dates = config_helper.get_stats_dates_from_config(
            self.config, self.retried)

        request_body = {
            "cursor": next_page_token,
            "filter": {
                "fromDate": stats_filter_dates["stats_from_date"],
                "toDate": stats_filter_dates["stats_to_date"]
            }
        }
        # time.sleep(self.request_delay_seconds)
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
            msg = self.response_error_message(response)
            if response.status_code == 400 and not self.retried:
                self.retried = True
                raise RetriableAPIError(msg, response)
            else:
                raise FatalAPIError(msg)

    def _request(
        self, prepared_request: requests.PreparedRequest, context: dict
    ) -> requests.Response:
        """
        Override default Singer request so we can modify payload during retry
        """
        if self.retried and not self.modified_request:
            prepared_request = self.prepare_request(None, None)
            self.modified_request = True
        response = self.requests_session.send(prepared_request, timeout=self.timeout)
        self._write_request_duration_log(
            endpoint=self.path,
            response=response,
            context=context,
            extra_tags={"url": prepared_request.path_url}
            if self._LOG_REQUEST_METRIC_URLS
            else None,
        )
        self.validate_response(response)
        return response
