from typing import Any, Dict, Optional, Union, List, Iterable

import requests
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.exceptions import RetriableAPIError, FatalAPIError
from tap_gong.client import GongStream


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

    def prepare_request_payload(self, context: Optional[dict], next_page_token: Optional[Any]) -> Optional[dict]:
        """Prepare the data payload for the REST API request."""
        request_body = {
            "cursor": next_page_token,
            "filter": {
                "fromDate": "2022-07-01",
                "toDate": "2022-10-01"
            }
        }
        #time.sleep(self.request_delay_seconds)
        return request_body

