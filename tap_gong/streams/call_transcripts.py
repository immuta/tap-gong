import time
from typing import Any, Dict, Optional, Union, List, Iterable
from singer_sdk import typing as th  # JSON Schema typing helpers
from tap_gong import config_helper as helper
from tap_gong.client import GongStream
from tap_gong.streams.calls import CallsStream


class CallTranscriptsStream(GongStream):
    """Define custom stream."""
    name = "call_transcripts"
    path = "/v2/calls/transcript"
    primary_keys = ["callId"]
    rest_method = "POST"
    records_jsonpath = "$.callTranscripts[*]"
    parent_stream_type = CallsStream 
    ignore_parent_replication_key = False

    schema = th.PropertiesList(
        th.Property("callId", th.StringType),
        th.Property("transcript", th.ArrayType(
            th.ObjectType(
                th.Property("speakerId", th.StringType),
                th.Property("topic", th.StringType),
                th.Property("sentences", th.ArrayType(
                    th.ObjectType(
                        th.Property("start", th.IntegerType),
                        th.Property("end", th.IntegerType),
                        th.Property("text", th.StringType),
                    )
                )
            ),
        )
    ))).to_dict()

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the data payload for the REST API request."""
        fromDateTime = helper.get_date_time_string(self.get_starting_timestamp(context), helper.date_time_format_string)
        toDateTime = helper.get_date_time_string_from_config(self.config, helper.end_date_key,
                                                             helper.date_time_format_string)
        request_body = {
            "cursor": next_page_token,
            "filter": {
                "callIds": [context["call_id"]],
                "fromDateTime": fromDateTime,
                "toDateTime": toDateTime
            }
        }
        time.sleep(self.request_delay_seconds)
        return request_body
