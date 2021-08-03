import time
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

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
        request_body = {
            "filter": {
                "callIds": [context["call_id"]],
                "fromDateTime": self.config("start_date"),
                "toDateTime": None,
                "cursor": next_page_token,
            }
        }
        time.sleep(self.request_delay_seconds)
        return request_body