"""Stream type classes for tap-gong."""

import time
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_gong.client import GongStream

class CallsStream(GongStream):
    """Define custom stream."""
    name = "calls"
    path = "/v2/calls/extensive"
    primary_keys = ["id"]
    replication_key = "started"
    records_jsonpath = "$.calls[*]"
    rest_method = "POST"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("started", th.DateTimeType),
        th.Property("metaData", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("url", th.StringType),
            th.Property("title", th.StringType),
            th.Property("scheduled", th.DateTimeType),
            th.Property("started", th.DateTimeType),
            th.Property("duration", th.IntegerType),
            th.Property("primaryUserId", th.StringType),
            th.Property("direction", th.StringType),
            th.Property("system", th.StringType),
            th.Property("scope", th.StringType),
            th.Property("media", th.StringType),
            th.Property("language", th.StringType),
            th.Property("workspaceId", th.StringType),
            th.Property("sdrDisposition", th.StringType),
            th.Property("clientUniqueId", th.StringType),
            th.Property("customData", th.StringType),
        )),
        th.Property("context", th.ArrayType(th.StringType)),
        th.Property("parties", th.ArrayType(
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("emailAddress", th.StringType),
                th.Property("name", th.StringType),
                th.Property("title", th.StringType),
                th.Property("userId", th.StringType),
                th.Property("speakerId", th.StringType),
                th.Property("context", th.ArrayType(
                    th.ObjectType(
                        th.Property("system", th.StringType),
                        th.Property("objects", th.ArrayType(
                            th.ObjectType(
                                th.Property("objectType", th.StringType),
                                th.Property("objectId", th.StringType),
                                th.Property("fields", th.ArrayType(
                                    th.ObjectType(
                                        th.Property("name", th.StringType),
                                        th.Property("value", th.StringType),
                                    )),
                                ),
                            )),
                        ),
                    )),
                ),
                th.Property("affiliation", th.StringType),
                th.Property("methods", th.ArrayType(th.StringType)),
            )),
        ),
        th.Property("content", th.ObjectType(
            th.Property("trackers", th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType),
                    th.Property("count", th.IntegerType),
                )),
            ),
            th.Property("topics", th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType),
                    th.Property("duration", th.IntegerType),
                )),
            ),
            th.Property("pointsOfInterest", th.ObjectType(
                th.Property("actionItems", th.ArrayType(th.StringType)),
            )),
        )),
        th.Property("interaction", th.ObjectType(
            th.Property("speakers", th.ArrayType(
                th.ObjectType(
                    th.Property("id", th.StringType),
                    th.Property("userId", th.StringType),
                    th.Property("talkTime", th.NumberType),
                )),
            ),
            th.Property("interactionStats", th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType),
                    th.Property("value", th.NumberType),
                )),
            ),
            th.Property("video", th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType),
                    th.Property("duration", th.NumberType),
                )),
            ),
            th.Property("questions", th.ObjectType(
                th.Property("companyCount", th.IntegerType),
                th.Property("nonCompanyCount", th.IntegerType),
            )),
        )),
        th.Property("collaboration", th.ObjectType()),
    ).to_dict()

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        row["id"] = row["metaData"]["id"]
        row["started"] = row["metaData"]["started"]
        return row

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the data payload for the REST API request."""
        start_time = self.get_starting_timestamp(context).strftime('%Y-%m-%dT%H:%M:%SZ')
        request_body = {
            "filter": {
                "fromDateTime": start_time,
                "toDateTime": None,
                "cursor": next_page_token
            },
            "contentSelector": {
                "context": "Extended",
                "contextTiming": ["Now"],
                "exposedFields": {
                    "collaboration": {
                        "publicComments": True
                    },
                    "content": {
                        "pointsOfInterest": True,
                        "structure": True,
                        "topics": True,
                        "trackers": True
                    },
                    "interaction": {
                        "personInteractionStats": True,
                        "questions": True,
                        "speakers": True,
                        "video": True
                    },
                    "media": False,
                    "parties": True
                }
            }
        }
        return request_body

    def get_records(self, context: Optional[dict]):
        "Overwrite default method to return both the record and child context."
        for row in self.request_records(context):
            row = self.post_process(row, context)
            child_context = {"call_id": row["id"]}
            yield (row, child_context)