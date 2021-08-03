"""Stream type classes for tap-gong."""

import time
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_gong.client import GongStream

class CallDetailsStream(GongStream):
    """Define custom stream."""
    name = "calls"
    path = "/v2/calls/extensive"
    primary_keys = ["id"]
    records_jsonpath = "$.calls[*]"
    rest_method = "POST"

    schema = th.PropertiesList(
        th.Property("metaData", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("url", th.StringType),
            th.Property("title", th.StringType),
            th.Property("scheduled", th.StringType),
            th.Property("started", th.StringType),
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
                    th.Property("talkTime", th.FloatType),
                )),
            ),
            th.Property("interactionStats", th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType),
                    th.Property("value", th.FloatType),
                )),
            ),
            th.Property("video", th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType),
                    th.Property("duration", th.FloatType),
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
        return row

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the data payload for the REST API request."""
        request_body = {
            "filter": {
                "fromDateTime": self.config["start_date"],
                "toDateTime": None
            },
            "filter": {
                "fromDateTime": self.config["start_date"],
                "toDateTime": None
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

