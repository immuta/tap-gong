from typing import Any, Dict, Optional, Union, List, Iterable
from singer_sdk import typing as th
from tap_gong import config_helper as helper
from tap_gong.client import GongStream


class CallsStream(GongStream):
    "Stream for all call data"
    name = "calls"
    path = "/v2/calls/extensive"
    primary_keys = ["id"]
    records_jsonpath = "$.calls[*]"
    next_page_token_jsonpath = "$.records.cursor"
    rest_method = "POST"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("started", th.DateTimeType),
        th.Property(
            "metaData",
            th.ObjectType(
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
                th.Property("purpose", th.StringType),
                th.Property("meetingUrl", th.StringType),
                th.Property("isPrivate", th.BooleanType),
            ),
        ),
        th.Property(
            "context",
            th.ArrayType(
                th.ObjectType(
                    th.Property("system", th.StringType),
                    th.Property(
                        "objects",
                        th.ArrayType(
                            th.ObjectType(
                                th.Property("objectType", th.StringType),
                                th.Property("objectId", th.StringType),
                                th.Property("timing", th.StringType),
                                th.Property(
                                    "fields",
                                    th.ArrayType(
                                        th.ObjectType(
                                            th.Property("name", th.StringType),
                                            th.Property("value", th.StringType),
                                        )
                                    ),
                                ),
                            )
                        ),
                    ),
                )
            ),
        ),
        th.Property(
            "parties",
            th.ArrayType(
                th.ObjectType(
                    th.Property("id", th.StringType),
                    th.Property("emailAddress", th.StringType),
                    th.Property("name", th.StringType),
                    th.Property("title", th.StringType),
                    th.Property("userId", th.StringType),
                    th.Property("speakerId", th.StringType),
                    th.Property(
                        "context",
                        th.ArrayType(
                            th.ObjectType(
                                th.Property("system", th.StringType),
                                th.Property(
                                    "objects",
                                    th.ArrayType(
                                        th.ObjectType(
                                            th.Property("objectType", th.StringType),
                                            th.Property("objectId", th.StringType),
                                            th.Property(
                                                "fields",
                                                th.ArrayType(
                                                    th.ObjectType(
                                                        th.Property(
                                                            "name", th.StringType
                                                        ),
                                                        th.Property(
                                                            "value", th.StringType
                                                        ),
                                                    )
                                                ),
                                            ),
                                        )
                                    ),
                                ),
                            )
                        ),
                    ),
                    th.Property("affiliation", th.StringType),
                    th.Property("phoneNumber", th.StringType),
                    th.Property("methods", th.ArrayType(th.StringType)),
                )
            ),
        ),
        th.Property(
            "content",
            th.ObjectType(
                th.Property(
                    "trackers",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("name", th.StringType),
                            th.Property("count", th.IntegerType),
                            th.Property("type", th.StringType)
                        )
                    ),
                ),
                th.Property(
                    "topics",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("name", th.StringType),
                            th.Property("duration", th.IntegerType),
                        )
                    ),
                ),
                th.Property(
                    "pointsOfInterest",
                    th.ObjectType(
                        th.Property(
                            "actionItems",
                            th.ArrayType(
                                th.ObjectType(
                                    th.Property("snippetStartTime", th.NumberType),
                                    th.Property("snippetEndTime", th.NumberType),
                                    th.Property("speakerID", th.StringType),
                                    th.Property("snippet", th.StringType),
                                )
                            ),
                        ),
                    ),
                ),
            ),
        ),
        th.Property(
            "interaction",
            th.ObjectType(
                th.Property(
                    "speakers",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("id", th.StringType),
                            th.Property("userId", th.StringType),
                            th.Property("talkTime", th.NumberType),
                        )
                    ),
                ),
                th.Property(
                    "interactionStats",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("name", th.StringType),
                            th.Property("value", th.StringType),
                        )
                    ),
                ),
                th.Property(
                    "video",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("name", th.StringType),
                            th.Property("duration", th.NumberType),
                        )
                    ),
                ),
                th.Property(
                    "questions",
                    th.ObjectType(
                        th.Property("companyCount", th.IntegerType),
                        th.Property("nonCompanyCount", th.IntegerType),
                    ),
                ),
            ),
        ),
        # th.Property("collaboration", th.ObjectType()),
    ).to_dict()

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        row["id"] = row["metaData"]["id"]
        row["started"] = row["metaData"]["started"]


        # some name / value pairs are returned where value = 'string' but others are value = 5
        # this breaks our schema - cast them all to string
        self.process_recursively(row, 'value', lambda n: str(n))
        return row

    def process_recursively(self, search_dict, field, op):
        """
        Takes a dict with nested lists and dicts,
        and searches all dicts for a key of the field
        provided.
        """
        fields_found = []

        for key, value in search_dict.items():

            if key == field:
                fields_found.append(value)
                search_dict['value'] = op(value)

            elif isinstance(value, dict):
                results = self.process_recursively(value, field, op)
                for result in results:
                    fields_found.append(result)

            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        more_results = self.process_recursively(item, field, op)
                        for another_result in more_results:
                            fields_found.append(another_result)

        return fields_found

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the data payload for the REST API request."""
        fromDateTime = helper.get_date_time_string(self.get_starting_timestamp(context), helper.date_time_format_string)
        toDateTime = helper.get_date_time_string_from_config(self.config, helper.end_date_key,
                                                             helper.date_time_format_string)
        request_body = {
            "cursor": next_page_token,
            "filter": {"fromDateTime": fromDateTime, "toDateTime": toDateTime},
            "contentSelector": {
                "context": "Extended",
                "contextTiming": ["Now"],
                "exposedFields": {
                    "collaboration": {"publicComments": True},
                    "content": {
                        "pointsOfInterest": True,
                        "structure": True,
                        "topics": True,
                        "trackers": True,
                    },
                    "interaction": {
                        "personInteractionStats": True,
                        "questions": True,
                        "speakers": True,
                        "video": True,
                    },
                    "media": False,
                    "parties": True,
                },
            },
        }
        return request_body

    def get_records(self, context: Optional[dict]):
        "Overwrite default method to return both the record and child context."
        for row in self.request_records(context):
            row = self.post_process(row, context)
            child_context = {"call_id": row["id"]}
            yield (row, child_context)
