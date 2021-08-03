
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_gong.client import GongStream


class CallsStream(GongStream):
    """Define custom stream."""
    name = "calls"
    path = "/v2/calls"
    primary_keys = ["id"]
    replication_key = "started"
    records_jsonpath = "$.calls[*]"

    schema = th.PropertiesList(
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
    ).to_dict()

    def get_records(self, context: Optional[dict]):
        "Overwrite default method to return both the record and child context."
        for row in self.request_records(context):
            row = self.post_process(row, context)
            child_context = {"call_id": row["id"]}
            yield (row, child_context)

