"""Gong tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_gong.streams import (
    CallsStream,
    CallTranscriptsStream,
    UsersStream,
    InteractionStatsStream,
    AggregatedActivityStream
)

STREAM_TYPES = [
    CallsStream,
    CallTranscriptsStream,
    UsersStream,
    InteractionStatsStream,
    AggregatedActivityStream
]


class TapGong(Tap):
    """Gong tap class."""
    name = "tap-gong"

    config_jsonschema = th.PropertiesList(
        th.Property("access_key", th.StringType, required=True),
        th.Property("access_key_secret", th.StringType, required=True),
        th.Property("start_date", th.DateTimeType, default=None),
        th.Property("end_date", th.DateTimeType, default=None),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapGong.cli()
