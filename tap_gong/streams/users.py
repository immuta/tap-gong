from typing import Any, Dict, Optional, Union, List, Iterable
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_gong.client import GongStream


class UsersStream(GongStream):
    name = "users"
    path = "/v2/users"
    primary_keys = ["id"]
    records_jsonpath = "$.users[*]"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("emailAddress", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("active", th.BooleanType),
        th.Property("emailAliases", th.ArrayType(th.StringType)),
        th.Property("firstName", th.StringType),
        th.Property("lastName", th.StringType),
        th.Property("title", th.StringType),
        th.Property("phoneNumber", th.StringType),
        th.Property("extension", th.StringType),
        th.Property("personalMeetingUrls", th.ArrayType(th.StringType)),
        th.Property("settings", th.ObjectType(
            th.Property("webConferencesRecorded", th.BooleanType),
            th.Property("preventWebConferenceRecording", th.BooleanType),
            th.Property("telephonyCallsImported", th.BooleanType),
            th.Property("emailsImported", th.BooleanType),
            th.Property("preventEmailImport", th.BooleanType),
            th.Property("nonRecordedMeetingsImported", th.BooleanType),
        )),
        th.Property("managerId", th.StringType),
        th.Property("meetingConsentPageUrl", th.StringType),
    ).to_dict()

