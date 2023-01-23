from dateutil import parser
from dateutil.relativedelta import *
from datetime import datetime, timezone


date_time_format_string = "%Y-%m-%dT%H:%M:%SZ"
start_date_key = "start_date"
end_date_key = "end_date"


def get_date_time_string(value, format_string):
    if value:
        return value.strftime(format_string)
    return None


def get_date_time_string_from_config(config, key, format_string):
    return get_date_time_string(get_date_time_value_from_config(config, key), format_string)


def get_date_time_value_from_config(config, key):
    conf_date_time = config.get(key, '').strip()
    if conf_date_time:
        return parser.parse(conf_date_time)
    return None


def date_value_to_iso_string(time):
    # python datetime.isoformat() end with +00:00, but need Z
    # i.e. we need 2023-01-19T01:23:45Z instead of 2023-01-19T01:23:45+00:00
    return f'{time.isoformat()[:-6]}Z'


def extended_config_validation(config):
    """Date validation (match with gong API documentation):
        Both start_date(date and time) and end_date(date time) are required, check that they are valid.
    """
    try:
        start_date = get_date_time_value_from_config(config, start_date_key)
        end_date = get_date_time_value_from_config(config, end_date_key)
        if not start_date:
            raise BaseException(
                'Missing valid start date in configuration. Please provide correct start date.')
        if not end_date:
            raise BaseException(
                'Missing valid end date in configuration. Please provide correct end date.')
    except Exception as e:
        raise BaseException(
            f'Configuration error: Invalid date found in configuration file: \n"{e}"')


def get_stats_dates_from_config(config, retry=False):
    """
    Stats filter date:
    As per gong API documentation, stats date filter has the following requirements:
    1. fromDate and toDate are required and supports format "YYYY-MM-DD"
    2. fromDate is inclusive and must be less than toDate
    3. toDate is exclusive and cannot be greater than current date

    Dates are retrieved to satisfy the above criteria.
    1. If start_date provided in config is greater than or equal to the end_date then fromDate will be decreased by one day
    2. In the case of a retry request, we subtract 1 extra day to attempt to avoid UTC date conversion issue
    """
    start_date = get_date_time_value_from_config(config, start_date_key)
    end_date = get_date_time_value_from_config(config, end_date_key)
    now = datetime.now(timezone.utc) if not retry else datetime.now(
        timezone.utc) - relativedelta(days=1)
    if end_date > now:
        end_date = now
    if start_date >= end_date - relativedelta(days=1):
        start_date = end_date - relativedelta(days=1)
    return {
        "stats_from_date": date_value_to_iso_string(start_date),
        "stats_to_date": date_value_to_iso_string(end_date)
    }
