from dateutil import parser
from dateutil.relativedelta import *
from datetime import datetime


date_time_format_string = "%Y-%m-%dT%H:%M:%SZ"
date_format_string = "%Y-%m-%d"
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
        return parser.parse(parser.parse(conf_date_time).strftime(date_time_format_string))
    return None


def extended_config_validation(config):
    """Date validation (match with gong API documentation):
        Both start_date(date and time) and end_date(date time) are optional and start_date can be equal or less than
        end_date.We are validating if date can be parsed from the provided config parameters.
    """
    try:
        start_date = get_date_time_value_from_config(config, start_date_key)
        end_date = get_date_time_value_from_config(config, end_date_key)
        if start_date and end_date and start_date > end_date:
            raise BaseException('Invalid date range in configuration. Please provide correct date range.')
    except Exception as e:
        raise BaseException(
            f'Configuration error: Invalid date found in configuration file: \n"{e}"')


def get_stats_dates_from_config(config):
    """
    Stats filter date:
    As per gong API documentation, stats date filter has the following requirements:
    1. fromDate and toDate are required and supports format "YYYY-MM-DD"
    2. fromDate is inclusive and must be less than toDate
    3. toDate is exclusive and cannot be greater than current date

    Dates are retrieved to satisfy the above criteria.
    1. If no date provided for start_date in config, then python min date is used for fromDate
    2. If no date provided for end_date in config, then current date is used for toDate
    3. If same date provided for both start_date and end_date in config, fromDate will be decreased to one day
    4. If end_date provided in config is greater than current date then current date will be used for toDate
    """
    start_date = get_date_time_value_from_config(config, start_date_key)
    end_date = get_date_time_value_from_config(config, end_date_key)
    current_date = datetime.now().date()
    stats_from_date = start_date.date() if start_date else datetime.min.date()
    stats_to_date = end_date.date() if end_date else current_date
    if stats_to_date > current_date:
        stats_to_date = current_date
    if stats_from_date >= stats_to_date:
        stats_from_date = stats_from_date + relativedelta(days=-1)
    return {
        "stats_from_date": stats_from_date.strftime(date_format_string),
        "stats_to_date": stats_to_date.strftime(date_format_string)
    }
