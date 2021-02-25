import calendar
import time
from datetime import datetime, timedelta


def generate_milliseconds_epoch_from_timestamp(
    formatted_timestamp_string, minutes_to_add=0
):
    """Returns the 1970 epoch as a number from the given timestamp.

    Keyword arguments:
    timestamp_string -- the timestamp as a string formatted to %Y-%m-%dT%H:%M:%S.%f%z
    minutes_to_add -- if any minutes are to be added to the time, set to greater than 0
    """
    timestamp = datetime.strptime(formatted_timestamp_string, "%Y-%m-%dT%H:%M:%S.%f")
    if minutes_to_add > 0:
        timestamp = timestamp + timedelta(minutes=minutes_to_add)

    return int((timestamp - datetime(1970, 1, 1)).total_seconds() * 1000.0)


def add_milliseconds_to_timestamp(timestamp, number_of_milliseconds, add_offset):
    """Returns the timestamp formatted for ingestion as a string.

    Keyword arguments:
    timestamp -- the timestamp to use as a base
    number_of_milliseconds -- the amount of milliseconds to add
    add_offset -- True to add the offset to the returned timestamp string
    """
    timestamp_edited = timestamp + timedelta(milliseconds=number_of_milliseconds)
    timestamp_with_three_digit_microseconds = timestamp_edited.strftime(
        "%Y-%m-%dT%H:%M:%S.%f"
    )[:-3]

    offset = "+0000" if add_offset else ""
    return (
        timestamp_edited,
        "{0}{1}".format(timestamp_with_three_digit_microseconds, offset),
    )


def format_time_to_timezome_free(timestamp):
    """Returns the timestamp formatted for timezone free time.

    Keyword arguments:
    timestamp -- the timestamp to format
    """
    if timestamp.endswith("+0000"):
        timestamp = timestamp.replace("+0000", "")
    elif timestamp.endswith("Z"):
        timestamp = timestamp[:-1]

    timestamp_object = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f")
    timestamp_string = timestamp_object.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]

    return f"{timestamp_string}Z"


def get_current_epoch_seconds():
    """Gets the current date/time epoch in seconds"""
    return calendar.timegm(time.gmtime())
