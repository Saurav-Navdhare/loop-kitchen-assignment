from datetime import datetime, timedelta

import pytz


# Function to get the start and end times for the previous hour
def get_previous_hour_times() -> tuple[datetime, datetime]:
    end_time = datetime.utcnow().replace(second=0, microsecond=0)
    start_time = end_time - timedelta(hours=1)
    return start_time, end_time


# Function to get the start and end times for the previous day
def get_previous_day_times():
    end_time = datetime.utcnow().replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    start_time = end_time - timedelta(days=1)
    return start_time, end_time


# Function to get the start and end times for the previous week
def get_previous_week_times() -> tuple[datetime, datetime]:
    end_time = datetime.utcnow().replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    start_time = end_time - timedelta(weeks=1)
    return start_time, end_time


# Function to convert a UTC timestamp to a timezone-aware datetime object
def convert_utc_to_local(
    utc_timestamp: datetime,
    timezone_str: str,
) -> datetime:
    utc = pytz.utc
    local_tz = pytz.timezone(timezone_str)
    utc_dt = utc.localize(utc_timestamp)
    local_dt = utc_dt.astimezone(local_tz)
    return local_dt


# Iterate over each hour in the time range
def time_range(
    start_time: datetime,
    end_time: datetime,
    step=timedelta(hours=1),
):
    while start_time < end_time:
        yield start_time
        start_time += step
