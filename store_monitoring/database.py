import csv
import logging
from datetime import datetime, time, timedelta
from os.path import join

from .utils import convert_utc_to_local, time_range


logger = logging.getLogger(__name__)


def create_db(conn) -> None:
    """
    Create database tables
    store_data: Contains store status data
    menu_hours: Contains store business hours
    store_tz: Contains store timezone
    """
    with conn.cursor() as cur:
        logger.info("Creating database")
        cur.execute(
            """CREATE TABLE IF NOT EXISTS store_data (
            store_id BIGINT,
            status TEXT,
            timestamp_utc TIMESTAMPTZ
        );"""
        )
        cur.execute(
            """CREATE TABLE IF NOT EXISTS menu_hours (
                store_id BIGINT,
                dayofweek INT,
                start_time_local TIME,
                end_time_local TIME
            );"""
        )
        cur.execute(
            """CREATE TABLE IF NOT EXISTS store_tz (
                store_id BIGINT,
                timezone TEXT
            );"""
        )
        cur.execute(
            """CREATE TABLE IF NOT EXISTS reports (
                report_id TEXT,
                status TEXT
            );"""
        )
        conn.commit()
        logger.info("Database created.")


def init_db(conn) -> None:
    """
    Initialize database with data from CSV files
    """
    cur = conn.cursor()
    create_db(conn)
    logger.info("Initializing database")
    #  check if data is already there
    #  In all three tables. If yes, exit
    cur.execute(
        """SELECT CASE
         WHEN EXISTS (SELECT * FROM menu_hours LIMIT 1) THEN 1
         ELSE 0
       END"""
    )
    menu_hour_exists = cur.fetchone()
    cur.execute(
        """SELECT CASE
            WHEN EXISTS (SELECT * FROM store_tz LIMIT 1) THEN 1
            ELSE 0
        END"""
    )
    store_tz_exists = cur.fetchone()
    cur.execute(
        """SELECT CASE
            WHEN EXISTS (SELECT * FROM store_data LIMIT 1) THEN 1
            ELSE 0
        END"""
    )
    store_data_exists = cur.fetchone()
    if menu_hour_exists and store_tz_exists and store_data_exists:
        logger.info("Database already initialized")
        return
    init_menu_hours = """COPY menu_hours (
        store_id, dayOfWeek, start_time_local, end_time_local
        ) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');"""
    init_store_tz = """COPY store_tz (
        store_id, timezone
        ) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');"""
    try:
        if not menu_hour_exists:
            with open("Menu hours.csv", "r") as csv_file:
                cur.copy_expert(init_menu_hours, csv_file)
        if not store_tz_exists:
            with open("bq.csv", "r") as csv_file:
                cur.copy_expert(init_store_tz, csv_file)
        if not store_data_exists:
            update_db(conn)
        conn.commit()
        logger.info("Database initialized")
    except Exception as e:
        logger.error(e)
    finally:
        cur.close()


def update_db(conn) -> None:
    """
    Update database with data from CSV file by comparing
    the data in the CSV file with the data in the database.
    """
    cur = conn.cursor()
    try:
        logger.info("Updating database")
        cur.execute(
            """CREATE TEMPORARY TABLE IF NOT EXISTS tmp_store_data (
            store_id BIGINT,
            status TEXT,
            timestamp_utc TIMESTAMPTZ
        );"""
        )
        # Define the SQL query to execute
        query = """COPY tmp_store_data (
            store_id, status, timestamp_utc
            ) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');"""

        # Open the CSV file and execute the query
        with open("store status.csv", "r") as csv_file:
            cur.copy_expert(query, csv_file)
        conn.commit()

        #  Update store_status table with store_data table
        cur.execute(
            """INSERT INTO store_data (store_id, timestamp_utc, status)
            SELECT store_id, timestamp_utc, status
            FROM tmp_store_data
            WHERE NOT EXISTS (
                SELECT 1 FROM store_data
                WHERE store_data.store_id = tmp_store_data.store_id
                AND store_data.timestamp_utc = tmp_store_data.timestamp_utc
                AND store_data.status = tmp_store_data.status
            );"""
        )
        cur.execute("DROP TABLE tmp_store_data;")
        conn.commit()
        logger.info("Database updated")
    except Exception as e:
        logger.error(e)

    finally:
        cur.close()


def fetch_store_ids(conn) -> list[int]:
    """
    Fetch store IDs from database

    Args:
        conn (psycopg2.connection): Database

    Returns:
        list: List of store IDs
    """
    with conn.cursor() as cur:
        logger.debug("Fetching store IDs")
        cur.execute("SELECT store_id FROM menu_hours")
        rows = cur.fetchall()
        store_ids = [row[0] for row in rows]
    return store_ids


def fetch_store_hours(conn, store_id: int) -> dict[int, tuple]:
    """
    Fetch store hours from database using store_id

    Args:
        conn (psycopg2.connection): Database psycopg2.connection
        store_id (int): Store ID

    Returns:
        dict: Dictionary containing store hours
    """
    with conn.cursor() as cur:
        logger.debug("Fetching store hours")
        cur.execute(
            """SELECT *
            FROM menu_hours
            WHERE store_id = %s""",
            (store_id,),
        )
        rows = cur.fetchall()
        hours = {}
        for row in rows:
            hours[row[0]] = (row[1], row[2])
    return hours


def fetch_store_timezone(conn, store_id) -> str:
    """
    Fetch store timezone from database using store_id

    Args:
        conn (psycopg2.connection): Database psycopg2.connection
        store_id (int): Store ID

    Returns:
        str: Store timezone
    """
    with conn.cursor() as cur:
        logger.debug("Fetching store timezone")
        cur.execute(
            """SELECT timezone
            FROM store_tz
            WHERE store_id = %s""",
            (store_id,),
        )
        row = cur.fetchone()
        if row:
            timezone_str = row[0]
        else:
            timezone_str = "America/Chicago"  # Default timezone
    return timezone_str


def fetch_store_data(
    conn,
    store_id,
    start_time,
    end_time,
) -> list[tuple[datetime, str]]:
    """
    Fetch store status data from database using store_id

    Args:
        conn (psycopg2.connection): Database psycopg2.connection
        store_id (int): Store ID
        start_time (datetime): Start time
        end_time (datetime): End time

    Returns:
        list: List of tuples containing store status data
    """
    with conn.cursor() as cur:
        logger.debug("Fetching store status data")
        cur.execute(
            """SELECT timestamp_utc, status
            FROM store_data
            WHERE store_id = %s
            AND timestamp_utc >= %s
            AND timestamp_utc < %s""",
            (store_id, start_time, end_time),
        )
        rows = cur.fetchall()
    return rows


def calculate_uptime_downtime(
    conn,
    store_id: int,
    start_time: datetime,
    end_time: datetime,
) -> dict[str, float]:
    """
    Calculate uptime and downtime for a given store

    Args:
        conn (psycopg2.connection): Database psycopg2.connection
        store_id (int): Store ID
        start_time (datetime): Start time
        end_time (datetime): End time

    Returns:
        dict: Dictionary containing uptime and downtime

    Logic:
        1. Fetch store hours from database
        2. Fetch store status data from database
        3. Iterate over each hour in the time range of business hours
        4. Check if store is open during the hour
        5. Compute uptime and downtime for the hour
        6. Compute uptime and downtime for the day
        7. Compute uptime and downtime for the week
    """
    store_timezone = fetch_store_timezone(conn, store_id)
    store_data = fetch_store_data(conn, store_id, start_time, end_time)
    hours = fetch_store_hours(conn, store_id)

    # Create datetime objects for the start and end times in local timezone
    local_start_time = convert_utc_to_local(start_time, store_timezone)
    local_end_time = convert_utc_to_local(end_time, store_timezone)

    # Initialize uptime and downtime counters
    uptime_minutes_hour = 0
    downtime_minutes_hour = 0
    uptime_minutes_day = 0
    downtime_minutes_day = 0
    uptime_minutes_week = 0
    downtime_minutes_week = 0

    # Iterate over each hour in the time range
    delta = timedelta(hours=1)
    for hour_start_time in time_range(local_start_time, local_end_time, delta):
        hour_end_time = hour_start_time + delta

        # Check if the hour is within business hours
        hour_dow = hour_start_time.weekday()
        try:
            hour_business_start_time, hour_business_end_time = hours[hour_dow]
        except KeyError:
            #  Store is open 24 hours
            hour_business_start_time = time(0, 0)
            hour_business_end_time = time(23, 59, 59)
        hour_uptime = 0
        hour_downtime = 0
        if (
            hour_start_time.time() < hour_business_end_time
            and hour_end_time.time() > hour_business_start_time
        ):
            # Calculate uptime and downtime within the hour
            for data_timestamp_utc, data_status in store_data:
                data_local_timestamp = convert_utc_to_local(
                    data_timestamp_utc, store_timezone
                )
                if (
                    data_local_timestamp >= hour_start_time
                    and data_local_timestamp < hour_end_time
                ):
                    if data_status == "active":
                        hour_uptime += (
                            data_local_timestamp
                            + delta
                            - max(data_local_timestamp, hour_start_time)
                        ).total_seconds() / 60
                    elif data_status == "inactive":
                        hour_downtime += (
                            data_local_timestamp
                            + delta
                            - max(data_local_timestamp, hour_start_time)
                        ).total_seconds() / 60

            # Add hour uptime and downtime to daily and weekly counters
            if (
                hour_start_time.time()
                >= datetime.strptime("09:00:00", "%H:%M:%S").time()
                and hour_start_time.time()
                < datetime.strptime("21:00:00", "%H:%M:%S").time()
            ):
                uptime_minutes_day += hour_uptime
                downtime_minutes_day += hour_downtime
            uptime_minutes_week += hour_uptime
            downtime_minutes_week += hour_downtime

        # Add hour uptime and downtime to hourly counters
        uptime_minutes_hour += hour_uptime
        downtime_minutes_hour += hour_downtime

    # Convert uptime and downtime to hours

    uptime_hours_last_day = uptime_minutes_day / 60
    downtime_hours_last_day = downtime_minutes_day / 60
    uptime_hours_last_week = uptime_minutes_week / 60
    downtime_hours_last_week = downtime_minutes_week / 60

    # Return calculated values as a dictionary
    return {
        "store_id": store_id,
        "uptime_last_hour": uptime_minutes_hour,
        "downtime_last_hour": downtime_minutes_hour,
        "uptime_last_day": uptime_hours_last_day,
        "downtime_last_day": downtime_hours_last_day,
        "uptime_last_week": uptime_hours_last_week,
        "downtime_last_week": downtime_hours_last_week,
    }


def generate_report(conn, report_id: str) -> None:
    """
    Generate uptime and downtime report for all stores as a CSV file.
    Also stores the report path in the database.

    Args:
        conn (psycopg2.connection): Database psycopg2.connection
    """
    # Fetch all stores_ids from database
    stores = fetch_store_ids(conn)

    #  Compute uptime and downtime for each store and append to a list
    report_data = []
    for store_id in stores:
        report_data.append(
            calculate_uptime_downtime(
                conn,
                store_id,
                datetime.utcnow() - timedelta(weeks=1),
                datetime.utcnow(),
            )
        )

    # Write report data to CSV file
    #  Get unique name for the report file by appending current timestamp
    report_name = join(
        "reports",
        f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
    )

    with open(report_name, "w") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "store_id",
                "uptime_last_hour",
                "downtime_last_hour",
                "uptime_last_day",
                "downtime_last_day",
                "uptime_last_week",
                "downtime_last_week",
            ],
        )
        logger.info(f"Writing report to {report_name}")
        writer.writeheader()
        writer.writerows(report_data)

    logger.info("Updating report status in database")

    with conn.cursor() as cur:
        cur.execute(
            """UPDATE reports
            SET status = %s
            WHERE report_id = %s""",
            (report_name, report_id),
        )
        conn.commit()

    logger.info(f"Report {report_id} generated successfully")
