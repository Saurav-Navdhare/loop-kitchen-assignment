import logging

from . import conn
from .database import generate_report as generate_report_db

logger = logging.getLogger(__name__)


def get_report_status(report_id: str):
    with conn.cursor() as cur:
        try:
            cur.execute(
                """SELECT "status"
                FROM reports
                WHERE report_id = %s
                """,
                (report_id,),
            )
            rows = cur.fetchall()
            if not rows:
                return None
            return rows[0][0]
        except Exception as e:
            logger.error(e)
            conn.rollback()


def generate_report(report_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO reports (report_id, status)
            VALUES (%s, %s)
            """,
            (report_id, "Running"),
        )
        conn.commit()
    generate_report_db(conn, report_id)
