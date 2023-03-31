from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks
from fastapi.responses import FileResponse, UJSONResponse
from pydantic import BaseModel

from .report import generate_report, get_report_status


class Report(BaseModel):
    """Report model for /trigger_report and /get_report endpoints"""

    report_id: str


# Create a new router
router = APIRouter()


# /trigger_report endpoint that will trigger report generation from the data
# provided (stored in DB)
@router.get(
    "/trigger_report",
    summary="Trigger report generation and get report ID",
    response_model=Report,
    responses={
        200: {"description": "Report ID"},
        500: {"description": "Internal Server Error"},
    },
)
async def trigger_report(background_tasks: BackgroundTasks):
    report_id = str(uuid4())
    background_tasks.add_task(generate_report, report_id)
    return Report(report_id=report_id)


# /get_report endpoint that will return the status of the report or the csv
# file if the report is ready
@router.post(
    "/get_report",
    summary="Get report status or CSV file",
    response_class=UJSONResponse,
    responses={
        200: {"description": "Report status or CSV file"},
        404: {"description": "Report not found"},
        500: {"description": "Internal Server Error"},
    },
)
async def get_report(report: Report):
    status = get_report_status(report.report_id)
    if not status:
        return "Report not found", 404
    if status == "Running":
        return "Running"
    return FileResponse(status, media_type="text/csv")
