from common_code.config import get_settings
from common_code.logger.logger import get_logger, Logger
from common_code.service.models import Service
from common_code.service.enums import ServiceStatus
from common_code.common.enums import FieldDescriptionType, ExecutionUnitTagName, ExecutionUnitTagAcronym
from common_code.common.models import FieldDescription, ExecutionUnitTag
from common_code.tasks.models import TaskData
# Imports required by the service's model
import pandas as pd
import tempfile
import zipfile
import io
from ydata_profiling import ProfileReport

api_description = """YData Profiling
This service will produce a complete Exploratory Data Analysis (EDA) on a csv file and export it to a zipped html file.
"""
api_summary = """YData Profiling
A simple Exploratory Data Analysis (EDA) tool.
"""

api_title = "Text to Speech API."
version = "0.0.1"

settings = get_settings()


class MyService(Service):
    """
    YData Profiling model
    """

    # Any additional fields must be excluded for Pydantic to work
    _logger: Logger

    def __init__(self):
        super().__init__(
            name="YData Profiling",
            slug="ydata-profiling",
            url=settings.service_url,
            summary=api_summary,
            description=api_description,
            status=ServiceStatus.AVAILABLE,
            data_in_fields=[
                FieldDescription(
                    name="csv",
                    type=[FieldDescriptionType.TEXT_CSV],
                ),
            ],
            data_out_fields=[
                FieldDescription(
                    name="result", type=[FieldDescriptionType.APPLICATION_ZIP]
                ),
            ],
            tags=[
                ExecutionUnitTag(
                    name=ExecutionUnitTagName.DATA_PREPROCESSING,
                    acronym=ExecutionUnitTagAcronym.DATA_PREPROCESSING,
                ),
            ],
            has_ai=False,
            docs_url="https://docs.swiss-ai-center.ch/reference/services/ydata-profiling/",
        )
        self._logger = get_logger(settings)

    def process(self, data):
        raw = data["csv"].data
        df = pd.read_csv(io.BytesIO(raw))

        profile = ProfileReport(df, title="Profiling Report")

        with tempfile.TemporaryDirectory() as tmpdirname:
            target_file = tmpdirname + "/report.html"
            profile.to_file(target_file)
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(
                    zip_buffer, "a", zipfile.ZIP_DEFLATED, False
            ) as zipper:
                zipper.write(target_file, arcname="report.html")
            zip_buffer.seek(0)
            return {
                "result": TaskData(
                    data=zip_buffer.read(),
                    type=FieldDescriptionType.APPLICATION_ZIP,
                )
            }

