from pipelines.flows.tx_leg import tx_leg_pipeline

from datetime import timedelta, datetime
from prefect.schedules import Interval

tx_leg_pipeline.serve(
  name="tx_leg_pipeline",
  schedule=Interval(
    timedelta(hours=24),
    anchor_date=datetime(2025, 1, 1, 21, 0),
    timezone="America/Chicago"
  )
)