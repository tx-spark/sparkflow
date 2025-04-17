from pipelines.flows.tx_leg import tx_leg_pipeline

tx_leg_pipeline.serve(
  name="tx_leg_pipeline",
  cron="0 21 * * *",
  timezone="America/Chicago"
)