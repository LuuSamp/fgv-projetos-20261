resource "aws_cloudwatch_event_rule" "glue_etl_schedule" {
  count = var.eventbridge_enabled ? 1 : 0

  name                = "${local.name_prefix}-glue-schedule"
  description         = "Weekly trigger for incremental classicmodels Glue ETL"
  schedule_expression = var.glue_schedule_cron
  tags                = local.tags
}

resource "aws_cloudwatch_event_target" "glue_etl" {
  count = var.eventbridge_enabled ? 1 : 0

  rule      = aws_cloudwatch_event_rule.glue_etl_schedule[0].name
  target_id = "StartGlueJob"
  arn       = aws_glue_job.etl.arn
  role_arn  = data.aws_iam_role.glue.arn
}
