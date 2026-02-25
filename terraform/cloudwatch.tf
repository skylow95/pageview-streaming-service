resource "aws_sns_topic" "alerts" {
  count = var.enable_cloudwatch_alarms && var.create_sns_topic ? 1 : 0

  name = "${var.alarm_name_prefix}-alerts"
}

resource "aws_sns_topic_subscription" "alerts_email" {
  count = var.enable_cloudwatch_alarms && var.create_sns_topic && var.alert_email != "" ? 1 : 0

  topic_arn = aws_sns_topic.alerts[0].arn
  protocol  = "email"
  endpoint  = var.alert_email
}

locals {
  alarm_actions = var.enable_cloudwatch_alarms ? (var.alarm_sns_topic_arn != "" ? [var.alarm_sns_topic_arn] : (var.create_sns_topic ? [aws_sns_topic.alerts[0].arn] : [])) : []
}

resource "aws_cloudwatch_metric_alarm" "s3_error_rate" {
  count = var.enable_cloudwatch_alarms ? 1 : 0

  alarm_name          = "${var.alarm_name_prefix}-s3-error-rate"
  alarm_description   = "S3 write error rate above threshold (${var.s3_error_rate_threshold}+ errors per ${var.s3_error_rate_period_seconds}s)"
  alarm_actions       = local.alarm_actions
  ok_actions         = local.alarm_actions
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = var.s3_error_rate_evaluation_periods
  metric_name         = "s3_errors"
  namespace           = var.metrics_namespace
  period              = var.s3_error_rate_period_seconds
  statistic           = "Sum"
  threshold           = var.s3_error_rate_threshold
  treat_missing_data  = "notBreaching"

  dimensions = var.app_dimensions
}

resource "aws_cloudwatch_metric_alarm" "kafka_consumer_lag" {
  count = var.enable_cloudwatch_alarms ? 1 : 0

  alarm_name          = "${var.alarm_name_prefix}-kafka-consumer-lag"
  alarm_description   = "Kafka consumer lag exceeds ${var.kafka_consumer_lag_threshold} records"
  alarm_actions       = local.alarm_actions
  ok_actions         = local.alarm_actions
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "kafka_consumer_fetch_manager_records_lag_max"
  namespace           = var.metrics_namespace
  period              = 60
  statistic           = "Maximum"
  threshold           = var.kafka_consumer_lag_threshold
  treat_missing_data   = "notBreaching"

  dimensions = var.app_dimensions
}

resource "aws_cloudwatch_metric_alarm" "kafka_streams_down" {
  count = var.enable_cloudwatch_alarms && var.enable_kafka_streams_alarm ? 1 : 0

  alarm_name          = "${var.alarm_name_prefix}-kafka-streams-down"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 2
  metric_name         = "kafka_streams_state"
  namespace           = var.metrics_namespace
  period              = 60
  statistic           = "Minimum"
  threshold           = 1
  treat_missing_data = "breaching"
  alarm_description  = "Kafka Streams is not in RUNNING state"
  alarm_actions      = local.alarm_actions
  ok_actions        = local.alarm_actions

  dimensions = var.app_dimensions
}
