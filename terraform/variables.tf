variable "aws_region" {
  default = "eu-west-2"
  description = ""
}

variable "s3_endpoint" {
  default = "http://localhost:4566"
  description = ""
}

variable "bucket_name" {
  default = "pageview-data"
  description = ""
}

variable "metrics_namespace" {
  default     = "PageviewPipeline"
  description = "CloudWatch namespace for application metrics"
}

variable "alarm_name_prefix" {
  default     = "pageview-pipeline"
  description = "Prefix for CloudWatch alarm names"
}

variable "alarm_sns_topic_arn" {
  default     = ""
  description = "SNS topic ARN for alarm notifications (optional - leave empty to create one)"
}

variable "create_sns_topic" {
  default     = false
  description = "Create SNS topic for alarm notifications when alarm_sns_topic_arn is empty"
}

variable "alert_email" {
  default     = ""
  description = "Email to subscribe to SNS topic (required when create_sns_topic=true)"
}

variable "app_dimensions" {
  default     = {}
  description = "Dimensions to filter metrics (e.g. application, environment)"
}

variable "enable_kafka_streams_alarm" {
  default     = false
  description = "Enable Kafka Streams state alarm (requires app to publish kafka_streams_state gauge)"
}

variable "enable_cloudwatch_alarms" {
  default     = false
  description = "Create CloudWatch alarms (set true when deploying to real AWS; requires valid credentials)"
}

variable "s3_error_rate_threshold" {
  default     = 1
  description = "S3 write errors in period to trigger alarm (1 = any error)"
}

variable "s3_error_rate_period_seconds" {
  default     = 300
  description = "S3 error rate evaluation period in seconds (default 5 min)"
}

variable "s3_error_rate_evaluation_periods" {
  default     = 1
  description = "Number of periods S3 errors must exceed threshold"
}

variable "kafka_consumer_lag_threshold" {
  default     = 10000
  description = "Kafka consumer lag (records) to trigger alarm"
}