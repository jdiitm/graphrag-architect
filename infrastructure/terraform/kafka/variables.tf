variable "cluster_name" {
  description = "Name of the MSK cluster"
  type        = string
  default     = "graphrag-kafka"
}

variable "kafka_version" {
  description = "Apache Kafka version for the MSK cluster"
  type        = string
  default     = "3.6.0"
}

variable "number_of_broker_nodes" {
  description = "Number of broker nodes in the cluster (must be multiple of AZs)"
  type        = number
  default     = 3
}

variable "instance_type" {
  description = "EC2 instance type for Kafka brokers"
  type        = string
  default     = "kafka.m5.large"
}

variable "ebs_volume_size" {
  description = "EBS volume size in GiB per broker"
  type        = number
  default     = 100
}

variable "subnet_ids" {
  description = "List of subnet IDs for broker placement"
  type        = list(string)
}

variable "security_group_ids" {
  description = "List of security group IDs for the brokers"
  type        = list(string)
}

variable "kms_key_arn" {
  description = "ARN of the KMS key for at-rest encryption"
  type        = string
  default     = ""
}

variable "log_retention_hours" {
  description = "Kafka log retention in hours"
  type        = number
  default     = 168
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 30
}

variable "tags" {
  description = "Additional tags to apply to all resources"
  type        = map(string)
  default     = {}
}
