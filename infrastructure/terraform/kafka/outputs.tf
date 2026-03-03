output "bootstrap_brokers_tls" {
  description = "TLS bootstrap broker connection string"
  value       = aws_msk_cluster.graphrag.bootstrap_brokers_tls
}

output "bootstrap_brokers_plaintext" {
  description = "Plaintext bootstrap broker connection string (dev only)"
  value       = aws_msk_cluster.graphrag.bootstrap_brokers
}

output "cluster_arn" {
  description = "ARN of the MSK cluster"
  value       = aws_msk_cluster.graphrag.arn
}

output "zookeeper_connect_string" {
  description = "ZooKeeper connection string"
  value       = aws_msk_cluster.graphrag.zookeeper_connect_string
}

output "current_version" {
  description = "Current running version of the MSK cluster"
  value       = aws_msk_cluster.graphrag.current_version
}
