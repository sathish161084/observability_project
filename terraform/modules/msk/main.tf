resource "aws_security_group" "msk" {
  name   = "${var.name}-msk"
  vpc_id = var.vpc_id
  ingress {
    from_port   = 9092
    to_port     = 9098
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
resource "aws_msk_configuration" "this" {
  kafka_versions = [var.kafka_version]
  name           = "${var.name}-config"
  server_properties = <<-EOT
auto.create.topics.enable=true
num.partitions=3
default.replication.factor=2
min.insync.replicas=2
  EOT
}
resource "aws_msk_cluster" "this" {
  cluster_name           = "${var.name}-msk"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = 2
  broker_node_group_info {
    instance_type   = var.broker_instance_type
    client_subnets  = var.private_subnet_ids
    security_groups = [aws_security_group.msk.id]
    storage_info {
      ebs_storage_info {
        volume_size = 100
      }
    }
  }
  configuration_info {
    arn      = aws_msk_configuration.this.arn
    revision = aws_msk_configuration.this.latest_revision
  }
  encryption_info {
    encryption_in_transit {
      client_broker = "TLS_PLAINTEXT"
      in_cluster    = true
    }
  }
}
output "bootstrap_brokers" { value = aws_msk_cluster.this.bootstrap_brokers }
output "bootstrap_brokers_tls" { value = aws_msk_cluster.this.bootstrap_brokers_tls }
