variable "name" {
  type = string
}

variable "vpc_cidr" {
  type = string
}

variable "availability_zones" {
  type = list(string)
}

variable "cluster_version" {
  type = string
}

variable "node_instance_types" {
  type = list(string)
}

variable "tags" {
  type = map(string)
}

variable "oidc_thumbprint" {
  type = string
}

variable "alb_controller_policy_arn" {
  type = string
}