variable "aws_region" { type = string default = "eu-west-1" }
variable "cluster_name" { type = string default = "three-tier-eks-prod-starter" }
variable "availability_zones" { type = list(string) default = ["eu-west-1a","eu-west-1b","eu-west-1c"] }
variable "enable_msk" { type = bool default = true }
