module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "5.13.0"

  name = "${var.name}-vpc"
  cidr = var.vpc_cidr

  azs = slice(var.availability_zones, 0, 2)

  public_subnets = [
    cidrsubnet(var.vpc_cidr, 4, 0),
    cidrsubnet(var.vpc_cidr, 4, 1)
  ]

  private_subnets = [
    cidrsubnet(var.vpc_cidr, 4, 2),
    cidrsubnet(var.vpc_cidr, 4, 3)
  ]

  enable_nat_gateway   = true
  single_nat_gateway   = true
  enable_dns_hostnames = true
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "20.24.2"

  cluster_name    = var.name
  cluster_version = var.cluster_version

  cluster_endpoint_public_access            = true
  enable_cluster_creator_admin_permissions  = true
  enable_irsa                               = true

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  eks_managed_node_groups = {
    default = {
      desired_size   = 2
      min_size       = 2
      max_size       = 4
      instance_types = var.node_instance_types
      capacity_type  = "ON_DEMAND"
    }
  }

  tags = var.tags
}

data "aws_iam_policy_document" "alb_assume_role" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [module.eks.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${replace(module.eks.cluster_oidc_issuer_url, "https://", "")}:sub"
      values   = ["system:serviceaccount:kube-system:aws-load-balancer-controller"]
    }
  }
}

resource "aws_iam_role" "alb_controller" {
  name               = "${var.name}-alb-controller"
  assume_role_policy = data.aws_iam_policy_document.alb_assume_role.json
}

resource "aws_iam_role_policy_attachment" "alb_controller" {
  role       = aws_iam_role.alb_controller.name
  policy_arn = var.alb_controller_policy_arn
}

output "cluster_name" {
  value = module.eks.cluster_name
}

output "cluster_endpoint" {
  value = module.eks.cluster_endpoint
}

output "cluster_ca_certificate" {
  value = module.eks.cluster_certificate_authority_data
}

output "vpc_id" {
  value = module.vpc.vpc_id
}

output "private_subnets" {
  value = module.vpc.private_subnets
}

output "vpc_cidr" {
  value = module.vpc.vpc_cidr_block
}

output "alb_controller_role_arn" {
  value = aws_iam_role.alb_controller.arn
}