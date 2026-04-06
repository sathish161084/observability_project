data "aws_eks_cluster_auth" "this" {
  name = module.eks.cluster_name
}
resource "aws_iam_policy" "alb_controller" {
  name   = "${var.cluster_name}-alb-controller"
  policy = file("${path.module}/policy/aws-load-balancer-controller-policy.json")
}
module "ecr" {
  source = "../../modules/ecr"
  repository_names = ["three-tier-frontend","three-tier-backend","three-tier-worker"]
}
module "eks" {
  source = "../../modules/eks"
  name = var.cluster_name
  vpc_cidr = "10.60.0.0/16"
  availability_zones = var.availability_zones
  cluster_version = "1.31"
  node_instance_types = ["t3.medium"]
  tags = { Environment = "dev", Project = "three-tier" }
  oidc_thumbprint = "9e99a48a9960b14926bb7f3b02e22da0ecd4e1c6"
  alb_controller_policy_arn = aws_iam_policy.alb_controller.arn
}
module "msk" {
  source = "../../modules/msk"
  count  = var.enable_msk ? 1 : 0
  name = var.cluster_name
  vpc_id = module.eks.vpc_id
  vpc_cidr = module.eks.vpc_cidr
  private_subnet_ids = module.eks.private_subnets
  kafka_version = "3.7.0"
  broker_instance_type = "kafka.t3.small"
}
module "app_secret" {
  source = "../../modules/secretsmanager"
  secret_name = "/three-tier/dev/app"
  secret_value = { example_api_key = "replace-me" }
}
provider "helm" {
  kubernetes {
    host = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_ca_certificate)
    token = data.aws_eks_cluster_auth.this.token
  }
}
resource "helm_release" "aws_load_balancer_controller" {
  name       = "aws-load-balancer-controller"
  repository = "https://aws.github.io/eks-charts"
  chart      = "aws-load-balancer-controller"
  namespace  = "kube-system"
  set { name = "clusterName" value = module.eks.cluster_name }
  set { name = "serviceAccount.create" value = "true" }
  set { name = "serviceAccount.name" value = "aws-load-balancer-controller" }
  set { name = "serviceAccount.annotations.eks\.amazonaws\.com/role-arn" value = module.eks.alb_controller_role_arn }
  set { name = "region" value = var.aws_region }
  set { name = "vpcId" value = module.eks.vpc_id }
  depends_on = [module.eks]
}
