terraform {
  backend "s3" {
    bucket         = "observability-terraform-state-442999092835"
    key            = "observability-eks-platform-prod/dev/terraform.tfstate"
    region         = "us-east-1"
    use_lockfile = true
    encrypt        = true
  }
}