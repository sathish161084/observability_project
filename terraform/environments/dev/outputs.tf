output "cluster_name" { value = module.eks.cluster_name }
output "ecr_repository_urls" { value = module.ecr.repository_urls }
output "alb_controller_role_arn" { value = module.eks.alb_controller_role_arn }
output "msk_bootstrap_brokers_tls" { value = try(module.msk[0].bootstrap_brokers_tls, null) }
output "app_secret_arn" { value = module.app_secret.secret_arn }
