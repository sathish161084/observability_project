resource "aws_ecr_repository" "repos" {
  for_each = toset(var.repository_names)
  name                 = each.value
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = true }
}
output "repository_urls" { value = { for k, v in aws_ecr_repository.repos : k => v.repository_url } }
