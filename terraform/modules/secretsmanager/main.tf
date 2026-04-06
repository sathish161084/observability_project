resource "aws_secretsmanager_secret" "app" {
  name = var.secret_name
}
resource "aws_secretsmanager_secret_version" "app" {
  secret_id     = aws_secretsmanager_secret.app.id
  secret_string = jsonencode(var.secret_value)
}
output "secret_arn" { value = aws_secretsmanager_secret.app.arn }
