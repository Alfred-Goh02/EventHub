# The one image repository. Every chart runs the same image (eventhub/app) with a
# different command, as compose does.

resource "aws_ecr_repository" "this" {
  name = var.name

  # IMMUTABLE: images are tagged by git commit SHA, never latest, and a tag once pushed
  # can never be pointed at different bytes. A rollback to a SHA is a rollback to exactly
  # what ran, and the eventhub-common chart already refuses to render tag latest.
  image_tag_mutability = "IMMUTABLE"

  force_delete = var.force_delete

  # Basic scanning on every push: known CVEs in the image's operating-system packages.
  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "AES256"
  }
}

# Storage is billed per GB-month, and a build per commit adds up. Only the newest
# keep_images survive; with immutable tags there is nothing else to tell images apart by.
resource "aws_ecr_lifecycle_policy" "this" {
  repository = aws_ecr_repository.this.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep the newest ${var.keep_images} images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = var.keep_images
        }
        action = {
          type = "expire"
        }
      },
    ]
  })
}
