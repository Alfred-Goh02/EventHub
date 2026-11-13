variable "name" {
  description = "The repository's name, such as eventhub/app."
  type        = string
}

variable "keep_images" {
  description = "How many images the lifecycle policy keeps; older ones are expired."
  type        = number
  default     = 20
  nullable    = false

  validation {
    condition     = var.keep_images >= 1
    error_message = "keep_images must keep at least one image."
  }
}

variable "force_delete" {
  description = "Whether `terraform destroy` may delete the repository while it still holds images."
  type        = bool
  default     = false
  nullable    = false
}
