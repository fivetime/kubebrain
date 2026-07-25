#!/usr/bin/env bash

validate_name_token() {
  local name="$1"
  local value="${!name}"
  if [[ ! "$value" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]]; then
    echo "${name} must contain only letters, digits, dot, underscore, or dash, start with a letter or digit, and be at most 128 characters" >&2
    exit 2
  fi
}

validate_image_reference() {
  local name="$1"
  validate_image_reference_value "$name" "${!name}"
}

validate_image_reference_value() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[A-Za-z0-9][A-Za-z0-9._:@/-]*$ ]]; then
    echo "${name} must be a non-empty image reference without whitespace or shell metacharacters" >&2
    exit 2
  fi
}

validate_positive_integer() {
  local name="$1"
  local value="${!name}"
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "${name} must be a positive integer" >&2
    exit 2
  fi
}

validate_version_token() {
  local name="$1"
  local value="${!name}"
  if [[ ! "$value" =~ ^[A-Za-z0-9._+-]+$ ]]; then
    echo "${name} must be a tag-like version without whitespace or path separators" >&2
    exit 2
  fi
}
