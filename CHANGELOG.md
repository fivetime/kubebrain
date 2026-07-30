# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project follows semantic versioning where release compatibility permits.

## Unreleased

### Changed

- Standardized local builds through a modern `Makefile`.
- Moved the production container definition to `build/kubebrain.Dockerfile`.
- Updated CI, image publishing, dependency automation, and security scanning.
- Updated the Go patch toolchain and security-sensitive dependencies.

### Security

- Containers now build reproducibly and run as an unprivileged user.
- Image publishing produces SBOM and provenance attestations.
