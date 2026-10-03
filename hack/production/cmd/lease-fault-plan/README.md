# `lease-fault-plan`

`--seal-command-draft DRAFT --write-command-plan OUTPUT` is the staging path
for the dedicated native lease-fault command. The private draft is a complete
`NativeCommandPlan` with every `files` value empty and explicit absolute paths
for each admitted input. Sealing hashes only those listed files, runs the same
offline `CheckLocal` validation as `--command-plan`, and creates a new mode-0600
output without overwriting an existing file.

The printed digest is a local integrity pin, **not independent approval**.
Review the complete generated plan and live evidence, independently approve its
exact SHA256, then run `--command-plan OUTPUT --approve-sha256 SHA256`. Release
authentication remains a separate `--release-plan` preflight. Neither mode
admits the live cluster or executes a fault; only `lease-fault-run` performs
the one-shot execution with its required online gates and recovery path.
