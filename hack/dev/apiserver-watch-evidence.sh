#!/usr/bin/env bash
# The caller has exclusively created a mode-0700 evidence directory and stopped
# both writers. Archive only these two files, never PKI or the kubeconfig.
archive_apiserver_watch_evidence() {
  local evidence_dir="$1" work_dir="$2" filename
  for filename in configmap-watch.jsonl kube-apiserver.log; do
    if [[ -L "$work_dir/$filename" ]]; then
      echo "refusing symlink evidence source: $filename" >&2
      return 1
    fi
    if [[ -f "$work_dir/$filename" ]]; then
      if [[ -e "$evidence_dir/$filename" || -L "$evidence_dir/$filename" ]]; then
        echo "refusing to overwrite archived evidence: $filename" >&2
        return 1
      fi
      cp -- "$work_dir/$filename" "$evidence_dir/$filename" || return 1
      chmod 0600 "$evidence_dir/$filename" || return 1
    fi
  done
}
