#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

project_version="$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)"
package_base="target/components/packages/jcustenborder-kafka-connect-transform-common-${project_version}"
plugin_dir="${package_base}"

if [[ ! -d "${plugin_dir}" ]]; then
  mvn -B clean package
fi

if [[ ! -d "${plugin_dir}" ]]; then
  echo "Expected plugin package directory was not created: ${plugin_dir}" >&2
  exit 1
fi

export PLUGIN_DIR="${repo_root}/${plugin_dir}"
payload="$(mktemp)"

cleanup() {
  docker compose down -v >/dev/null 2>&1 || true
  rm -f "${payload}"
}
trap cleanup EXIT

docker compose up -d

for i in {1..60}; do
  if curl -fsS "http://localhost:8083/connector-plugins?connectorsOnly=false" -o "${payload}"; then
    break
  fi
  if [[ "${i}" -eq 60 ]]; then
    echo "Connect REST API did not become ready." >&2
    docker compose logs connect >&2 || true
    exit 1
  fi
  sleep 5
done

MANIFEST_PATH="src/main/resources/META-INF/services/org.apache.kafka.connect.transforms.Transformation" \
PAYLOAD_PATH="${payload}" \
python3 - <<'PY'
import json
import os
import sys

with open(os.environ["MANIFEST_PATH"], encoding="utf-8") as manifest_file:
    expected = {
        line.strip()
        for line in manifest_file
        if line.strip() and not line.startswith("#")
    }

with open(os.environ["PAYLOAD_PATH"], encoding="utf-8") as payload_file:
    payload_text = payload_file.read()
    plugins = json.loads(payload_text)

transformations = {
    plugin.get("class")
    for plugin in plugins
    if plugin.get("type") == "transformation"
}
missing = sorted(expected - transformations)
project_connectors = sorted(
    plugin.get("class")
    for plugin in plugins
    if plugin.get("type") in {"source", "sink"}
    and plugin.get("class", "").startswith("com.github.jcustenborder.kafka.connect.transform.common.")
)

if missing or project_connectors:
    if missing:
        print("Missing transformation plugins:", file=sys.stderr)
        for class_name in missing:
            print(f"  {class_name}", file=sys.stderr)
    if project_connectors:
        print("Expected no project-specific source or sink connectors:", file=sys.stderr)
        for class_name in project_connectors:
            print(f"  {class_name}", file=sys.stderr)
    print("REST payload:", file=sys.stderr)
    print(payload_text, file=sys.stderr)
    sys.exit(1)

print(f"Validated {len(expected)} transformation plugins; no project-specific source or sink connectors found.")
PY
