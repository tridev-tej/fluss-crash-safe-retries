#!/usr/bin/env bash
set -euo pipefail

hash_file() {
  local hash_output
  hash_output=$(sha256sum -- "$1")
  printf '%s\n' "${hash_output%% *}"
}

summarize_output() {
  LC_ALL=C sed -E 's#\x1B\[[0-?]*[ -/]*[@-~]##g' "$launch/client.stdout.log" > "$launch/stdout.normalized"
  sed -nE 's/^[[:space:]]*\|[[:space:]]*(L[0-9]{3}_[0-9a-f]{8})[[:space:]]*\|[[:space:]]*$/\1/p' "$launch/stdout.normalized" > "$launch/markers.observed"
  local line count=0
  while IFS= read -r line; do
    if [[ "$line" == *'Complete execution of the SQL update statement.'* ]]; then
      count=$((count + 1))
    fi
  done < "$launch/stdout.normalized"
  printf '%s\n' "$count" > "$launch/dml.observed"
}

admit_client() {
  if [[ "$native_result" -ne 0 ]]; then
    exit "$native_result"
  fi
  if grep -aFq '[ERROR]' "$launch/client.stdout.log" "$launch/client.stderr.log"; then
    exit 70
  fi
  local -a expected_markers observed_markers
  mapfile -t expected_markers < "$sql_dir/$sql_program.markers"
  mapfile -t observed_markers < "$launch/markers.observed"
  [[ ${#expected_markers[@]} -gt 0 ]]
  [[ "${observed_markers[*]}" == "${expected_markers[*]}" ]]
  local expected_dml observed_dml
  expected_dml=$(< "$sql_dir/$sql_program.dml-count")
  observed_dml=$(< "$launch/dml.observed")
  [[ "$expected_dml" =~ ^[0-9]+$ ]]
  [[ "$observed_dml" == "$expected_dml" ]]
}

: "${LAB_RUN_ID:?LAB_RUN_ID required}"
[[ "$LAB_RUN_ID" =~ ^[A-Za-z0-9_-]+$ ]]
sql_dir="/evidence/$LAB_RUN_ID/sql"
sql_program="${SQL_PROGRAM:-program}"
[[ "$sql_program" =~ ^(program|verify|reject)$ ]]
launch="$sql_dir/launch-$sql_program"
mkdir "$launch"
trap 'printf "%s\n" "$?" > "$launch/wrapper.exit"' EXIT
hash_file "$0" > "$launch/wrapper.sha256"
for input_name in preparation.json sql-hashes.json expected.json; do
  hash_file "$sql_dir/$input_name" > "$launch/$input_name.sha256"
done
expected=$(< "$sql_dir/$sql_program.expected.sha256")
[[ "$expected" =~ ^[0-9a-f]{64}$ ]]
printf '%s\n' "$expected" > "$launch/prepared.sha256"
[[ "$(hash_file "$sql_dir/$sql_program.sql")" == "$expected" ]]
cp "$sql_dir/$sql_program.sql" "$launch/executed.sql"
chmod 444 "$launch/executed.sql"
hash_file "$launch/executed.sql" > "$launch/launch.sha256"
[[ "$(< "$launch/launch.sha256")" == "$expected" ]]
set +e
timeout --signal=TERM --kill-after=15s 600s /opt/flink/bin/sql-client.sh -f "$launch/executed.sql" > "$launch/client.stdout.log" 2> "$launch/client.stderr.log"
native_result=$?
set -e
printf '%s\n' "$native_result" > "$launch/native.exit"
hash_file "$launch/executed.sql" > "$launch/after.sha256"
[[ "$(< "$launch/after.sha256")" == "$expected" ]]
[[ "$(hash_file "$sql_dir/$sql_program.sql")" == "$expected" ]]
summarize_output
admit_client
