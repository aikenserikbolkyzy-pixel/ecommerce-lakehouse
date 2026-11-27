#!/usr/bin/env bash
# Shared helpers — sourced by every script in this folder.
set -euo pipefail

# Remember any install dirs the user's shell already knows (used by 00 and 03)
ORIG_HADOOP_HOME="${HADOOP_HOME:-}"
ORIG_SPARK_HOME="${SPARK_HOME:-}"

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../cluster.env
source "$PROJECT_DIR/cluster.env"

export HADOOP_HOME SPARK_HOME
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$PATH"

log()  { printf '\033[1;34m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; }
ok()   { printf '\033[1;32m[ OK ]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[WARN]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[FAIL]\033[0m %s\n' "$*" >&2; exit 1; }

# Abort if cluster.env still has <PLACEHOLDER> values
require_cluster_env() {
  local v
  for v in MASTER_IP WORKER1_IP WORKER2_IP MASTER_USER WORKER1_USER WORKER2_USER; do
    if [[ -z "${!v:-}" || "${!v}" == \<* ]]; then
      die "Edit cluster.env first: $v is still '${!v:-}'"
    fi
  done
}

# Abort unless running on the named host
require_host() {
  [[ "$(hostname -s)" == "$1" ]] || die "Run this on '$1' (this machine is '$(hostname -s)')."
}

node_ip() {
  case "$1" in
    master)  echo "$MASTER_IP" ;;
    worker1) echo "$WORKER1_IP" ;;
    worker2) echo "$WORKER2_IP" ;;
    *) die "unknown node '$1'" ;;
  esac
}

node_user() {
  case "$1" in
    master)  echo "$MASTER_USER" ;;
    worker1) echo "$WORKER1_USER" ;;
    worker2) echo "$WORKER2_USER" ;;
    *) die "unknown node '$1'" ;;
  esac
}

# Real install dir of a tool: /opt/<name> symlink > $XXX_HOME from the shell > PATH lookup
# usage: detect_install_dir hadoop|spark
detect_install_dir() {
  local name="$1" bin orig
  case "$name" in
    hadoop) bin=hadoop;       orig="$ORIG_HADOOP_HOME" ;;
    spark)  bin=spark-submit; orig="$ORIG_SPARK_HOME" ;;
  esac
  if [[ -x "/opt/$name/bin/$bin" ]]; then readlink -f "/opt/$name"; return; fi
  if [[ -n "$orig" && -x "$orig/bin/$bin" ]]; then readlink -f "$orig"; return; fi
  if command -v "$bin" >/dev/null 2>&1; then
    dirname "$(dirname "$(readlink -f "$(command -v "$bin")")")"; return
  fi
  echo ""
}

java_home() {
  if [[ -n "${JAVA_HOME:-}" && -x "$JAVA_HOME/bin/java" ]]; then echo "$JAVA_HOME"; return; fi
  if command -v java >/dev/null 2>&1; then
    dirname "$(dirname "$(readlink -f "$(command -v java)")")"; return
  fi
  echo ""
}
export JAVA_HOME="${JAVA_HOME:-$(java_home)}"

# "spark-core_2.12-3.5.1.jar" -> prints "3.5.1 2.12"
spark_and_scala_version() {
  local jars="$1/jars" core
  core="$(basename "$(ls "$jars"/spark-core_*.jar 2>/dev/null | head -1)" 2>/dev/null || true)"
  if [[ "$core" =~ spark-core_([0-9]+\.[0-9]+)-(.+)\.jar ]]; then
    echo "${BASH_REMATCH[2]} ${BASH_REMATCH[1]}"
  else
    echo "unknown unknown"
  fi
}
