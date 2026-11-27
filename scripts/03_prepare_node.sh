#!/usr/bin/env bash
# =====================================================================
# 03_prepare_node.sh — run on EACH laptop (after 01).
#  * /opt/hadoop and /opt/spark symlinks -> identical paths on all nodes
#  * local directories under /bigdata (HDFS blocks, YARN, Spark spill, logs)
#  * environment block in ~/.bashrc
# If auto-detection fails, pass the real install dirs:
#   ./scripts/03_prepare_node.sh /home/me/hadoop-3.3.6 /home/me/spark-3.5.1-bin-hadoop3
# =====================================================================
source "$(dirname "$0")/lib.sh"

REAL_HADOOP="${1:-$(detect_install_dir hadoop)}"
REAL_SPARK="${2:-$(detect_install_dir spark)}"
[[ -x "$REAL_HADOOP/bin/hadoop" ]]      || die "Hadoop not found. Pass its dir: $0 <hadoop_dir> <spark_dir>"
[[ -x "$REAL_SPARK/bin/spark-submit" ]] || die "Spark not found. Pass its dir: $0 <hadoop_dir> <spark_dir>"
REAL_HADOOP="$(readlink -f "$REAL_HADOOP")"
REAL_SPARK="$(readlink -f "$REAL_SPARK")"
ME="$(id -un)"

# 1) Identical install paths
link() {   # link <real_dir> <link_path>
  local real="$1" lnk="$2"
  if [[ -L "$lnk" || ! -e "$lnk" ]]; then
    sudo ln -sfn "$real" "$lnk"; ok "$lnk -> $real"
  elif [[ "$(readlink -f "$lnk")" == "$real" ]]; then
    ok "$lnk is the install dir itself"
  else
    die "$lnk exists and is a different directory; move it or pass the right path"
  fi
}
link "$REAL_HADOOP" "$HADOOP_HOME"
link "$REAL_SPARK"  "$SPARK_HOME"

# Config files and jars must be writable by the cluster user
for d in "$REAL_HADOOP/etc/hadoop" "$REAL_SPARK/conf" "$REAL_SPARK/jars"; do
  if [[ ! -w "$d" ]]; then
    log "Making $d writable for $ME"
    sudo chown -R "$ME:$ME" "$d"
  fi
done

# 2) Local directories
log "Creating $LOCAL_ROOT layout"
sudo mkdir -p "$LOCAL_ROOT"
sudo chown "$ME:$ME" "$LOCAL_ROOT"
mkdir -p \
  "$LOCAL_ROOT"/hadoop/{tmp,logs,pids} \
  "$LOCAL_ROOT"/hadoop/hdfs/datanode \
  "$LOCAL_ROOT"/hadoop/yarn/{local,logs} \
  "$LOCAL_ROOT"/spark/{local,work,logs,pids} \
  "$STAGING_DIR" "$EVIDENCE_DIR"
if [[ "$(hostname -s)" == "master" ]]; then
  mkdir -p "$LOCAL_ROOT"/hadoop/hdfs/{namenode,namesecondary}
fi
chmod 700 "$LOCAL_ROOT/hadoop/hdfs/datanode"   # DataNode refuses group/world-writable dirs
ok "$(find "$LOCAL_ROOT" -maxdepth 3 -type d | sort | tr '\n' ' ')"

# 3) Shell environment (interactive shells; daemons use hadoop-env.sh / spark-env.sh)
sed -i '/# >>> bigdata-cluster >>>/,/# <<< bigdata-cluster <<</d' ~/.bashrc
cat >> ~/.bashrc <<'EOF'
# >>> bigdata-cluster >>>
export JAVA_HOME="${JAVA_HOME:-$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")}"
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export YARN_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_HOME=/opt/spark
export PYSPARK_PYTHON=/usr/bin/python3
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$PATH"
# <<< bigdata-cluster <<<
EOF
ok "~/.bashrc updated — run: source ~/.bashrc"

# 4) Disk check
avail_gb=$(( $(df --output=avail -B1 "$LOCAL_ROOT" | tail -1) / 1024**3 ))
need=35; [[ "$(hostname -s)" == "master" ]] && need=55
if (( avail_gb < need )); then
  warn "Only ${avail_gb} GB free under $LOCAL_ROOT; recommended >= ${need} GB on $(hostname -s) (README §2)"
else
  ok "${avail_gb} GB free under $LOCAL_ROOT"
fi
