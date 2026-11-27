#!/usr/bin/env bash
# =====================================================================
# 04_install_configs.sh — run on MASTER only (after 02 and 03 on all nodes).
# Pushes conf/hadoop/* and conf/spark/* to all 3 nodes, adds the managed
# block to hadoop-env.sh, distributes the Iceberg runtime jar, and checks
# that Hadoop/Spark versions are identical everywhere.
# Re-run it after every config change.
# =====================================================================
source "$(dirname "$0")/lib.sh"
require_cluster_env
require_host master

CONF="$PROJECT_DIR/conf"
HADOOP_FILES=(core-site.xml hdfs-site.xml yarn-site.xml mapred-site.xml workers)
SPARK_FILES=(spark-defaults.conf spark-env.sh workers)
STAMP="$(date +%Y%m%d%H%M%S)"

# --- Iceberg runtime jar on master --------------------------------------
read -r SPARK_VER SCALA_VER <<<"$(spark_and_scala_version "$SPARK_HOME")"
SPARK_MM="$(cut -d. -f1,2 <<<"$SPARK_VER")"
EXPECT_PREFIX="iceberg-spark-runtime-${SPARK_MM}_${SCALA_VER}-"

JAR="${ICEBERG_JAR:-}"
if [[ -z "$JAR" ]]; then
  JAR="$(ls "$SPARK_HOME"/jars/iceberg-spark-runtime-*.jar 2>/dev/null | head -1 || true)"
fi
if [[ ! -f "$JAR" ]]; then
  die "No Iceberg runtime jar found. Download the one for Spark $SPARK_VER / Scala $SCALA_VER, e.g.:
  wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/org/apache/iceberg/${EXPECT_PREFIX%-}/1.10.0/${EXPECT_PREFIX}1.10.0.jar
then re-run this script."
fi
JAR_NAME="$(basename "$JAR")"
[[ "$JAR_NAME" == "$EXPECT_PREFIX"* ]] \
  || die "$JAR_NAME does not match Spark $SPARK_VER / Scala $SCALA_VER (expected ${EXPECT_PREFIX}<ver>.jar)"
ok "Iceberg jar: $JAR_NAME  (Spark $SPARK_VER, Scala $SCALA_VER)"

MASTER_HADOOP_VER="$(hadoop version 2>/dev/null | head -1)"

# --- Push to every node --------------------------------------------------
for n in "${ALL_NODES[@]}"; do
  log "===== $n ====="

  # Back up the existing conf dirs once per run
  ssh "$n" "tar czf ~/conf-backup-$STAMP.tgz -C $HADOOP_HOME/etc hadoop -C $SPARK_HOME conf 2>/dev/null || true"

  # Hadoop + Spark config files
  scp -q "${HADOOP_FILES[@]/#/$CONF/hadoop/}" "$n:$HADOOP_HOME/etc/hadoop/"
  scp -q "${SPARK_FILES[@]/#/$CONF/spark/}"   "$n:$SPARK_HOME/conf/"
  scp -q "$CONF/hadoop/hadoop-env.sh.append"  "$n:/tmp/hadoop-env.sh.append"

  ssh "$n" bash -s <<EOF
set -e
f=$HADOOP_HOME/etc/hadoop/hadoop-env.sh
touch "\$f"
sed -i '/# >>> bigdata-cluster >>>/,/# <<< bigdata-cluster <<</d' "\$f"
cat /tmp/hadoop-env.sh.append >> "\$f" && rm -f /tmp/hadoop-env.sh.append
chmod +x $SPARK_HOME/conf/spark-env.sh
EOF
  ok "configs installed"

  # Iceberg jar: exactly one, the master's version
  others="$(ssh "$n" "ls $SPARK_HOME/jars/ | grep '^iceberg-spark-runtime' | grep -vx '$JAR_NAME' || true")"
  [[ -z "$others" ]] || die "$n has other Iceberg runtime jars: $others — delete them from $SPARK_HOME/jars"
  if ! ssh "$n" "test -f $SPARK_HOME/jars/$JAR_NAME"; then
    [[ "$n" == "master" ]] && cp "$JAR" "$SPARK_HOME/jars/" || scp -q "$JAR" "$n:$SPARK_HOME/jars/"
  fi
  ok "Iceberg jar present"

  # Version consistency
  node_core="$(ssh "$n" "basename \$(ls $SPARK_HOME/jars/spark-core_*.jar | head -1)")"
  node_hadoop="$(ssh "$n" "$HADOOP_HOME/bin/hadoop version 2>/dev/null | head -1")"
  [[ "$node_core" == "spark-core_${SCALA_VER}-${SPARK_VER}.jar" ]] \
    || die "$n runs $node_core but master runs spark-core_${SCALA_VER}-${SPARK_VER}.jar"
  [[ "$node_hadoop" == "$MASTER_HADOOP_VER" ]] \
    || die "$n runs '$node_hadoop' but master runs '$MASTER_HADOOP_VER'"
  ok "versions match master ($MASTER_HADOOP_VER, Spark $SPARK_VER)"
done

log "Effective HDFS settings (from master):"
echo "  dfs.replication = $(hdfs getconf -confKey dfs.replication)"
echo "  dfs.blocksize   = $(hdfs getconf -confKey dfs.blocksize)"
echo "  fs.defaultFS    = $(hdfs getconf -confKey fs.defaultFS)"
ok "Configs distributed to ${ALL_NODES[*]}"
