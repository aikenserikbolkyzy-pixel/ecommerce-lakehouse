#!/usr/bin/env bash
# =====================================================================
# spark-env.sh — identical file on all nodes; per-node values branch on
# the hostname. Installed to $SPARK_HOME/conf/ by 04_install_configs.sh
# =====================================================================

# Java (non-interactive SSH does not read ~/.bashrc)
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
if [ -z "${JAVA_HOME:-}" ] || [ ! -x "${JAVA_HOME}/bin/java" ]; then
  export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")"
fi

# Hadoop client configuration -> Spark finds HDFS (fs.defaultFS) and YARN
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export YARN_CONF_DIR=/opt/hadoop/etc/hadoop

# "Hadoop-free" Spark builds ship without Hadoop jars -> borrow them from Hadoop
_spark_jars="${SPARK_HOME:-/opt/spark}/jars"
if ! ls "$_spark_jars"/hadoop-client-api-*.jar >/dev/null 2>&1 \
   && ! ls "$_spark_jars"/hadoop-common-*.jar >/dev/null 2>&1; then
  export SPARK_DIST_CLASSPATH="$(/opt/hadoop/bin/hadoop classpath)"
fi
unset _spark_jars

# Standalone master
export SPARK_MASTER_HOST=master
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080

# Standalone workers (one per laptop). Values for 16 GB laptops; see README §2.
export SPARK_WORKER_CORES=4
case "$(hostname -s)" in
  master) export SPARK_WORKER_MEMORY=1500m ;;   # master also runs NameNode, RM, driver
  *)      export SPARK_WORKER_MEMORY=3g ;;
esac
export SPARK_WORKER_WEBUI_PORT=8081
export SPARK_DAEMON_MEMORY=512m

# Local disks (shuffle spill, logs, pids) — not /tmp
export SPARK_LOCAL_DIRS=/bigdata/spark/local
export SPARK_WORKER_DIR=/bigdata/spark/work
export SPARK_LOG_DIR=/bigdata/spark/logs
export SPARK_PID_DIR=/bigdata/spark/pids
export SPARK_WORKER_OPTS="-Dspark.worker.cleanup.enabled=true -Dspark.worker.cleanup.interval=1800 -Dspark.worker.cleanup.appDataTtl=86400"

# Same Python on driver and executors (minor version must match on all nodes)
export PYSPARK_PYTHON=/usr/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3
