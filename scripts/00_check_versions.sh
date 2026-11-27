#!/usr/bin/env bash
# =====================================================================
# 00_check_versions.sh — run on EVERY laptop, then compare the 3 outputs.
# Java, Hadoop, Spark, Scala and Python minor versions must match on all
# nodes, and the Iceberg jar must match Spark's major.minor + Scala version.
# Usage: ./scripts/00_check_versions.sh
# =====================================================================
source "$(dirname "$0")/lib.sh"

H="$(detect_install_dir hadoop)"
S="$(detect_install_dir spark)"
OUT="$PWD/versions_$(hostname -s).txt"

report() {
  echo "host           : $(hostname -s)   ip: $(hostname -I 2>/dev/null | awk '{print $1}')"
  echo "os             : $(. /etc/os-release && echo "$PRETTY_NAME")"
  echo "cpu cores      : $(nproc)"
  echo "ram            : $(free -g | awk '/^Mem:/{print $2" GB total, "$7" GB available"}')"
  echo "disk free (/)  : $(df -h --output=avail / | tail -1 | tr -d ' ')"
  echo "java           : $(java -version 2>&1 | grep -iE 'version' | head -1 || echo 'NOT FOUND')"
  echo "JAVA_HOME      : $(java_home)"

  if [[ -n "$H" ]]; then
    echo "hadoop home    : $H"
    echo "hadoop         : $("$H/bin/hadoop" version 2>/dev/null | head -1)"
  else
    echo "hadoop         : NOT FOUND"
  fi

  if [[ -n "$S" ]]; then
    local sv scala mm ice hc
    read -r sv scala <<<"$(spark_and_scala_version "$S")"
    mm="$(cut -d. -f1,2 <<<"$sv")"
    ice="$(ls "$S/jars" | grep -E '^iceberg-spark-runtime' | tr '\n' ' ' || true)"
    hc="$(ls "$S/jars" | grep -E '^hadoop-client-api|^hadoop-common' | head -1 || true)"
    echo "spark home     : $S"
    echo "spark          : $sv  (Scala $scala)"
    echo "iceberg jar    : ${ice:-NONE in $S/jars}"
    echo "needed jar     : iceberg-spark-runtime-${mm}_${scala}-<ICEBERG_VERSION>.jar"
    echo "spark's hadoop : ${hc:-none (Hadoop-free build: spark-env.sh sets SPARK_DIST_CLASSPATH)}"
  else
    echo "spark          : NOT FOUND"
  fi

  echo "python3        : $(python3 --version 2>&1)"
  echo "pip pyspark    : $(python3 -c 'import pyspark; print(pyspark.__version__)' 2>/dev/null \
                             || echo 'not pip-installed (fine: spark-submit uses $SPARK_HOME/python)')"
}

report | tee "$OUT"
echo
ok "Saved to $OUT — compare it with the other two laptops."
cat <<'EOF'
Matching rules:
  * java / hadoop / spark / scala / python3 (major.minor) identical on all 3 nodes.
  * Iceberg jar = iceberg-spark-runtime-<spark major.minor>_<scala>-<iceberg version>.jar
      Spark 3.5.x (Scala 2.12) -> iceberg-spark-runtime-3.5_2.12-1.10.0.jar (or newer 1.x)
      Spark 4.0.x (Scala 2.13) -> iceberg-spark-runtime-4.0_2.13-1.10.0.jar (Iceberg >= 1.10)
      Spark 4.1.x (Scala 2.13) -> iceberg-spark-runtime-4.1_2.13-1.11.0.jar (Iceberg >= 1.11)
  * Exactly ONE iceberg-spark-runtime jar per node.
  * If pyspark is pip-installed, its version must equal the Spark version above.
EOF
