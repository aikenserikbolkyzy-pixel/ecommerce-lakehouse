# Distributed Clickstream Lakehouse — Phase 1
**Cluster networking · HDFS (replication = 3) · YARN · Spark Standalone + Iceberg · Raw ingestion**

Team: Aiken Otargali · Zhuldyzay Kalka · Inkar Tlegenova — Distributed Big Data Systems

Pipeline position: **Local ZIP → unzip → HDFS (128 MB blocks, ×3)** → Spark → Iceberg (Bronze/Silver/Gold) → MLlib → Pandas/Matplotlib → scalability experiment.
Phase 1 delivers the bold part, plus a smoke test proving that Spark and Iceberg work on top of it.

```
ecommerce-lakehouse/
├── cluster.env                  # ONLY file you edit: IPs + Linux user names
├── conf/
│   ├── hadoop/  core-site.xml  hdfs-site.xml  yarn-site.xml  mapred-site.xml  workers  hadoop-env.sh.append
│   └── spark/   spark-defaults.conf  spark-env.sh  workers
├── scripts/
│   ├── 00_check_versions.sh     # every node  – versions, RAM, disk, matching Iceberg jar
│   ├── 01_network_setup.sh      # every node  – hostname, /etc/hosts, sshd, firewall, NTP, no-suspend
│   ├── 02_ssh_setup.sh          # master      – passwordless SSH to all 3 nodes
│   ├── 03_prepare_node.sh       # every node  – /opt symlinks, /bigdata dirs, ~/.bashrc
│   ├── 04_install_configs.sh    # master      – push configs + Iceberg jar to all nodes
│   ├── 05_format_namenode.sh    # master      – once
│   ├── 06_start_cluster.sh      # master      – HDFS + YARN + Spark + History Server
│   ├── 07_stage_dataset.sh      # master      – unzip + header check + manifest (sizes, row counts)
│   ├── 08_ingest_hdfs.sh        # master      – upload to /data/raw/ecommerce, wait for 3 replicas
│   ├── 09_verify_hdfs.sh        # master      – dfsadmin -report, fsck -files -blocks -locations, PASS/FAIL
│   ├── 10_run_smoke_test.sh     # master      – Spark on 3 nodes + Iceberg catalog test
│   ├── cluster_status.sh  stop_cluster.sh  lib.sh
└── jobs/phase1_smoke_test.py
```

---

## 1. Topology

| Laptop | Hostname | HDFS | YARN | Spark Standalone | Also |
|---|---|---|---|---|---|
| Node 1 | `master` | NameNode, SecondaryNameNode, DataNode | ResourceManager, NodeManager | Master, Worker, driver | History Server, local staging |
| Node 2 | `worker1` | DataNode | NodeManager | Worker | |
| Node 3 | `worker2` | DataNode | NodeManager | Worker | |

| Service | Port | Web UI |
|---|---|---|
| NameNode RPC / UI | 9000 / 9870 | http://master:9870 |
| DataNode data / UI | 9866 / 9864 | |
| YARN ResourceManager | 8032 / 8088 | http://master:8088 |
| Spark Master | 7077 / 8080 | http://master:8080 |
| Spark Worker UI | 8081 | http://worker1:8081 |
| Spark History Server | 18080 | http://master:18080 |
| Running Spark app | 4040 | http://master:4040 |

**Cluster manager choice.** Spark jobs run on **Spark Standalone** (`spark://master:7077`); this matches the experiment in the proposal (`--total-executor-cores N`). YARN is started too, as Hadoop's resource manager and as an alternative (`--master yarn`). On 16 GB laptops, don't run YARN jobs and Standalone jobs at the same time, because both would claim the same RAM.

**Replication = 3 on 3 DataNodes** means every laptop stores a full copy of every block. Consequences you can cite in the report:
- HDFS data stays readable if any two DataNodes are down (the NameNode on `master` remains a single point of failure).
- Every Spark read task is data-local (`NODE_LOCAL`/`PROCESS_LOCAL` in the Spark UI → Stages → *Locality Level*).
- Storage cost is 3×: ~14.7 GB of CSV uses ~44 GB of cluster disk.
- Each block crosses the network twice during upload (master → worker1 → worker2 pipeline), so upload speed depends on the LAN.

---

## 2. Prerequisites and sizing

**Network**
- All 3 laptops on the **same LAN**. Many university Wi-Fi networks block laptop-to-laptop traffic ("client isolation"). If `ping worker1` fails, use a phone hotspot, or (better) a small router or switch with Ethernet cables.
- Give each laptop a **fixed IP**: a DHCP reservation in the router, or *Settings → Network → IPv4 → Manual*. If an IP changes, update `cluster.env`, re-run `01` on all nodes, then restart the cluster.

**Disk (free space)**

| Node | Needed | What uses it |
|---|---|---|
| master | ≥ 55 GB | zip (~5 GB) + unzipped CSV (14.7 GB) + full HDFS replica (14.7 GB) + Iceberg layers + Spark spill |
| worker1, worker2 | ≥ 35 GB | full HDFS replica + Iceberg layers + Spark spill |

Once `09_verify_hdfs.sh` passes, you can delete `/bigdata/staging/ecommerce/*.csv` (keep the zip) to recover 14.7 GB on master.

**RAM.** The shipped files assume 16 GB laptops with 4 or more CPU threads. For other sizes, edit these values:

| Laptop RAM | `SPARK_WORKER_MEMORY` master / workers (spark-env.sh) | `spark.executor.memory` / `.cores` | `spark.driver.memory` | `yarn.nodemanager.resource.memory-mb` |
|---|---|---|---|---|
| 8 GB | 1500m / 3g | 1400m / 2 | 1g | 2048 (do not run YARN jobs) |
| **16 GB (default)** | **4g / 6g** | **2g / 2** | **2g** | **6144** |
| 32 GB | 10g / 18g | 4g / 2 (set `SPARK_WORKER_CORES=8` if the CPU has 8+ threads) | 4g | 12288 |

On 8 GB laptops, develop on the 10% sample first, as planned in the proposal.

**Software.** Already installed on all nodes: Java, Hadoop, Spark, Python 3. Run `00` on each laptop and compare the outputs (step 3).

---

## 3. Step-by-step

> Every step names **where** to run it. Run the scripts from the project root (`cd ecommerce-lakehouse`). The scripts stop at the first error and print why.

### Step 1 — Fill in `cluster.env` (one laptop)
Find each laptop's IP with `hostname -I` and its user name with `whoami`. Replace the 6 placeholders:
```bash
MASTER_IP="192.168.1.10"     WORKER1_IP="192.168.1.11"     WORKER2_IP="192.168.1.12"
MASTER_USER="aiken"          WORKER1_USER="zhuldyzay"      WORKER2_USER="inkar"
```

### Step 2 — Put the same project folder on all 3 laptops
Use a Git repo (recommended, since the code has to be submitted anyway) or a USB stick. The folder can live anywhere, e.g. `~/ecommerce-lakehouse`.
```bash
chmod +x scripts/*.sh conf/spark/spark-env.sh
```

### Step 3 — Check versions (every laptop)
```bash
./scripts/00_check_versions.sh
```
Compare the three `versions_<host>.txt` files. These must match on all nodes:
- **Java** (same major version). Spark 3.5 runs on Java 8/11/17; Spark 4.x needs Java 17+; Iceberg 1.7+ needs Java 11+. Hadoop 3.3/3.4 is certified on Java 8/11. If Hadoop misbehaves on Java 17, pin Java 11 for Hadoop in `hadoop-env.sh.append` and keep Spark on 17 in `spark-env.sh` (they are set separately).
- **Hadoop**, **Spark + Scala**, and **Python 3 minor version** (PySpark fails with *"Python in worker has different version"* otherwise).
- **Iceberg runtime jar** — exactly one, named `iceberg-spark-runtime-<Spark major.minor>_<Scala>-<Iceberg version>.jar`:

| Spark (from `00`) | Scala | Jar to use |
|---|---|---|
| 3.5.x | 2.12 | `iceberg-spark-runtime-3.5_2.12-1.10.0.jar` (any 1.x ≥ 1.4 works) |
| 3.5.x | 2.13 | `iceberg-spark-runtime-3.5_2.13-1.10.0.jar` |
| 4.0.x | 2.13 | `iceberg-spark-runtime-4.0_2.13-1.10.0.jar` (Iceberg ≥ 1.10) |
| 4.1.x | 2.13 | `iceberg-spark-runtime-4.1_2.13-1.11.0.jar` (Iceberg ≥ 1.11) |

Manual checks if you prefer them:
```bash
java -version
hadoop version | head -1
spark-submit --version 2>&1 | grep -E "version|Scala"
ls $SPARK_HOME/jars | grep -E "^spark-core_|^iceberg-spark-runtime"   # spark-core_<scala>-<spark>.jar
python3 --version
```
If the jar is missing, it is enough to download it **on master only**; step 7 copies it to the other two:
```bash
wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.0/iceberg-spark-runtime-3.5_2.12-1.10.0.jar
```
Write the final versions into the report ("fixed, documented versions" is a risk mitigation in your proposal).

### Step 4 — Network setup (every laptop, its own role)
```bash
./scripts/01_network_setup.sh master     # on Node 1
./scripts/01_network_setup.sh worker1    # on Node 2
./scripts/01_network_setup.sh worker2    # on Node 3
```
This script:
- renames the laptop (hostname);
- writes this block to `/etc/hosts`:
  ```
  <MASTER_IP>   master
  <WORKER1_IP>  worker1
  <WORKER2_IP>  worker2
  ```
- comments out Ubuntu's `127.0.1.1 <hostname>` line (otherwise the NameNode listens on loopback only);
- installs and starts `openssh-server`;
- allows the other two IPs through `ufw`;
- enables NTP and disables suspend (so closing the lid does not kill the DataNode).

Open a new terminal afterwards so the prompt shows the new hostname. A one-off `sudo: unable to resolve host` warning during the rename is harmless.

### Step 5 — Passwordless SSH (master)
```bash
./scripts/02_ssh_setup.sh
```
This creates an ed25519 key and writes `~/.ssh/config` with each laptop's user name. It then runs `ssh-copy-id` to `master`, `worker1` and `worker2`; you type each password once. It finishes by checking that `ssh worker1 hostname` returns `worker1` with no password prompt. Running it on the workers too (all-to-all SSH) is optional.

Manual equivalent:
```bash
ssh-keygen -t ed25519 -N "" -f ~/.ssh/id_ed25519
for h in master worker1 worker2; do ssh-copy-id $h; done
for h in master worker1 worker2; do ssh -o BatchMode=yes $h hostname; done
```

### Step 6 — Prepare each node (every laptop)
```bash
./scripts/03_prepare_node.sh            # auto-detects Hadoop/Spark
# or: ./scripts/03_prepare_node.sh /path/to/hadoop-3.x.y /path/to/spark-3.5.x-bin-hadoop3
source ~/.bashrc
```
This script:
- makes `/opt/hadoop` and `/opt/spark` symlinks to the real install dirs. `start-dfs.sh` and `start-workers.sh` run the *master's* paths on the workers, so the paths must be identical everywhere;
- creates `/bigdata/...` (HDFS blocks, YARN, Spark spill, logs, staging, evidence);
- adds the environment variables to `~/.bashrc`.

### Step 7 — Install configs on all nodes (master)
```bash
./scripts/04_install_configs.sh
```
This script:
- copies `conf/hadoop/*` → `$HADOOP_HOME/etc/hadoop/` and `conf/spark/*` → `$SPARK_HOME/conf/` on all 3 nodes (backing up the old files to `~/conf-backup-*.tgz`);
- appends the `JAVA_HOME`/log/pid block to `hadoop-env.sh`;
- copies the Iceberg jar into `$SPARK_HOME/jars` everywhere, which also works offline;
- **fails** if a node runs a different Hadoop or Spark version.

It ends by printing `dfs.replication = 3` and `dfs.blocksize = 134217728`. Re-run it after any config edit, then restart the cluster.

Key settings (full comments are in the files):

| File | Setting | Value |
|---|---|---|
| core-site.xml | `fs.defaultFS` | `hdfs://master:9000` |
| hdfs-site.xml | `dfs.replication` / `dfs.blocksize` | `3` / `134217728` |
| hdfs-site.xml | `dfs.namenode.rpc-bind-host` | `0.0.0.0` |
| hdfs-site.xml | `dfs.client.block.write.replace-datanode-on-failure.policy` | `NEVER` (Hadoop's advice for ≤ 3 nodes) |
| hdfs-site.xml | `dfs.permissions.enabled` | `false` (different user names per laptop) |
| yarn-site.xml | `yarn.resourcemanager.hostname` | `master` |
| spark-defaults.conf | `spark.master` | `spark://master:7077` |
| spark-defaults.conf | `spark.sql.catalog.lake` (+ `.type`, `.warehouse`) | Iceberg `SparkCatalog`, `hadoop`, `hdfs://master:9000/warehouse/iceberg` |
| spark-defaults.conf | `spark.sql.files.maxPartitionBytes` | `134217728` (1 read task per HDFS block) |
| spark-defaults.conf | `spark.eventLog.dir` | `hdfs://master:9000/spark-logs` (job history = experiment evidence) |

### Step 8 — Format the NameNode (master, ONCE)
```bash
./scripts/05_format_namenode.sh
# raw: hdfs namenode -format -nonInteractive -clusterId CID-ecommerce-lakehouse
```
The script refuses to run a second time. `--force` wipes HDFS on all 3 nodes and re-formats. Never re-format only the NameNode: the DataNodes would then fail with *Incompatible clusterIDs*.

### Step 9 — Start the cluster (master)
```bash
./scripts/06_start_cluster.sh
```
Raw equivalent:
```bash
start-dfs.sh && hdfs dfsadmin -safemode wait
start-yarn.sh
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-workers.sh
$SPARK_HOME/sbin/start-history-server.sh
```
Expected `cluster_status.sh` output:
```
--- master    DataNode, HistoryServer, Master, NameNode, NodeManager, ResourceManager, SecondaryNameNode, Worker
--- worker1   DataNode, NodeManager, Worker
--- worker2   DataNode, NodeManager, Worker
[ OK ] HDFS: 3/3 DataNodes live
[ OK ] YARN: 3/3 NodeManagers running
[ OK ] Spark: 3/3 workers ALIVE, 12 cores total
```
To stop everything: `./scripts/stop_cluster.sh`. Both scripts are safe to re-run, for example after a laptop slept.

### Step 10 — Move `archive (7).zip` from Windows to master
Pick one option:

**A. Windows PC on the same network** (PowerShell; OpenSSH client is built into Windows 10/11):
```powershell
scp "C:\Users\user\Downloads\archive (7).zip" <MASTER_USER>@<MASTER_IP>:/bigdata/staging/ecommerce-archive.zip
```
**B. Dual-boot (Windows partition on the master laptop).** Click the Windows drive in Ubuntu *Files* to mount it, then:
```bash
cp "/media/$USER/<WINDOWS_DRIVE>/Users/user/Downloads/archive (7).zip" /bigdata/staging/ecommerce-archive.zip
```
**C. USB stick.** It must be **exFAT or NTFS**, because FAT32 cannot hold files of 4 GB or more.
```bash
cp "/media/$USER/<USB>/archive (7).zip" /bigdata/staging/ecommerce-archive.zip
```
Renaming the file to `ecommerce-archive.zip` removes the spaces and parentheses that break shell commands.

### Step 11 — Unzip and build the manifest (master)
```bash
./scripts/07_stage_dataset.sh /bigdata/staging/ecommerce-archive.zip
```
This unzips to `/bigdata/staging/ecommerce/` and checks the 9-column header. It then writes `manifest.csv` with exact byte sizes and row counts (≈ 42.4 M rows in October, 67,501,979 in November, ≈ 110 M in total). Every later phase checks its counts against this manifest.

Streaming alternative that skips the 14.7 GB local unzip:
```bash
unzip -p /bigdata/staging/ecommerce-archive.zip 2019-Oct.csv | \
  hdfs dfs -D dfs.replication=3 -D dfs.blocksize=134217728 -put - /data/raw/ecommerce/2019-Oct.csv
```
With this route you lose the local row-count manifest, so the scripted path is recommended.

### Step 12 — Ingest into HDFS (master)
```bash
./scripts/08_ingest_hdfs.sh
```
Raw equivalent:
```bash
hdfs dfs -mkdir -p /data/raw/ecommerce
hdfs dfs -D dfs.replication=3 -D dfs.blocksize=134217728 -put /bigdata/staging/ecommerce/2019-Oct.csv /data/raw/ecommerce/
hdfs dfs -D dfs.replication=3 -D dfs.blocksize=134217728 -put /bigdata/staging/ecommerce/2019-Nov.csv /data/raw/ecommerce/
hdfs dfs -setrep -w 3 /data/raw/ecommerce
```
The script also:
- skips files that are already uploaded;
- logs seconds and MB/s per file to `/bigdata/evidence/phase1/ingest_log.csv` (ingestion throughput for the report);
- copies the manifest and log to `hdfs:///data/meta/ingest/`.

Expect roughly 10–25 min on Wi-Fi and 3–6 min on gigabit Ethernet.

### Step 13 — Verify distribution and replication (master)
```bash
./scripts/09_verify_hdfs.sh
```
Raw equivalent:
```bash
hdfs dfsadmin -report
hdfs fsck /data/raw/ecommerce/ -files -blocks -locations
hdfs dfs -stat "%n replication=%r blocksize=%o bytes=%b" "/data/raw/ecommerce/*.csv"
```
Checks performed, each reported as PASS or FAIL:

| Check | Expected |
|---|---|
| Live DataNodes | 3 |
| `dfs.replication` / `dfs.blocksize` | 3 / 134217728 |
| Per file: replication, block size, bytes = manifest | 3, 128 MB, equal |
| Blocks per file = ⌈size / 128 MB⌉ | ≈ 43 (Oct), ≈ 68 (Nov), ≈ 111 total |
| fsck status / average replication / under-replicated / missing | HEALTHY / 3.0 / 0 / 0 |
| Replicas per DataNode | each node holds all ≈ 111 blocks (≈ 333 replicas in total) |

The full outputs are saved for the report in `/bigdata/evidence/phase1/hdfs_verify_<timestamp>/`.

A line of `fsck` output looks like this (each block lists the 3 laptops holding it):
```
/data/raw/ecommerce/2019-Nov.csv 9006762395 bytes, replicated: replication=3, 68 block(s):  OK
0. BP-...:blk_1073741825_1001 len=134217728 Live_repl=3  [DatanodeInfoWithStorage[192.168.1.10:9866,...], DatanodeInfoWithStorage[192.168.1.11:9866,...], DatanodeInfoWithStorage[192.168.1.12:9866,...]]
```

### Step 14 — Spark + Iceberg acceptance test (master)
```bash
./scripts/10_run_smoke_test.sh            # add --yarn to also run MapReduce "pi" on YARN
```
`jobs/phase1_smoke_test.py` checks that:
- tasks ran on `master`, `worker1` and `worker2`;
- the number of input partitions ≈ the number of HDFS blocks;
- Spark's row count per file equals the manifest (one distributed full scan, which also prints the `event_type` distribution and the scan time);
- the Iceberg catalog `lake` creates namespaces **`lake.bronze`, `lake.silver`, `lake.gold`**;
- a throw-away table supports INSERT, snapshots and `VERSION AS OF` time travel, with its data files under `hdfs://master:9000/warehouse/iceberg/`.

The results are written to `/bigdata/evidence/phase1/smoke_test_<timestamp>.json`. When it prints `ALL CHECKS PASSED`, Phase 1 is complete.

---

## 4. Troubleshooting

| Symptom | Cause → fix |
|---|---|
| `Live datanodes (1)`; worker logs show `Retrying connect to server: master/…:9000` | Firewall, or the NameNode bound to loopback. On master, `ss -ltnp \| grep 9000` must show `0.0.0.0:9000`. Re-run `01` (127.0.1.1 line) and `04`. |
| DataNode log: `Incompatible clusterIDs` | The NameNode was re-formatted. Run `05_format_namenode.sh --force`, or delete `/bigdata/hadoop/hdfs/datanode/*` on that node and restart. |
| `ERROR: JAVA_HOME is not set` from start-dfs.sh | The `hadoop-env.sh` block is missing → re-run `04`. |
| `Permission denied (publickey)` while starting | Re-run `02`; check user names in `~/.ssh/config`. |
| `ping worker1` fails | Wi-Fi client isolation → use a hotspot or router; or a wrong IP in `cluster.env`. |
| Spark: `Initial job has not accepted any resources` | No worker has enough free memory/cores for one executor (check http://master:8080), or a firewall blocks Spark's random ports. |
| `Python in worker has different version` | Install the same Python 3 minor version on all nodes; `PYSPARK_PYTHON=/usr/bin/python3`. |
| `ClassNotFoundException: ...IcebergSparkSessionExtensions` or `NoSuchMethodError` | Iceberg jar missing or built for another Spark/Scala version → step 3 table, re-run `04`. |
| `Name node is in safe mode` | Wait (`hdfs dfsadmin -safemode wait`). Only if all 3 DataNodes are live and it persists: `hdfs dfsadmin -safemode leave`. |
| YARN NodeManager `UNHEALTHY` | Disk above 95% → delete the staging CSVs, `hdfs dfs -expunge`. |
| A laptop slept or rebooted | Re-run `./scripts/06_start_cluster.sh` (idempotent). If its IP changed, see §2. |
| Daemon logs | `/bigdata/hadoop/logs/` (HDFS/YARN), `/bigdata/spark/logs/` (Spark) on each node. |

---

## 5. Where distributed computing is used in Phase 1 (for the report)

- **Storage (HDFS).** Each CSV is cut into 128 MB blocks (≈ 111 in total), and each block is replicated on all 3 DataNodes. The NameNode keeps only metadata (block → DataNode map); the bytes live on the three laptops.
- **Ingestion.** `hdfs dfs -put` streams each block through a replication pipeline (master → worker1 → worker2), so replicas are written in parallel with the upload.
- **Compute (Spark Standalone).** 3 workers × 4 cores = 12 executor cores. Reading `/data/raw/ecommerce/*.csv` creates about one task per block, and tasks are scheduled on a node that holds the block (data locality). The smoke test's `groupBy` is a distributed shuffle across the laptops.
- **Table format (Iceberg on HDFS).** The catalog metadata and Parquet data files are HDFS files, so they are replicated ×3 as well. Snapshots and time travel come from Iceberg's metadata layer, without copying data.

---

## 6. Evidence produced (keep for the final report)

```
/bigdata/evidence/phase1/
├── ingest_log.csv                            # upload seconds + MB/s per file
├── hdfs_verify_<ts>/dfsadmin_report.txt      # capacity and usage per DataNode
├── hdfs_verify_<ts>/fsck_files_blocks_locations.txt
├── hdfs_verify_<ts>/replicas_per_datanode.txt
├── hdfs_verify_<ts>/stat.txt, manifest.csv
└── smoke_test_<ts>.json                      # hosts, partitions, row counts, Iceberg checks
```
Screenshots worth taking for the slides: NameNode UI → *Datanodes* tab (3 live), *Utilities → Browse* → a file's block info showing 3 locations, Spark Master UI with 3 workers, and the smoke-test job in the History Server.
