# Distributed Clickstream Lakehouse on a 3-Laptop Cluster

**CSS 439 Distributed Big Data Systems · SDU University** · Aiken Otargali (master) · Zhuldyzay Kalka (worker1) · Inkar Tlegenova (worker2)

End-to-end pipeline on 110 million real e-commerce events (Kaggle / REES46, Oct–Nov 2019, 14.7 GB):

```
Kaggle CSV ─► HDFS (3 replicas) ─► Spark ─► Apache Iceberg (Bronze → Silver → Gold) ─► Spark MLlib ─► results + charts
              Phase 1                Phase 2                                            Phase 3         Phase 4
```

| | |
|---|---|
| Cluster | 3 laptops on one Wi-Fi: 2 × WSL2 Ubuntu (mirrored networking) + 1 × VirtualBox Ubuntu (bridged) |
| Software | Hadoop 3.3.6 (Java 8) · Spark 4.1.4 / Scala 2.13 (Java 17) · Iceberg 1.11.0 · Python 3.14 |
| Data | 109,950,743 raw events → 109,563,335 cleaned events, 23.0 M sessions, 5.3 M users |

Phase documents: [`README_Phase1.md`](README_Phase1.md) (cluster + HDFS), [`README_Phase2.md`](README_Phase2.md)
(Spark + Iceberg), [`README_Phase3.md`](README_Phase3.md) (ML). This file: project map, Phase 4 and the demo runbook.

---

## 1. Directory tree

```
ecommerce-lakehouse/
├── README.md                         ← this file: map, Phase 4, live demo runbook
├── README_Phase1.md  README_Phase2.md  README_Phase3.md
├── cluster.env                       IPs, users, paths, HDFS layout (single source of truth)
├── conf/
│   ├── hadoop/   core-site.xml  hdfs-site.xml  yarn-site.xml  mapred-site.xml  workers  hadoop-env.sh.append
│   └── spark/    spark-env.sh  spark-defaults.conf  workers
├── scripts/                          cluster operations (bash, run on master unless noted)
│   ├── 00_check_versions.sh          every node: versions, RAM, disk
│   ├── 01_network_setup.sh           every node: hostname, /etc/hosts, SSH server
│   ├── 02_ssh_setup.sh               master: password-less SSH to all nodes
│   ├── 03_prepare_node.sh            every node: /opt/hadoop, /opt/spark, /bigdata layout
│   ├── 04_install_configs.sh         master: push Hadoop/Spark configs + Iceberg jar to all nodes
│   ├── 05_format_namenode.sh         master: ONE time only
│   ├── 06_start_cluster.sh           HDFS + YARN + Spark Standalone + History Server
│   ├── 07_stage_dataset.sh  08_ingest_hdfs.sh  09_verify_hdfs.sh  10_run_smoke_test.sh
│   ├── 11_run_phase2.sh              Phase 2 runner (Bronze, Silver, Iceberg features, Gold)
│   ├── 12_run_phase3.sh              Phase 3 runner (LogReg, ALS)
│   ├── cluster_status.sh  stop_cluster.sh  lib.sh
├── jobs/
│   ├── phase1_smoke_test.py          Spark + HDFS + Iceberg smoke test
│   ├── phase2/
│   │   ├── common.py                 SparkSession, Iceberg catalog `local`, table names, helpers
│   │   ├── 01_ingest_bronze.py       CSV → bronze_events (days(event_time), event_type)
│   │   ├── 02_clean_silver.py        DQ, filter, dedup, sessionization → silver_events
│   │   ├── 03_iceberg_features_demo.py  time travel + rollback, schema evolution, MERGE INTO
│   │   └── 04_gold_aggregations.py   funnel, RFM, hourly trends → gold_* + CSV
│   ├── phase3/
│   │   ├── 05_ml_purchase_prediction.py  weighted LogisticRegression (cart → purchase)
│   │   ├── 05b_name_coefficients.py      readable coefficient names from the saved model
│   │   └── 06_ml_recommender_als.py      implicit ALS recommender + ranking metrics
│   └── phase4/
│       ├── 07_run_benchmark.sh       scalability (p = 1, 2, 4, 8 × 3 runs) + CSV vs Iceberg
│       ├── 07_benchmark_workload.py  benchmark workload / summary (S_p, E_p)
│       ├── 07b_shuffle_analysis.py   local vs Wi-Fi shuffle traffic per run (History Server API)
│       ├── 08_visualize_results.py   pandas + matplotlib charts from ./results (300 dpi)
│       ├── 09_demo_iceberg.py        live Iceberg demo (snapshots, time travel, delete + rollback)
│       └── 09_demo.sh                runs the live demo
├── results/                          SMALL outputs only (committed to Git with git add -f)
│   ├── gold_funnel_metrics.csv  gold_rfm_segments_summary.csv  gold_rfm_rf_grid.csv  gold_hourly_trends.csv
│   ├── logreg_metrics.json  logreg_coefficients.csv  logreg_curve_points.csv
│   ├── als_metrics.json  als_sample_recommendations.csv
│   ├── benchmark_raw.jsonl  benchmark_results.csv  benchmark_shuffle.csv
│   └── plots/   01_conversion_funnel.png … 10_hourly_heatmap.png
└── evidence/                         copies of /bigdata/evidence/* (JSON per run, plans, logs)
```

Data that lives on the cluster, not in Git:

| Where | What |
|---|---|
| HDFS `/data/raw/ecommerce/` | `2019-Oct.csv`, `2019-Nov.csv` (111 blocks × 3 replicas) |
| HDFS `/warehouse/iceberg/ecommerce/` | Iceberg tables: `bronze_events`, `silver_events`, `daily_category_summary`, `daily_category_increment`, `gold_funnel_metrics`, `gold_rfm_segments`, `gold_rfm_segment_summary`, `gold_rfm_rf_grid`, `gold_hourly_trends`, `ml_cart_features`, `ml_user_item_interactions`, `gold_user_recommendations` |
| HDFS `/models/` | `logreg_cart_purchase`, `als_implicit` |
| HDFS `/spark-logs/` | Spark event logs (History Server) |
| local `/bigdata/` on each laptop | DataNode blocks, Spark scratch, logs, `/bigdata/evidence/` |

## 2. Where distributed computing is used

| Step | Distributed work | Evidence |
|---|---|---|
| Storage | HDFS splits 14.7 GB into 111 × 128 MB blocks, 3 replicas on 3 laptops; survived a worker dropping out during ingestion | `09_verify_hdfs.sh`, `hdfs fsck` |
| Bronze | 110 parallel read tasks, one per HDFS block, data-local, no shuffle | Spark UI stages, `01_*.json` |
| Silver | one hash shuffle by `(user_id, user_session)` serves dedup + window sessionization | `02_silver_plan_ecommerce.txt` |
| Gold | groupBy shuffles, distributed `percentile_approx`, HyperLogLog distinct counts | `04_*.json` |
| ML | window feature engineering, Spark ML pipeline, distributed L-BFGS (LogReg), block ALS | `05_*.json`, `06_*.json` |
| Benchmark | same workload on 1 → 8 executor cores across 3 laptops | `results/benchmark_results.csv`, `03_scalability.png` |

---

## 3. Phase 4 — benchmark and charts

### 3.1 Scalability + storage benchmark (≈ 45–70 min)

```bash
cd ~/ecommerce-lakehouse
./scripts/cluster_status.sh                     # 3/3 everywhere
chmod +x jobs/phase4/*.sh
./jobs/phase4/07_run_benchmark.sh               # warm-up + scaling + storage + summary
```

* **Scalability:** workload = filter (2 weeks of Silver, partition-pruned, `price > 0`) → shuffle-heavy
  session aggregation `groupBy(user_id, user_session)` → category revenue `groupBy(main_category)`.
  For p = 1, 2, 4, 8: `--total-executor-cores p`, 1-core executors with 700 MB each (equal memory per core),
  `spark.sql.shuffle.partitions = 4p`, AQE coalescing off. Fresh `spark-submit` per run, timer starts after
  all p executors registered, one warm-up run, runs interleaved (run 1: p1 p2 p4 p8, run 2: …).
  Result: median $T_p$, $S_p = T_1/T_p$, $E_p = S_p/p$.
* **Storage:** raw CSV vs Iceberg `bronze_events`, 3 interleaved runs, results checked equal.
  Q1 = purchases of one day (Iceberg reads 1 partition), Q2 = revenue per event type over all rows
  (no pruning possible; Iceberg reads only 2 of 9 columns).
* Output: `results/benchmark_results.csv` (`record_type` = raw / summary), raw lines in
  `results/benchmark_raw.jsonl`, logs in `/bigdata/evidence/phase4/logs/`.
* Partial runs: `./jobs/phase4/07_run_benchmark.sh scaling | storage | summarize`;
  start over with `FRESH=1`; other settings `RUNS=3 PS="1 2 4 8" START=2019-11-01 END=2019-11-15`.

### 3.2 Why the curve looks like that: shuffle traffic per run

```bash
python3 jobs/phase4/07b_shuffle_analysis.py                 # reads the History Server on :18080
```
For every scaling run: executors per laptop, shuffle MB read locally vs over Wi-Fi, fetch-wait time,
spill to disk → `results/benchmark_shuffle.csv` (raw + median per p) and chart 11.

### 3.3 Charts

```bash
sudo apt install -y python3-pandas python3-matplotlib      # once, on master
python3 jobs/phase4/08_visualize_results.py                 # -> results/plots/*.png (300 dpi)
explorer.exe "$(wslpath -w results/plots)"                  # open the folder in Windows
```

| File | Shows |
|---|---|
| `01_conversion_funnel.png` | view → cart → purchase sessions, overall + top 8 categories |
| `02_rfm_segments.png` | share of buyers vs share of revenue per RFM segment |
| `03_scalability.png` | $T_p$ and $S_p$ vs ideal linear for p = 1, 2, 4, 8 |
| `04_storage_format.png` | raw CSV vs Iceberg latency for Q1 (pruning) and Q2 (columnar) |
| `05_logreg_roc_pr.png` · `06_logreg_confusion_matrix.png` · `07_logreg_coefficients.png` | purchase prediction model |
| `08_als_vs_popularity.png` | ALS vs most-popular: Precision / Recall / NDCG @10 |
| `09_daily_activity.png` · `10_hourly_heatmap.png` | daily views / purchases (15 Nov gap), weekday × hour |
| `11_shuffle_network.png` | shuffle data read locally vs over Wi-Fi, fetch-wait time per p |

### 3.4 Save to Git

```bash
mkdir -p evidence && cp -r /bigdata/evidence/phase4 evidence/
git add -A && git add -f results/*.csv results/*.jsonl results/plots/*.png
git commit -m "Phase 4: scalability + storage benchmark, charts, demo runbook" && git push
```

---

## 4. Live demo runbook (10 minutes)

### Before the teacher arrives (T − 30 min)

- [ ] All 3 laptops: charger in, Windows sleep = **Never**, lid close = **Do nothing**, same Wi-Fi, `hostname -I` unchanged (else update `cluster.env` + rerun `01_network_setup.sh` on all).
- [ ] Aiken's and Inkar's Ubuntu terminals open (closing them stops WSL).
- [ ] Master: `cd ~/ecommerce-lakehouse && ./scripts/06_start_cluster.sh && ./scripts/cluster_status.sh` → `3/3 DataNodes`, `3/3 NodeManagers`, `3/3 workers ALIVE`.
- [ ] Dry run of the Iceberg demo once: `./jobs/phase4/09_demo.sh` (it restores the table at the end).
- [ ] Browser tabs open (use IPs, Windows does not know `master`):
      `http://192.168.100.47:9870` (HDFS) · `http://192.168.100.47:8080` (Spark Master) · `http://192.168.100.47:18080` (History).
- [ ] `results/plots/` open in Windows Explorer; slides open; screenshots in a folder as a backup.
- [ ] Two terminals on master, font large (Ctrl + / zoom), `clear` before each step.

### The 10 minutes

| Time | Who | Show | Commands / clicks | Say |
|---|---|---|---|---|
| 0:00–1:00 | Aiken | architecture slide | — | 3 laptops, HDFS → Spark → Iceberg → MLlib; 110 M events |
| 1:00–3:00 | Aiken | **HDFS: 3 replicas on 3 laptops** | Browser `:9870` → *Datanodes*: 3 live nodes, similar used space on each. Terminal: see block below | "each 128 MB block is stored on all three laptops" |
| 3:00–4:30 | Zhuldyzay | **Spark Master: 3 workers** | Browser `:8080` → 3 workers ALIVE, 12 cores; `:18080` → an app → *Executors* tab: executors on 3 IPs; *Stages*: shuffle read/write | "work is spread across all three machines" |
| 4:30–6:30 | Inkar | **Iceberg time travel live** | `./jobs/phase4/09_demo.sh` (start it, talk while Spark starts ~30 s) | snapshots → MERGE before/after → live DELETE → `VERSION AS OF` still has the rows → rollback |
| 6:30–8:30 | Inkar | **ML metrics + charts** | `python3 -m json.tool results/logreg_metrics.json \| head -25` · `python3 -m json.tool results/als_metrics.json \| head -40` · open `05`, `08`, `01`, `02` PNGs | ROC-AUC 0.72 / PR-AUC 0.66 vs 0.43 baseline; ALS 2.3× better than popularity |
| 8:30–9:30 | Zhuldyzay | **scalability + storage** | open `03_scalability.png`, `04_storage_format.png` | speed-up and efficiency; why not linear (Wi-Fi shuffle, startup); Iceberg pruning + columnar |
| 9:30–10:00 | all | limitations + questions | — | Wi-Fi bottleneck, 15 Nov purchase gap, ALS on 20 % of users |

**HDFS commands for 1:00–3:00** (terminal on master):

```bash
hdfs dfs -ls -h /data/raw/ecommerce
hdfs fsck /data/raw/ecommerce/2019-Oct.csv -files -blocks -locations | grep -m 3 "len="
hdfs fsck /data/raw/ecommerce | grep -E "Total blocks|Average block replication|Under-replicated|Status"
hdfs dfs -du -h -s /warehouse/iceberg/ecommerce
```
Each `len=134217728 Live_repl=3 [...]` line lists three DataNodes: 192.168.100.47 (master),
192.168.100.67 (worker1), 192.168.100.15 (worker2).

### If something goes wrong during the demo

| Problem | Do this |
|---|---|
| A worker shows as dead | `ping` it; wake the laptop; on master `./scripts/06_start_cluster.sh` (already-running daemons are skipped). HDFS still serves all data from the other replicas — say so, it is the point of replication 3 |
| `09_demo.sh` hangs at start | Spark waits for executors: check `:8080`; if needed run it again; fallback = the saved output in `evidence/phase2/logs/03_*.log` |
| Browser cannot open `:9870` | use the IP, not `master`; check Windows firewall settings from Phase 1 |
| No time left | skip to the charts; every number is also in `results/*.json` |

---

## 5. Key results (full data)

| Area | Result |
|---|---|
| Ingestion | 14.7 GB → HDFS in 27 min over Wi-Fi (≈ 8.5 MB/s × 3 replicas), 111 blocks × 3, HEALTHY |
| Bronze / Silver | 109,950,743 → 256,773 invalid, 130,635 duplicates → 109,563,335 rows; 23.0 M sessions |
| Iceberg | time travel + rollback, metadata-only schema evolution (66 files unchanged), MERGE INTO verified (0 mismatches) |
| Funnel | view → cart 10.1 %, cart → purchase 60.6 %, view → purchase 6.1 % of sessions; electronics 75.6 % of revenue |
| RFM | Champions = 6.6 % of buyers, 31.5 % of revenue; 86.9 % of users never buy |
| LogReg | ROC-AUC 0.720, PR-AUC 0.664 (baseline 0.431), accuracy 0.664, F1 0.623 |
| ALS | Precision@10 0.047 vs 0.020 popularity (2.3×), Recall@10 0.155 vs 0.067 |
| Scalability (2 weeks, 26.3 M events) | median T₁ 94.1 s → T₂ 40.6 s (S = 2.32) → T₄ 65.8 s (S = 1.43) → T₈ 68.2 s (S = 1.38); beyond 2 cores the shuffle crosses Wi-Fi (chart 11) |
| Storage | Iceberg vs raw CSV, identical results: one-day query 2.7 s vs 42.6 s (15.8×), full scan of 2 columns 7.8 s vs 41.8 s (5.4×) |

## 6. Limitations

* Wi-Fi (~10 MB/s) is the bottleneck for every shuffle and for HDFS replication; a wired switch would change the speed-up curve.
* 8–9 GB laptops: executors of 1.4 GB (700 MB in the benchmark); ALS trained on 20 % of users and evaluated on a 20,000-user sample (`recommendForAllUsers` for all 585 k users did not finish in 2.5 h).
* Source data has no purchase events on 2019-11-15 (views and carts present); excluded from the ML labels, visible in chart 09.
* Session conversion uses the dataset's `user_session` id; times are UTC.
