# Distributed Clickstream Lakehouse — Phase 3

Distributed AI / ML pipeline with Spark MLlib (Task 5). Both jobs read the Phase 2 Iceberg table
`local.ecommerce.silver_events`, do all feature work and training on the Spark cluster, and write
small result files to `./results/`.

```
silver_events ──05──> ml_cart_features ──> Pipeline(StringIndexer, OneHotEncoder, VectorAssembler,
                                             weighted LogisticRegression) ──> logreg_metrics.json
silver_events ──06──> ml_user_item_interactions ──> ALS(implicitPrefs) ──> gold_user_recommendations
                                                                         └─> als_metrics.json
```

## Files

```
jobs/phase3/
├── 05_ml_purchase_prediction.py   # cart -> purchase classifier (LogisticRegression)
└── 06_ml_recommender_als.py       # implicit-feedback recommender (ALS)
scripts/12_run_phase3.sh           # runs 5 and 6 with spark-submit; logs in /bigdata/evidence/phase3/logs
```
The jobs reuse `jobs/phase2/common.py` (Spark session, Iceberg catalog `local`, helpers).

## Run (on master, from the project root, cluster 3/3)

```bash
./scripts/12_run_phase3.sh --dev      # quick test on 5 % / 2 % of users, *_dev tables, ./results_dev
./scripts/12_run_phase3.sh            # full run
./scripts/12_run_phase3.sh 6          # only one job
ALS_SAMPLE=0.5 ./scripts/12_run_phase3.sh 6    # ALS on more users (default 0.2)
ALS_EXTRA="--reuse" ./scripts/12_run_phase3.sh 6   # reuse the saved model, only recommend + evaluate
./scripts/12_run_phase3.sh 5b         # readable names for the saved LogReg coefficients
```

### Exact spark-submit commands (what the runner executes)

```bash
cd ~/ecommerce-lakehouse
export LAKE_NS=ecommerce RESULTS_DIR=./results EVIDENCE_DIR=/bigdata/evidence/phase3
SUBMIT="$SPARK_HOME/bin/spark-submit --master spark://master:7077 --py-files jobs/phase2/common.py"

$SUBMIT --name phase3-05_ml_purchase_prediction jobs/phase3/05_ml_purchase_prediction.py --sample 1.0
$SUBMIT --name phase3-06_ml_recommender_als     jobs/phase3/06_ml_recommender_als.py --sample 0.2
```

Optional ALS parameters: `--eval-users 20000` (0 = recommendForAllUsers for every user), `--reuse`, `--rank 20 --max-iter 10 --reg 0.1 --alpha 10 --min-item-users 20
--min-user-items 3 --max-user-items 500 --k 10 --candidates 50`.

## Job 05 — Purchase prediction (binary classification)

| Item | Choice |
|---|---|
| Unit | the **first cart** of a product in a session |
| Label | 1 if the **same product is purchased later in the same session**, else 0 |
| Features | log price; price ÷ category average price; hour; day of week; weekend; events, views and carts earlier in the session; views of this product earlier in the session; log seconds since session start; user history from **previous** sessions (sessions, views, carts, purchases, has bought before); `main_category` and `brand` (top 300 + "other") via `StringIndexer` → `OneHotEncoder` → `VectorAssembler` |
| Leakage control | every feature uses only rows **before** the cart (window frames end at the previous row); user history uses earlier sessions only |
| Imbalance | `weightCol`: balanced class weights `n / (2·n_class)` computed on the training set |
| Split | 80 / 20 **by user** (hash of `user_id`), so one customer is never in both sets |
| Model | `LogisticRegression(maxIter=100, regParam=0.01)` inside a Spark ML `Pipeline` |
| Data quality | days with carts but zero purchases (2019-11-15) are excluded, otherwise their carts would be wrongly labelled 0 |
| Metrics | ROC-AUC, PR-AUC (+ random baseline = positive rate), accuracy, precision, recall, F1, confusion matrix |

Distributed work: one shuffle puts each `(user_id, user_session)` on one task; all session- and
product-level window features and the label are computed there without another shuffle. User history is a
window over ~23 M session rows. The feature table is stored in Iceberg (`ml_cart_features`), the ML
pipeline trains on it with the data cached across the executors.

Outputs: `results/logreg_metrics.json`, `results/logreg_coefficients.csv` (feature importance),
`results/logreg_curve_points.csv` (ROC / PR curves), model in `hdfs:///models/logreg_cart_purchase`.

Note: purchases are ~1.5 % of all events, but at the cart level the positive rate is much higher
(roughly every second cart session ends in a purchase). The job prints the real rate and computes the
weights from it.

## Job 06 — ALS recommender (implicit feedback)

| Step | Choice |
|---|---|
| Interaction score | view = 1, cart = 3, purchase = 5, summed per (user, product); ALS rating = `log1p(score)` |
| Pruning | products with ≥ 20 users; users with 3–500 products (very long histories are usually bots); `--sample` = fraction of users by hash (default 0.2 for the laptops) |
| Split | random 80 / 20 of the (user, product) pairs |
| Model | `ALS(implicitPrefs=True, rank=20, maxIter=10, regParam=0.1, alpha=10, coldStartStrategy="drop")`, checkpoints in HDFS; saved right after training so later runs can `--reuse` it |
| Recommendations | `recommendForUserSubset(50)` for a deterministic sample of 20,000 evaluation users (`--eval-users 0` = `recommendForAllUsers` for everyone; that took over 2.5 h on the laptops) → remove products already seen in train → top 10 per user → Iceberg `gold_user_recommendations` (with category, brand, avg price) |
| Metrics | Precision@10, Recall@10, NDCG@10 (`RankingEvaluator`) on all held-out interactions and on held-out carts/purchases; **most-popular baseline** with the same filtering; RMSE of predicted preference on held-out pairs |

RMSE note: implicit ALS predicts a preference between 0 and 1, not the original score, so RMSE is measured
against 1.0 for held-out interactions and is only a sanity check. Precision/Recall@10 and the lift over
the popularity baseline are the numbers to report.

Outputs: `results/als_metrics.json`, `results/als_sample_recommendations.csv` (10 users × 10 products),
Iceberg `local.ecommerce.gold_user_recommendations`, model in `hdfs:///models/als_implicit`.

## Evidence

`/bigdata/evidence/phase3/` — JSON per run (timings per step, dataset sizes, metrics) and `logs/`.
Spark UI during a run: `http://192.168.100.47:4040` → Jobs (labelled steps), Stages, Executors.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `OutOfMemoryError` / executor lost during ALS | lower `ALS_SAMPLE` (e.g. 0.1) or `--rank 10`; check all laptops are awake |
| ALS very slow | expected over Wi-Fi: each iteration shuffles factors between laptops; use `--max-iter 5` |
| `Table ... silver_events not found` | run Phase 2 first |
| Re-run | safe: tables are recreated, models overwritten |
