"""
Phase 3 / Job 5b - give the saved LogisticRegression coefficients readable names.

Loads hdfs:///models/logreg_cart_purchase (written by job 05), rewrites
./results/logreg_coefficients.csv and the "top_coefficients" in
./results/logreg_metrics.json. No retraining (about a minute).

Usage: spark-submit ... 05b_name_coefficients.py [--tag _dev]
"""
import argparse
import importlib.util
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "phase2"))

from pyspark.ml import PipelineModel  # noqa: E402

import common as C  # noqa: E402

# reuse the naming logic of job 05 (file name starts with a digit -> load by path)
_spec = importlib.util.spec_from_file_location("job05", os.path.join(HERE, "05_ml_purchase_prediction.py"))
job05 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(job05)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="")
    a = ap.parse_args()
    spark = C.get_spark("phase3-05b-name-coefficients")
    model = PipelineModel.load(f"{job05.MODEL_DIR}/logreg_cart_purchase{a.tag}")
    coefs, intercept = job05.coefficient_table(model)

    job05.write_csv([(n, round(c, 6)) for n, c in coefs], ["feature", "coefficient"],
                    f"logreg_coefficients{a.tag}.csv")
    mpath = os.path.join(C.RESULTS_DIR, f"logreg_metrics{a.tag}.json")
    if os.path.exists(mpath):
        with open(mpath) as f:
            m = json.load(f)
        m["intercept"] = round(intercept, 4)
        m["top_coefficients"] = [{"feature": n, "coef": round(c, 4)} for n, c in coefs[:15]]
        with open(mpath, "w") as f:
            json.dump(m, f, indent=2, default=str)
        C.log(f"  updated {mpath}")
    print("\n  Top 20 coefficients (positive = more likely to be purchased):")
    for n, c in coefs[:20]:
        print(f"    {c:+.4f}  {n}")
    print("\n  Numeric features:")
    for n, c in coefs:
        if n in job05.NUMERIC_FEATURES:
            print(f"    {c:+.4f}  {n}")
    spark.stop()


if __name__ == "__main__":
    main()
