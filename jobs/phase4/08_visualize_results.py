"""
Phase 4 / Job 8 - charts for the report and the demo (Task 6).

Plain Python (NOT spark-submit): pandas + matplotlib on the SMALL files in ./results only.
All heavy computation already happened on the cluster (Phases 2-4).

    python3 jobs/phase4/08_visualize_results.py [--results ./results]

Writes 300-dpi PNGs to ./results/plots/:
  01_conversion_funnel.png        a) view -> cart -> purchase, overall and by top categories
  02_rfm_segments.png             b) RFM segments: share of buyers vs share of revenue
  03_scalability.png              c) T_p and speed-up S_p vs ideal linear, p = 1, 2, 4, 8
  04_storage_format.png           d) raw CSV vs Iceberg Parquet query latency
  05_logreg_roc_pr.png            ROC and precision-recall curves (job 05)
  06_logreg_confusion_matrix.png  confusion matrix (job 05)
  07_logreg_coefficients.png      strongest coefficients (job 05 / 05b)
  08_als_vs_popularity.png        ALS vs most-popular baseline, Precision/Recall/NDCG@10 (job 06)
  09_daily_activity.png           views and purchases per day (shows the 2019-11-15 gap)
  10_hourly_heatmap.png           purchases by weekday x hour (UTC)
  11_shuffle_network.png          local vs Wi-Fi shuffle reads and fetch wait per p (needs 07b first)
A chart whose input file is missing is skipped with a message.
"""
import argparse
import json
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib import font_manager  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

# ------------------------------------------------------------------ style (validated palette)
BLUE, ORANGE, AQUA = "#257de0", "#f47230", "#00b894"     # categorical slots 1-3
RED = "#ec4647"                                           # diverging negative pole
GRAY = "#9c9c9c"
TEXT, TEXT2, GRID = "#191919", "#666666", "#e6e6e6"
SEQ_BLUES = ["#cce3fd", "#9dc8f7", "#6aabf1", "#338ced", "#2370cd", "#1757ac", "#0e3d7e"]

FONT = "Inter" if any("Inter" in f.name for f in font_manager.fontManager.ttflist) else "DejaVu Sans"
plt.rcParams.update({
    "font.family": FONT, "font.size": 9.5,
    "text.color": TEXT, "axes.labelcolor": TEXT2, "xtick.color": TEXT2, "ytick.color": TEXT2,
    "axes.edgecolor": GRID, "axes.linewidth": 0.8,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6, "grid.linestyle": "-",
    "axes.spines.top": False, "axes.spines.right": False, "axes.axisbelow": True,
    "axes.titlesize": 11, "axes.titleweight": "semibold", "axes.titlelocation": "left",
    "axes.titlepad": 10, "legend.frameon": False, "legend.fontsize": 9,
    "figure.dpi": 100, "savefig.dpi": 300, "savefig.bbox": "tight", "savefig.facecolor": "white",
})

def _compact(v):
    for div, suf in ((1e6, "M"), (1e3, "K")):
        if abs(v) >= div * (1 if suf == "M" else 10):
            return f"{v / div:.1f}".rstrip("0").rstrip(".") + suf
    return f"{v:,.0f}"


human = FuncFormatter(lambda v, _: _compact(v))
pct = FuncFormatter(lambda v, _: f"{v:.0%}")


def fmt_n(v):
    return f"{v / 1e6:.1f}M" if v >= 1e6 else f"{v / 1e3:.0f}K" if v >= 1e4 else f"{v:,.0f}"


def title(fig, main, sub=None):
    """Title + subtitle above the plot area, spaced in inches so short figures do not overlap."""
    h = fig.get_size_inches()[1]
    fig.text(0.01, 1.0 + (0.26 if sub else 0.04) / h, main, ha="left", va="bottom",
             fontsize=13, fontweight="semibold", color=TEXT)
    if sub:
        fig.text(0.01, 1.0 + 0.04 / h, sub, ha="left", va="bottom", fontsize=9.5, color=TEXT2)


def save(fig, out, name):
    path = os.path.join(out, name)
    fig.savefig(path)
    plt.close(fig)
    print(f"  saved {path}")
    return path


def read_csv(results, name):
    path = os.path.join(results, name)
    if not os.path.exists(path):
        print(f"  skip: {path} not found")
        return None
    return pd.read_csv(path)


def read_json(results, name):
    path = os.path.join(results, name)
    if not os.path.exists(path):
        print(f"  skip: {path} not found")
        return None
    with open(path) as f:
        return json.load(f)


def hbar_labels(ax, bars, values, fmt):
    xmax = ax.get_xlim()[1]
    for b, v in zip(bars, values):
        ax.text(b.get_width() + xmax * 0.01, b.get_y() + b.get_height() / 2, fmt(v),
                va="center", ha="left", fontsize=8.5, color=TEXT)


# ------------------------------------------------------------------ a) funnel
def chart_funnel(results, out):
    df = read_csv(results, "gold_funnel_metrics.csv")
    if df is None:
        return
    total = df[df["level"] == "total"].iloc[0]
    cats = (df[df["level"] == "category"].sort_values("view_sessions", ascending=False).head(8)
            .iloc[::-1])

    fig, (a1, a2) = plt.subplots(1, 2, figsize=(12, 4.8), gridspec_kw={"width_ratios": [1, 1.35]})
    title(fig, "Conversion funnel: view → cart → purchase",
          "Sessions that contain each step (Oct–Nov 2019, 109.6 M cleaned events)")

    stages = ["View", "Cart", "Purchase"]
    vals = [total["view_sessions"], total["cart_sessions"], total["purchase_sessions"]]
    y = np.arange(3)[::-1]
    bars = a1.barh(y, vals, height=0.5, color=[SEQ_BLUES[5], SEQ_BLUES[4], SEQ_BLUES[3]])
    a1.set_yticks(y, stages)
    a1.xaxis.set_major_formatter(human)
    a1.set_xlim(0, max(vals) * 1.45)
    a1.grid(axis="y", visible=False)
    a1.set_title("All sessions")
    for b, v in zip(bars, vals):
        a1.text(b.get_width() + max(vals) * 0.015, b.get_y() + b.get_height() / 2,
                f"{fmt_n(v)}" + ("" if v == vals[0] else f"  ({v / vals[0]:.1%} of view sessions)"),
                va="center", fontsize=8.5)
    a1.text(0, -0.95, f"cart → purchase: {vals[2] / vals[1]:.1%} of cart sessions", fontsize=9, color=TEXT2,
            transform=a1.get_yaxis_transform())

    yy = np.arange(len(cats))
    h = 0.36
    b1 = a2.barh(yy + h / 2 + 0.02, cats["view_to_cart_rate"], height=h, color=BLUE, label="view → cart")
    b2 = a2.barh(yy - h / 2 - 0.02, cats["view_to_purchase_rate"], height=h, color=ORANGE,
                 label="view → purchase")
    a2.set_yticks(yy, cats["main_category"])
    a2.xaxis.set_major_formatter(pct)
    a2.set_xlim(0, max(cats["view_to_cart_rate"].max(), cats["view_to_purchase_rate"].max()) * 1.25)
    a2.grid(axis="y", visible=False)
    a2.set_title("Top 8 categories by view sessions")
    hbar_labels(a2, b1, cats["view_to_cart_rate"], lambda v: f"{v:.1%}")
    hbar_labels(a2, b2, cats["view_to_purchase_rate"], lambda v: f"{v:.1%}")
    a2.legend(loc="lower right")
    fig.tight_layout()
    return save(fig, out, "01_conversion_funnel.png")


# ------------------------------------------------------------------ b) RFM
def chart_rfm(results, out):
    df = read_csv(results, "gold_rfm_segments_summary.csv")
    if df is None:
        return
    browsers = df[df["segment"].str.startswith("Browsers")]
    seg = df[~df["segment"].str.startswith("Browsers")].sort_values("share_of_revenue")
    sub = "Share of buying customers vs share of revenue per segment"
    if len(browsers):
        b = browsers.iloc[0]
        sub += f"  ·  not shown: {fmt_n(b['customers'])} users who never bought ({b['share_of_users']:.1%} of all users)"

    fig, ax = plt.subplots(figsize=(11, 5.2))
    title(fig, "RFM customer segments", sub)
    y = np.arange(len(seg))
    h = 0.36
    b1 = ax.barh(y + h / 2 + 0.02, seg["share_of_buyers"], height=h, color=BLUE, label="share of buyers")
    b2 = ax.barh(y - h / 2 - 0.02, seg["share_of_revenue"], height=h, color=ORANGE, label="share of revenue")
    ax.set_yticks(y, seg["segment"])
    ax.xaxis.set_major_formatter(pct)
    ax.set_xlim(0, max(seg["share_of_buyers"].max(), seg["share_of_revenue"].max()) * 1.3)
    ax.grid(axis="y", visible=False)
    hbar_labels(ax, b1, seg["share_of_buyers"], lambda v: f"{v:.1%}")
    hbar_labels(ax, b2, seg["share_of_revenue"], lambda v: f"{v:.1%}")
    ax.legend(loc="lower right")
    fig.tight_layout()
    return save(fig, out, "02_rfm_segments.png")


# ------------------------------------------------------------------ c) scalability
def chart_scalability(results, out):
    df = read_csv(results, "benchmark_results.csv")
    if df is None:
        return
    s = df[(df["record_type"] == "summary") & (df["benchmark"] == "scaling")].copy()
    raw = df[(df["record_type"] == "raw") & (df["benchmark"] == "scaling")].copy()
    if s.empty:
        print("  skip: no scaling summary rows")
        return
    s["p"] = s["p"].astype(int)
    raw["p"] = raw["p"].astype(int)
    s = s.sort_values("p")
    ps = s["p"].tolist()

    fig, (a1, a2) = plt.subplots(1, 2, figsize=(11.5, 4.6))
    title(fig, "Scalability of the Spark workload on the 3-laptop cluster",
          "Filter + session aggregation (shuffle) + category revenue · median of "
          f"{int(s['run'].max())} runs per p · 1-core executors, 700 MB each")

    a1.scatter(raw["p"], raw["seconds"], s=18, color=GRAY, alpha=0.7, zorder=2, label="single run")
    a1.plot(s["p"], s["median_seconds"], color=BLUE, lw=2, marker="o", ms=7, mec="white", mew=2,
            zorder=3, label="median $T_p$")
    for p, t in zip(s["p"], s["median_seconds"]):
        a1.annotate(f"{t:.0f} s", (p, t), textcoords="offset points", xytext=(6, 6), fontsize=8.5)
    a1.set_xscale("log", base=2)
    a1.set_xticks(ps, [str(p) for p in ps])
    a1.set_xlabel("total executor cores p")
    a1.set_ylabel("execution time [s]")
    a1.set_ylim(0, raw["seconds"].max() * 1.15)
    a1.set_title("Execution time $T_p$")
    a1.legend(loc="upper right")

    a2.plot(ps, ps, color=GRAY, lw=1.5, ls="--", label="ideal linear ($S_p = p$)")
    a2.plot(s["p"], s["speedup"], color=BLUE, lw=2, marker="o", ms=7, mec="white", mew=2,
            label="measured $S_p = T_1 / T_p$")
    for p, sp, e in zip(s["p"], s["speedup"], s["efficiency"]):
        a2.annotate(f"S={sp:.2f}\nE={e:.0%}", (p, sp), textcoords="offset points", xytext=(7, -22),
                    fontsize=8.5, color=TEXT)
    a2.set_xscale("log", base=2)
    a2.set_xticks(ps, [str(p) for p in ps])
    a2.set_yticks(ps)
    a2.set_xlabel("total executor cores p")
    a2.set_ylabel("speed-up")
    a2.set_ylim(0, max(ps) * 1.1)
    a2.set_title("Speed-up $S_p$ and efficiency $E_p = S_p / p$")
    a2.legend(loc="upper left")
    fig.tight_layout()
    return save(fig, out, "03_scalability.png")


# ------------------------------------------------------------------ d) storage
def chart_storage(results, out):
    df = read_csv(results, "benchmark_results.csv")
    if df is None:
        return
    s = df[(df["record_type"] == "summary") & (df["benchmark"] == "storage")].copy()
    if s.empty:
        print("  skip: no storage summary rows")
        return
    s[["query", "format"]] = s["config"].str.split(":", expand=True)
    piv = s.pivot(index="query", columns="format", values="median_seconds")
    labels = {"q1_one_day_purchases": "Q1: purchases on one day\n(partition pruning)",
              "q2_revenue_by_event_type_full_scan": "Q2: revenue by event type, all rows\n(columnar: 2 of 9 columns)"}

    fig, ax = plt.subplots(figsize=(10, 4.8))
    title(fig, "Raw CSV vs Apache Iceberg (Parquet): query latency",
          f"Same queries, same results checked · median of {int(s['run'].max())} runs · "
          "CSV = 14.7 GB of text in HDFS, Iceberg = bronze_events")
    x = np.arange(len(piv))
    w = 0.32
    bc = ax.bar(x - w / 2 - 0.01, piv["csv"], width=w, color=ORANGE, label="raw CSV")
    bi = ax.bar(x + w / 2 + 0.01, piv["iceberg"], width=w, color=BLUE, label="Iceberg (Parquet)")
    ax.set_xticks(x, [labels.get(q, q) for q in piv.index])
    ax.set_ylabel("query time [s]")
    ax.set_ylim(0, piv.values.max() * 1.25)
    ax.grid(axis="x", visible=False)
    for bars in (bc, bi):
        for b in bars:
            ax.text(b.get_x() + b.get_width() / 2, b.get_height() + piv.values.max() * 0.015,
                    f"{b.get_height():.1f} s", ha="center", va="bottom", fontsize=8.5)
    for i, q in enumerate(piv.index):
        ratio = piv.loc[q, "csv"] / piv.loc[q, "iceberg"]
        ax.text(x[i], piv.loc[q, "csv"] + piv.values.max() * 0.11, f"Iceberg {ratio:.1f}× faster",
                ha="center", fontsize=9.5, fontweight="semibold")
    ax.legend(loc="upper right")
    fig.tight_layout()
    return save(fig, out, "04_storage_format.png")


# ------------------------------------------------------------------ ML charts
def chart_roc_pr(results, out):
    cp = read_csv(results, "logreg_curve_points.csv")
    m = read_json(results, "logreg_metrics.json")
    if cp is None or m is None:
        return
    cp = cp.sort_values("fpr")
    roc = pd.concat([pd.DataFrame({"fpr": [0.0], "tpr_recall": [0.0]}), cp[["fpr", "tpr_recall"]],
                     pd.DataFrame({"fpr": [1.0], "tpr_recall": [1.0]})])
    pr = cp.dropna(subset=["precision"]).sort_values("tpr_recall")

    fig, (a1, a2) = plt.subplots(1, 2, figsize=(10.5, 4.6))
    title(fig, "Purchase prediction: weighted logistic regression (Spark MLlib)",
          f"Test set: {m['dataset']['test_rows']:,} carts by unseen users · "
          f"{m['dataset']['test_positive_rate']:.1%} of carts end in a purchase")
    a1.plot([0, 1], [0, 1], color=GRAY, lw=1.2, ls="--", label="random (AUC 0.50)")
    a1.plot(roc["fpr"], roc["tpr_recall"], color=BLUE, lw=2, label=f"model (AUC {m['roc_auc']:.3f})")
    a1.set_xlabel("false positive rate")
    a1.set_ylabel("true positive rate (recall)")
    a1.set_xlim(0, 1), a1.set_ylim(0, 1.02)
    a1.set_title("ROC curve")
    a1.legend(loc="lower right")

    base = m.get("pr_auc_random_baseline", m["dataset"]["test_positive_rate"])
    a2.axhline(base, color=GRAY, lw=1.2, ls="--", label=f"random (PR-AUC {base:.2f})")
    a2.plot(pr["tpr_recall"], pr["precision"], color=BLUE, lw=2, label=f"model (PR-AUC {m['pr_auc']:.3f})")
    a2.set_xlabel("recall")
    a2.set_ylabel("precision")
    a2.set_xlim(0, 1), a2.set_ylim(0, 1.02)
    a2.set_title("Precision-recall curve")
    a2.legend(loc="upper right")
    fig.tight_layout()
    return save(fig, out, "05_logreg_roc_pr.png")


def chart_confusion(results, out):
    m = read_json(results, "logreg_metrics.json")
    if m is None:
        return
    cm = m["confusion_matrix"]
    mat = np.array([[cm["tn"], cm["fp"]], [cm["fn"], cm["tp"]]], dtype=float)
    row = mat / mat.sum(axis=1, keepdims=True)
    from matplotlib.colors import LinearSegmentedColormap
    cmap = LinearSegmentedColormap.from_list("blues", SEQ_BLUES)

    fig, ax = plt.subplots(figsize=(5.6, 4.8))
    title(fig, "Confusion matrix (threshold 0.5)",
          f"Accuracy {m['accuracy']:.1%} · precision {m['precision']:.1%} · recall {m['recall']:.1%} · F1 {m['f1']:.3f}")
    ax.imshow(row, cmap=cmap, vmin=0, vmax=1)
    ax.grid(False)
    names = ["not purchased", "purchased"]
    ax.set_xticks([0, 1], names)
    ax.set_yticks([0, 1], names)
    ax.set_xlabel("predicted")
    ax.set_ylabel("actual")
    for i in range(2):
        for j in range(2):
            ax.text(j, i, f"{int(mat[i, j]):,}\n{row[i, j]:.1%} of row", ha="center", va="center",
                    fontsize=10, color="white" if row[i, j] > 0.55 else TEXT)
    for s in ax.spines.values():
        s.set_visible(False)
    fig.tight_layout()
    return save(fig, out, "06_logreg_confusion_matrix.png")


def chart_coefficients(results, out, n=16):
    df = read_csv(results, "logreg_coefficients.csv")
    if df is None:
        return
    if df["feature"].str.match(r"^f\d+$").all():
        print("  note: coefficients have generic names - run ./scripts/12_run_phase3.sh 5b first")
    d = df.reindex(df["coefficient"].abs().sort_values(ascending=False).index).head(n).iloc[::-1]
    fig, ax = plt.subplots(figsize=(9.5, 5.6))
    title(fig, f"What drives a cart to a purchase: {n} strongest coefficients",
          "Positive = more likely to be bought · brand/category = one-hot indicator · numeric features on their own scale")
    colors = [BLUE if c > 0 else RED for c in d["coefficient"]]
    bars = ax.barh(np.arange(len(d)), d["coefficient"], height=0.55, color=colors)
    ax.set_yticks(np.arange(len(d)), d["feature"])
    ax.axvline(0, color=TEXT2, lw=0.8)
    ax.grid(axis="y", visible=False)
    lim = d["coefficient"].abs().max() * 1.25
    ax.set_xlim(min(d["coefficient"].min() * 1.25, -0.25 * lim), max(d["coefficient"].max() * 1.4, 0.25 * lim))
    for b, v in zip(bars, d["coefficient"]):
        ax.text(v + (lim * 0.02 if v > 0 else -lim * 0.02), b.get_y() + b.get_height() / 2, f"{v:+.2f}",
                va="center", ha="left" if v > 0 else "right", fontsize=8.5)
    fig.tight_layout()
    return save(fig, out, "07_logreg_coefficients.png")


def chart_als(results, out):
    m = read_json(results, "als_metrics.json")
    if m is None:
        return
    k = 10
    names = [f"Precision@{k}", f"Recall@{k}", f"NDCG@{k}"]
    keys = [f"precision_at_{k}", f"recall_at_{k}", f"ndcg_at_{k}"]
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.4), sharey=False)
    d = m["dataset"]["train"]
    title(fig, "Recommender: ALS (implicit feedback) vs most-popular baseline",
          f"Top-10 unseen products for {m['als']['all_interactions']['users_evaluated']:,} evaluation users · "
          f"trained on {d['pairs'] / 1e6:.1f} M user-product pairs ({d['users']:,} users, {d['items']:,} products)")
    for ax, scope, label in ((axes[0], "all_interactions", "Held-out interactions (any)"),
                             (axes[1], "carts_and_purchases", "Held-out carts and purchases")):
        als = [m["als"][scope][kk] for kk in keys]
        pop = [m["popularity_baseline"][scope][kk] for kk in keys]
        x = np.arange(3)
        w = 0.32
        b1 = ax.bar(x - w / 2 - 0.01, als, width=w, color=BLUE, label="ALS")
        b2 = ax.bar(x + w / 2 + 0.01, pop, width=w, color=ORANGE, label="most popular")
        ax.set_xticks(x, names)
        top = max(als + pop)
        ax.set_ylim(0, top * 1.3)
        ax.grid(axis="x", visible=False)
        for b in list(b1) + list(b2):
            ax.text(b.get_x() + b.get_width() / 2, b.get_height() + top * 0.02, f"{b.get_height():.3f}",
                    ha="center", fontsize=8.5)
        for i in range(3):
            if pop[i]:
                ax.text(x[i], max(als[i], pop[i]) + top * 0.12, f"{als[i] / pop[i]:.1f}×",
                        ha="center", fontsize=9.5, fontweight="semibold")
        ax.set_title(label)
        ax.legend(loc="upper right")
    fig.tight_layout()
    return save(fig, out, "08_als_vs_popularity.png")


# ------------------------------------------------------------------ activity
def chart_daily(results, out):
    df = read_csv(results, "gold_hourly_trends.csv")
    if df is None:
        return
    daily = df.groupby("event_date", as_index=False)[["views", "carts", "purchases"]].sum()
    daily["event_date"] = pd.to_datetime(daily["event_date"])
    gaps = daily[(daily["purchases"] == 0) & (daily["carts"] > 0)]

    fig, (a1, a2) = plt.subplots(2, 1, figsize=(11.5, 5.8), sharex=True)
    title(fig, "Daily activity, October–November 2019",
          "Two measures on separate panels (different scales) · UTC days")
    a1.plot(daily["event_date"], daily["views"], color=BLUE, lw=2)
    a1.fill_between(daily["event_date"], daily["views"], color=BLUE, alpha=0.1, lw=0)
    a1.yaxis.set_major_formatter(human)
    a1.set_ylim(0, daily["views"].max() * 1.15)
    a1.set_title("Views per day")
    a2.plot(daily["event_date"], daily["purchases"], color=ORANGE, lw=2)
    a2.fill_between(daily["event_date"], daily["purchases"], color=ORANGE, alpha=0.1, lw=0)
    a2.yaxis.set_major_formatter(human)
    a2.set_ylim(0, daily["purchases"].max() * 1.25)
    a2.set_title("Purchases per day")
    for _, g in gaps.iterrows():
        for ax in (a1, a2):
            ax.axvspan(g["event_date"] - pd.Timedelta(hours=12), g["event_date"] + pd.Timedelta(hours=12),
                       color=GRAY, alpha=0.18, lw=0)
        a2.annotate(f"{g['event_date']:%d %b}: 0 purchases\n(missing in source data)",
                    (g["event_date"], 0), textcoords="offset points", xytext=(8, 28), fontsize=8.5,
                    color=TEXT, arrowprops={"arrowstyle": "-", "color": TEXT2, "lw": 0.8})
    peak = daily.loc[daily["views"].idxmax()]
    a1.annotate(f"peak {fmt_n(peak['views'])} views ({peak['event_date']:%d %b})",
                (peak["event_date"], peak["views"]), textcoords="offset points", xytext=(-150, -4),
                fontsize=8.5, color=TEXT)
    fig.tight_layout()
    return save(fig, out, "09_daily_activity.png")


def chart_heatmap(results, out):
    df = read_csv(results, "gold_hourly_trends.csv")
    if df is None:
        return
    daily = df.groupby("event_date")["purchases"].sum()
    ok_days = daily[daily > 0].index
    d = df[df["event_date"].isin(ok_days)]
    order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    piv = (d.groupby(["day_of_week", "event_hour"])["purchases"].mean().unstack("event_hour")
           .reindex(order))
    from matplotlib.colors import LinearSegmentedColormap
    cmap = LinearSegmentedColormap.from_list("blues", SEQ_BLUES)

    fig, ax = plt.subplots(figsize=(12, 3.9))
    title(fig, "When do people buy? Average purchases per hour",
          "Weekday × hour of day (UTC) · days without purchase data excluded")
    im = ax.imshow(piv.values, aspect="auto", cmap=cmap)
    ax.grid(False)
    ax.set_yticks(range(7), order)
    ax.set_xticks(range(0, 24, 2), [f"{h:02d}" for h in range(0, 24, 2)])
    ax.set_xlabel("hour of day (UTC)")
    for s in ax.spines.values():
        s.set_visible(False)
    cb = fig.colorbar(im, ax=ax, pad=0.01, fraction=0.025)
    cb.outline.set_visible(False)
    cb.ax.tick_params(colors=TEXT2, labelsize=8)
    cb.set_label("purchases / hour", color=TEXT2)
    r, c = np.unravel_index(np.nanargmax(piv.values), piv.shape)
    ax.text(c, r, f"{piv.values[r, c]:,.0f}", ha="center", va="center", fontsize=8, color="white")
    fig.tight_layout()
    return save(fig, out, "10_hourly_heatmap.png")


# ------------------------------------------------------------------ why: shuffle over Wi-Fi
def chart_shuffle(results, out):
    df = read_csv(results, "benchmark_shuffle.csv")
    if df is None:
        print("    (run: python3 jobs/phase4/07b_shuffle_analysis.py)")
        return
    s = df[df["record_type"] == "summary"].sort_values("p")
    if s.empty:
        return
    x = np.arange(len(s))
    labels = [f"p = {int(p)}" for p in s["p"]]

    fig, (a1, a2) = plt.subplots(1, 2, figsize=(11.5, 4.5))
    title(fig, "Why the speed-up flattens: shuffle data crossing Wi-Fi",
          "Scalability runs, median per p · from the Spark History Server (07b_shuffle_analysis.py)")
    loc, rem = s["shuffle_read_local_mb"].to_numpy(), s["shuffle_read_remote_mb"].to_numpy()
    a1.bar(x, loc, width=0.5, color=BLUE, label="read on the same laptop")
    a1.bar(x, rem, width=0.5, bottom=loc, color=ORANGE, label="read over Wi-Fi from another laptop",
           edgecolor="white", linewidth=1.5)
    top = (loc + rem).max()
    for i, (l, r) in enumerate(zip(loc, rem)):
        share = r / (l + r) if l + r else 0
        a1.text(x[i], l + r + top * 0.02, f"{share:.0%} over Wi-Fi", ha="center", fontsize=8.5)
    a1.set_xticks(x, labels)
    a1.set_ylabel("shuffle data read [MB]")
    a1.set_ylim(0, top * 1.55)
    a1.grid(axis="x", visible=False)
    a1.set_title("Where shuffle blocks were read from")
    a1.legend(loc="upper left")

    w = s["fetch_wait_s"].to_numpy()
    bars = a2.bar(x, w, width=0.5, color=ORANGE)
    for b, v in zip(bars, w):
        a2.text(b.get_x() + b.get_width() / 2, v + max(w.max(), 1) * 0.02, f"{v:.0f} s", ha="center", fontsize=8.5)
    a2.set_xticks(x, labels)
    a2.set_ylabel("seconds (summed over tasks)")
    a2.set_ylim(0, max(w.max(), 1) * 1.25)
    a2.grid(axis="x", visible=False)
    a2.set_title("Time tasks spent waiting for remote shuffle blocks")
    fig.tight_layout()
    return save(fig, out, "11_shuffle_network.png")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default="./results")
    a = ap.parse_args()
    out = os.path.join(a.results, "plots")
    os.makedirs(out, exist_ok=True)
    print(f"Charts from {a.results} -> {out} (font: {FONT})")
    made = [f(a.results, out) for f in (chart_funnel, chart_rfm, chart_scalability, chart_storage,
                                        chart_roc_pr, chart_confusion, chart_coefficients, chart_als,
                                        chart_daily, chart_heatmap, chart_shuffle)]
    print(f"done: {sum(1 for m in made if m)} charts")


if __name__ == "__main__":
    main()
