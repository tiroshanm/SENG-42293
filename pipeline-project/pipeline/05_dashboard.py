"""STEP 5 - SERVE/VISUALISE: CSV outputs -> PNG charts (runs on the host; needs pandas + matplotlib)."""
import json, os
import pandas as pd, matplotlib
matplotlib.use("Agg"); import matplotlib.pyplot as plt
OUT = os.path.join(os.path.dirname(__file__), "..", "output"); C = "#8b7355"; G = "#c4b5a0"
def save(name): plt.tight_layout(); plt.savefig(f"{OUT}/{name}", dpi=130); plt.close()

dq = {**json.load(open(f"{OUT}/dq_ingest.json")), **json.load(open(f"{OUT}/dq_clean.json"))}
steps = [("Flattened\nreviews", dq["reviews_flattened"]), ("After\ndedup", dq["after_dedup"]),
         ("Valid\nrating", dq["after_dedup"] - dq["rating_missing_or_invalid"])]
fig, ax = plt.subplots(figsize=(6.5, 3.6)); ax.bar([s[0] for s in steps], [s[1] for s in steps], color=[G, G, C])
for i, s in enumerate(steps): ax.text(i, s[1], f"{s[1]:,}", ha="center", va="bottom", fontsize=9)
ax.set_title("Data quality funnel"); ax.spines[["top", "right"]].set_visible(False); save("chart_dq_funnel.png")

c = pd.read_csv(f"{OUT}/city_summary.csv").sort_values("avg_rating")
fig, ax = plt.subplots(figsize=(6.5, 3.6)); ax.barh(c.city, c.avg_rating, color=C)
ax.set_xlim(3, 4.5); ax.set_title("Average rating by city"); ax.spines[["top", "right"]].set_visible(False); save("chart_city_rating.png")

m = pd.read_csv(f"{OUT}/monthly_volume.csv").iloc[:-1]   # drop the partial final month
fig, ax = plt.subplots(figsize=(6.5, 3.6)); ax.plot(m.month, m.n_reviews, color=C)
ax.set_xticks(m.month[::6]); ax.tick_params(axis="x", rotation=45); ax.set_title("Reviews per month")
ax.spines[["top", "right"]].set_visible(False); save("chart_monthly_volume.png")

d = pd.read_csv(f"{OUT}/rating_dist.csv")
fig, ax = plt.subplots(figsize=(6.5, 3.6)); ax.bar(d.rating.astype(str), d["count"], color=G)
ax.set_title("Rating distribution"); ax.spines[["top", "right"]].set_visible(False); save("chart_rating_dist.png")

s = pd.read_csv(f"{OUT}/sentiment_vs_rating.csv").set_index("label").reindex(["negative", "neutral", "positive"])
fig, ax = plt.subplots(figsize=(6.5, 3.6)); ax.bar(s.index, s.avg_rating, color=[G, G, C])
ax.set_title("Mean rating by lexicon sentiment label"); ax.set_ylim(1, 5); ax.spines[["top", "right"]].set_visible(False); save("chart_sentiment.png")
print("charts written")
