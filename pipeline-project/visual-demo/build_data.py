"""Bundle pipeline outputs + raw-data profile into data.js for the visual demo.
Run after ./run_all.sh:   python visual-demo/build_data.py
Pure Python (no Spark/pandas) so it also works on a laptop that only has the repo.

The step explorer needs before/after rows for one real hotel, so this script also replays the
silver and gold logic in plain Python and cross-checks the totals against output/ before writing."""
import csv, json, os, re, sys
from collections import Counter, defaultdict
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
OUT, RAW = f"{ROOT}/output", f"{ROOT}/data/raw"

def rows(name):
    with open(f"{OUT}/{name}") as f:
        return list(csv.DictReader(f))

def num(v):
    try: return float(v) if "." in v else int(v)
    except (TypeError, ValueError): return v

def issue_of(r):
    v = r.get("rating", "<missing>")
    return ("missing_key" if v == "<missing>" else "null" if v is None else
            "na_text" if v == "N/A" else "string_number" if isinstance(v, str) else
            "out_of_range" if not 1 <= v <= 5 else None)

# --- raw profile: what the messy source actually looks like before Spark touches it
files, issues, cities, stars, tags = [], Counter(), Counter(), Counter(), Counter()
samples, corrupt, n_reviews, hotels = {}, [], 0, []
for fn in sorted(os.listdir(RAW)):
    if not fn.endswith(".jsonl"): continue
    good = bad = 0
    for line in open(f"{RAW}/{fn}"):
        try: h = json.loads(line)
        except json.JSONDecodeError:
            bad += 1; corrupt.append({"file": fn, "chars": len(line.strip()), "head": line[:90], "tail": line.strip()[-160:]}); continue
        good += 1; hotels.append(h); cities[h["location"]["city"]] += 1; stars[h["stars"]] += 1
        seen = set()
        for r in h["reviews"]:
            n_reviews += 1
            if r["review_id"] in seen: issues["duplicate"] += 1; samples.setdefault("duplicate", {**r, "hotel": h["name"]})
            seen.add(r["review_id"])
            for t in r.get("tags", []): tags[t] += 1
            kind = issue_of(r)
            if kind: issues[kind] += 1; samples.setdefault(kind, {**r, "hotel": h["name"]})
            if "/" in r["date"]: issues["date_ddmmyyyy"] += 1; samples.setdefault("date_ddmmyyyy", {**r, "hotel": h["name"]})
    files.append({"file": fn, "lines": good + bad, "good": good, "bad": bad,
                  "kb": round(os.path.getsize(f"{RAW}/{fn}") / 1024)})

# --- plain-Python replay of 02_clean_flatten + 03_features (same rules as the Spark code)
POS = {"great", "friendly", "clean", "excellent", "peaceful", "comfortable", "amazing", "lovely", "good", "recommended"}
NEG = {"noisy", "dirty", "rude", "slow", "overpriced", "uncomfortable", "poor", "cold", "disappointing", "not"}

def as_spark_string(v):                       # JSON value read into a StringType column
    if v is None: return None
    if isinstance(v, str): return v
    return repr(float(v)) if isinstance(v, float) else str(v)

def cast_double(s):
    try: return float(s) if s is not None else None
    except ValueError: return None

def parse_date(s):
    for f in ("%Y-%m-%d", "%d/%m/%Y"):
        try: return datetime.strptime(s, f).date()
        except (TypeError, ValueError): pass
    return None

def features(text):
    words = re.split(r"\s+", re.sub(r"[^a-z\s]", "", text.lower()))
    p, n = sum(w in POS for w in words), sum(w in NEG for w in words)
    s = (p - n) / max(len(words), 1)
    return {"n_words": len(words), "pos_hits": p, "neg_hits": n, "sentiment": s,
            "label": "positive" if s > 0 else "negative" if s < 0 else "neutral"}

flat, seen, silver = [], set(), []
for h in hotels:
    for r in h["reviews"]:
        flat.append((h, r))
for h, r in flat:
    if r["review_id"] in seen: continue
    seen.add(r["review_id"])
    v = cast_double(as_spark_string(r.get("rating")))
    d = parse_date(r["date"])
    row = {"hotel_id": h["hotel_id"], "hotel_name": h["name"], "stars": h["stars"], "city": h["location"]["city"],
           "review_id": r["review_id"], "rating": v if v is not None and 1 <= v <= 5 else None, "review_date": d,
           "text": r["text"].strip()}
    row.update(features(row["text"])); silver.append(row)
rated = [r for r in silver if r["rating"] is not None]

checks = {
    "reviews_flattened": (len(flat), json.load(open(f"{OUT}/dq_clean.json"))["reviews_flattened"]),
    "after_dedup": (len(silver), json.load(open(f"{OUT}/dq_clean.json"))["after_dedup"]),
    "rating_missing_or_invalid": (len(silver) - len(rated), json.load(open(f"{OUT}/dq_clean.json"))["rating_missing_or_invalid"]),
    "n_rated_reviews": (len(rated), json.load(open(f"{OUT}/summary.json"))["n_rated_reviews"]),
}
for r in rows("sentiment_vs_rating.csv"):
    checks[f"label_{r['label']}"] = (sum(x["label"] == r["label"] for x in rated), int(r["n"]))
bad_checks = {k: v for k, v in checks.items() if v[0] != v[1]}
if bad_checks:
    sys.exit(f"python replay disagrees with output/: {bad_checks}")

def hotel_stats(hid):
    rs = [r for r in rated if r["hotel_id"] == hid]
    return {"n_reviews": len(rs), "avg_rating": round(sum(r["rating"] for r in rs) / len(rs), 3),
            "avg_sentiment": round(sum(r["sentiment"] for r in rs) / len(rs), 4),
            "pct_positive": round(sum(r["label"] == "positive" for r in rs) / len(rs), 3),
            "last_review": max(r["review_date"] for r in rs).isoformat()}
for t in rows("top_hotels.csv"):                 # aggregate replay must match the gold table too
    mine, theirs = hotel_stats(t["hotel_id"]), t
    if mine["n_reviews"] != int(theirs["n_reviews"]) or abs(mine["avg_rating"] - float(theirs["avg_rating"])) > .0015:
        sys.exit(f"hotel_stats replay mismatch for {t['hotel_id']}: {mine} vs {dict(theirs)}")

# --- demo hotel for the step explorer: the one whose reviews show the most kinds of problems
def kinds(h):
    k, ids = set(), set()
    for r in h["reviews"]:
        if r["review_id"] in ids: k.add("duplicate")
        ids.add(r["review_id"])
        if issue_of(r): k.add(issue_of(r))
        if "/" in r["date"]: k.add("date_ddmmyyyy")
    return k
demo = max(hotels, key=lambda h: (len(kinds(h)), -int(h["hotel_id"][2:])))
picked, got, ids = [], set(), set()
for i, r in enumerate(demo["reviews"]):        # one review per problem kind, plus clean ones, in file order
    k = "duplicate" if r["review_id"] in ids else issue_of(r) or ("date_ddmmyyyy" if "/" in r["date"] else None)
    ids.add(r["review_id"])
    if k and k not in got: got.add(k); picked.append(i)
    if k == "duplicate":
        first = next(j for j, x in enumerate(demo["reviews"]) if x["review_id"] == r["review_id"])
        if first not in picked: picked.append(first)
for i, r in enumerate(demo["reviews"]):
    if len(picked) >= 9: break
    if i not in picked and not issue_of(r) and "/" not in r["date"]: picked.append(i)
picked.sort()
demo_city = demo["location"]["city"]
city_month = defaultdict(list)
for r in rated:
    if r["city"] == demo_city: city_month[r["review_date"].strftime("%Y-%m")].append(r["rating"])
silver_by_city = Counter(r["city"] for r in silver)

# --- weekly lab results (course repo layout: <repo>/notebooks next to <repo>/pipeline-project); optional
NB = os.path.join(os.path.dirname(ROOT), "notebooks")
course = {"weeks": {}, "figures": []}
if os.path.isdir(f"{NB}/results"):
    for fn in sorted(os.listdir(f"{NB}/results")):
        if fn.endswith(".json"): course["weeks"][fn[:-5]] = json.load(open(f"{NB}/results/{fn}"))
if os.path.isdir(f"{NB}/figures"):
    course["figures"] = sorted(f for f in os.listdir(f"{NB}/figures") if f.endswith(".png"))

def read(p):
    with open(f"{ROOT}/{p}") as f: return f.read()

data = {
    "dq_ingest": json.load(open(f"{OUT}/dq_ingest.json")),
    "dq_clean": json.load(open(f"{OUT}/dq_clean.json")),
    "summary": json.load(open(f"{OUT}/summary.json")),
    "city_summary": [{k: num(v) for k, v in r.items()} for r in rows("city_summary.csv")],
    "monthly_volume": [{k: num(v) for k, v in r.items()} for r in rows("monthly_volume.csv")],
    "rating_dist": [{k: num(v) for k, v in r.items()} for r in rows("rating_dist.csv")],
    "sentiment": [{k: num(v) for k, v in r.items()} for r in rows("sentiment_vs_rating.csv")],
    "top_hotels": [{k: num(v) for k, v in r.items()} for r in rows("top_hotels.csv")],
    "raw": {"files": files, "issues": dict(issues), "hotels_by_city": dict(cities),
            "stars": dict(sorted(stars.items())), "tags": dict(tags.most_common()),
            "reviews_incl_dups": n_reviews, "samples": samples,
            "corrupt_sample": corrupt[0]["tail"] if corrupt else None, "corrupt": corrupt},
    "replay": {
        "checks": {k: v[0] for k, v in checks.items()},
        "silver_by_city": dict(sorted(silver_by_city.items())),
        "n_hotel_stats": len({r["hotel_id"] for r in rated}),
        "n_city_month": len({(r["city"], r["review_date"].strftime("%Y-%m")) for r in rated}),
        "demo": {"hotel": {k: v for k, v in demo.items() if k != "reviews"}, "n_reviews": len(demo["reviews"]),
                 "reviews": [{**demo["reviews"][i], "_pos": i} for i in picked],
                 "line": json.dumps(demo)[:220], "kinds": sorted(kinds(demo)),
                 "hotel_stats": hotel_stats(demo["hotel_id"]),
                 "city_month": [{"city": demo_city, "month": m, "n_reviews": len(v), "avg_rating": round(sum(v) / len(v), 3)}
                                for m, v in sorted(city_month.items())]},
    },
    "course": course,
    "scripts": {n: read(f"pipeline/{n}") for n in
                ["01_ingest.py", "02_clean_flatten.py", "03_features.py", "04_analytics.py", "05_dashboard.py", "common.py"]},
}
with open(f"{HERE}/data.js", "w") as f:
    f.write("// generated by build_data.py - do not edit by hand\nwindow.PIPELINE_DATA = ")
    json.dump(data, f, indent=1, default=str); f.write(";\n")
print("replay matches output/:", ", ".join(f"{k}={v[0]}" for k, v in checks.items()))
print("demo hotel:", demo["hotel_id"], demo["name"], sorted(kinds(demo)))
print("weekly lab results:", ", ".join(course["weeks"]) or "none found (notebooks/ not next to pipeline-project/)")
print("wrote", f"{HERE}/data.js")
