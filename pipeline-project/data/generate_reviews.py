"""Generate a synthetic, deliberately messy travel-review dataset (nested JSON lines).
Run:  python data/generate_reviews.py   ->  data/raw/hotels_00.jsonl ... hotels_04.jsonl
Seeded, so every student gets identical files."""
import json, random, os
from datetime import date, timedelta
random.seed(42)
OUT = os.path.join(os.path.dirname(__file__), "raw"); os.makedirs(OUT, exist_ok=True)
CITIES = {"Kandy": (7.29, 80.63), "Colombo": (6.93, 79.85), "Galle": (6.03, 80.22),
          "Ella": (6.87, 81.05), "Sigiriya": (7.96, 80.76), "Nuwara Eliya": (6.97, 80.78)}
CITY_BIAS = {"Kandy": .1, "Colombo": -.15, "Galle": .2, "Ella": .25, "Sigiriya": 0, "Nuwara Eliya": .05}
POS = ["great view", "friendly staff", "very clean", "excellent breakfast", "peaceful location",
       "comfortable bed", "amazing service", "lovely garden", "good value", "highly recommended"]
NEG = ["noisy at night", "dirty bathroom", "rude staff", "slow service", "overpriced",
       "uncomfortable bed", "poor wifi", "cold shower", "disappointing breakfast", "would not return"]
NEU = ["average stay", "close to the station", "small room", "ok location", "standard hotel"]
TAGS = ["family", "couple", "solo", "business", "scenic", "budget", "luxury"]
START = date(2023, 1, 1)

def make_text(r):
    k = random.randint(2, 4)
    pool = POS if r >= 4 else NEG if r <= 2 else NEU + random.sample(POS, 2) + random.sample(NEG, 2)
    return (", ".join(random.sample(pool, min(k, len(pool)))) + ".").capitalize()

def make_review(hid, n, quality, bias, city):
    r = min(5, max(1, round(random.gauss(3.6 + quality + bias, 1.0) * 2) / 2))
    d = START + timedelta(days=random.randint(0, 900))
    rev = {"review_id": f"{hid}-R{n:03d}", "user": f"u{random.randint(1, 4000)}", "rating": r,
           "text": make_text(r), "date": d.isoformat()}
    if random.random() < .6: rev["tags"] = random.sample(TAGS, random.randint(1, 3))
    if random.random() < .5: rev["helpful_votes"] = random.randint(0, 25)
    x = random.random()                                   # inject quality problems
    if x < .03: rev["rating"] = str(r)                    # type drift: "4.5"
    elif x < .05: rev["rating"] = "N/A"                   # unusable text value
    elif x < .06: rev["rating"] = random.choice([0, 11, -1])   # out of range
    elif x < .10: rev["rating"] = None                    # explicit null
    elif x < .15: del rev["rating"]                       # missing key
    if random.random() < .03:                             # inconsistent date format
        rev["date"] = d.strftime("%d/%m/%Y")
    return rev

hotels = []
for i in range(300):
    city = random.choice(list(CITIES)); hid = f"H-{1000+i}"
    lat, lon = CITIES[city]; q = random.gauss(0, .35)
    revs = [make_review(hid, n, q, CITY_BIAS[city], city) for n in range(random.randint(30, 90))]
    revs += [dict(random.choice(revs)) for _ in range(int(len(revs) * .02))]   # exact duplicates
    hotels.append({"hotel_id": hid, "name": f"{random.choice(['Lakeview','Hilltop','Palm','Royal','Sunrise','Misty'])} {random.choice(['Inn','Resort','Villa','Lodge','Residency'])} {i}",
                   "location": {"city": city, "lat": round(lat + random.uniform(-.05, .05), 4), "lon": round(lon + random.uniform(-.05, .05), 4)},
                   "stars": random.choice([2, 3, 3, 4, 4, 5]), "reviews": revs})
files = [open(f"{OUT}/hotels_{k:02d}.jsonl", "w") for k in range(5)]
for i, h in enumerate(hotels):
    line = json.dumps(h)
    if random.random() < .01: line = line[: len(line) // 2]          # corrupt (truncated) line
    files[i % 5].write(line + "\n")
for f in files: f.close()
print("hotels:", len(hotels), "reviews (incl. dups):", sum(len(h["reviews"]) for h in hotels))
