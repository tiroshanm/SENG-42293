"""Synthetic loyalty-programme membership (seeded) for survival analysis.
Each member has a tenure until cancellation (event) or is still active at study end (right-censored)."""
import csv, math, random
random.seed(11)
STUDY = 24                                       # months of observation
rows = []
for i in range(1, 4001):
    plan = random.choices(["free", "premium"], [.65, .35])[0]
    channel = random.choices(["search", "social", "email", "direct"], [.4, .25, .15, .2])[0]
    sat = round(min(5, max(1, random.gauss(3.7, .8))), 1)          # average rating the member gives
    bookings = max(0, int(random.gauss(3 + (2 if plan == "premium" else 0), 2)))   # bookings in first year
    entry = random.uniform(0, STUDY - 1)                           # month of joining (staggered entry)
    # Weibull baseline, proportional hazards multiplier
    hr = math.exp(-0.55 * (plan == "premium") + 0.35 * (channel == "social") - 0.45 * (sat - 3.7) - 0.08 * (bookings - 4))
    u = random.random()
    shape, scale = 1.3, 18.0
    t = scale * (-math.log(u) / hr) ** (1 / shape)                 # time to cancel (months)
    obs_end = STUDY - entry
    event = 1 if t <= obs_end else 0
    rows.append((f"M{i:05d}", plan, channel, sat, bookings, round(entry, 2), round(min(t, obs_end), 2), event))
with open("member_tenure.csv", "w", newline="") as f:
    w = csv.writer(f); w.writerow(["member_id", "plan", "channel", "avg_rating_given", "bookings_year1", "join_month", "tenure_months", "cancelled"]); w.writerows(rows)
print(len(rows), "members; cancelled:", sum(r[-1] for r in rows))
