"""Synthetic traveller follower network (seeded): 4 interest communities with a few hubs."""
import csv, random
random.seed(21)
INT = ["hill-country", "beach", "culture", "budget"]
N = 600
users = [(f"T{i:03d}", INT[i % 4]) for i in range(N)]
by = {k: [u for u, c in users if c == k] for k in INT}
weight = {u: 1 for u, _ in users}
hubs = random.sample([u for u, _ in users], 12)
for h in hubs: weight[h] = 25
edges = set()
for u, c in users:
    for _ in range(random.randint(3, 9)):
        pool = by[c] if random.random() < .85 else [x for x, _ in users]
        v = random.choices(pool, [weight[x] for x in pool])[0]
        if v != u: edges.add((u, v))
with open("follows.csv", "w", newline="") as f:
    w = csv.writer(f); w.writerow(["follower", "followed"]); w.writerows(sorted(edges))
with open("traveller_profiles.csv", "w", newline="") as f:
    w = csv.writer(f); w.writerow(["user_id", "interest"]); w.writerows(users)
print(len(edges), "edges;", len(hubs), "hubs")
