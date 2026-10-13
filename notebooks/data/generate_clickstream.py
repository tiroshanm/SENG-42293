"""Synthetic web clickstream for the travel-review site (seeded).
Users browse home -> search -> hotel page -> (reviews tab) -> booking -> payment -> confirmation.
Half the users see the new 'Book now' button (variant B), which genuinely lifts booking starts."""
import csv, random
from datetime import datetime, timedelta
random.seed(7)
hotels = [f"H-{1000+i}" for i in range(300)]
rows = []; start = datetime(2025, 1, 1)
for u in range(1, 6001):
    uid = f"U{u:05d}"; variant = "B" if u % 2 == 0 else "A"
    device = random.choices(["mobile", "desktop", "tablet"], [.55, .38, .07])[0]
    channel = random.choices(["search", "social", "email", "direct"], [.4, .25, .15, .2])[0]
    t0 = start + timedelta(days=random.randint(0, 89), hours=random.randint(7, 23), minutes=random.randint(0, 59))
    for visit in range(random.choice([1, 1, 1, 2, 2, 3])):
        t = t0 + timedelta(days=visit * random.randint(2, 20))
        def ev(page, hid=""):
            global t
            rows.append((uid, t.strftime("%Y-%m-%d %H:%M:%S"), page, hid, device, channel, variant))
            t += timedelta(seconds=random.randint(8, 150))
        ev("home")
        if random.random() > .85: continue                        # bounce
        ev("search")
        for _ in range(random.choice([1, 1, 2, 3])):
            h = random.choice(hotels); ev("hotel_page", h)
            if random.random() < .5: ev("reviews_tab", h)
            p_book = {"A": .16, "B": .21}[variant] * (1.15 if device == "desktop" else .9)
            if random.random() < p_book:
                ev("booking_start", h)
                if random.random() < .62:
                    ev("payment", h)
                    if random.random() < .8: ev("confirmation", h)
                break
with open("clickstream.csv", "w", newline="") as f:
    w = csv.writer(f); w.writerow(["user_id", "timestamp", "page", "hotel_id", "device", "channel", "variant"]); w.writerows(rows)
print(len(rows), "events")
