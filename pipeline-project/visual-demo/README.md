# ReviewFlow control room (visual demo)

An interactive, isometric "control room" for the travel-review pipeline, drawn as a glass pipe. Data at rest
sits in tanks on the pipe (raw files → bronze → silver → gold → output/ → display, plus a quarantine bin), and each
script is a dark processing unit between two tanks (dark = code you run, as in the Pipeline Guide). Data blocks
flow through the pipe and change shape as they pass a unit: grey raw lines (red ones branch off to quarantine),
bronze hotel blocks, many small silver review rows after `explode`, gold rows with a sentiment dot, a few blue
summary tables, and green charts heading for the display.

All numbers come from the real pipeline outputs. The **Run pipeline** button replays the five steps as an
animation; it does not start Spark.

## Open it
```bash
open visual-demo/index.html          # or double-click it - no server or build step needed
```
- `index.html#run` starts the replay automatically (useful on a projector)
- `index.html#insights` opens straight onto the charts
- `index.html#explore2` opens the step explorer on step 2; `#explore2.4` jumps to its 4th operation

## Refresh the data after a pipeline run
```bash
./run_all.sh                         # runs steps 1-5, then rebuilds visual-demo/data.js
python visual-demo/build_data.py     # or rebuild data.js on its own
```
`build_data.py` is plain Python (no Spark or pandas). It reads `output/*.json|csv` and profiles `data/raw/*.jsonl`
(issue counts, one sample record per issue, files with corrupt lines).
It also replays the silver and gold rules in plain Python to produce the explorer's before/after rows for one hotel, and
refuses to write `data.js` if the replay's totals disagree with `output/` (flattened, de-duplicated, invalid ratings,
rated reviews, sentiment labels, and every row of `top_hotels.csv`).

## What to show in class
| Area | Shows |
|---|---|
| **Step explorer** (click a step in *Batch tracking*, or *Look inside* on a stage) | Every operation inside each script (40 in total): before/after rows for one real hotel, the matching lines of the real script highlighted, rows-at-this-point, and a diagram of what Spark does (lazy plan vs. action vs. shuffle vs. write vs. collect) |
| *Explain each step* + Run pipeline | Guided lecture mode: the tracked batch stops at every stage and the explorer plays that script's operations before moving on |
| **Tracked data batch** card (left; click the highlighted block or the database map tool) | The dataset itself between stages: format, location, row count and grain, column changes vs. the previous stage (added / type-changed / flattened / dropped), which injected data-quality problems are still open or fixed, and real sample rows. Updates live during a run; when idle, step through the 6 snapshots and the batch moves to each |
| **12-week roadmap** (top bar) | L01–L12 in three parts plus milestones M1–M5. A lecture lights up where it lives on the map and opens a panel with its topics, the explorer operations that teach it, real results from that week's notebook (`notebooks/results/wNN.json`), figures, and links to the notes and notebook. `index.html#L06` or `#M2` opens one directly |
| Lab district (behind the pipeline) | L06 Predict, L07 Optimise, L08 Survival, L10 Network, L11 Web: the weekly labs that build on the pipeline but are not part of `run_all.sh`. Predict and Optimise read gold; the others bring their own dataset (small container beside them) |
| Quality gates | The 7 Week 12 checks, computed from `output/`: shown in Insights, in L12, and logged after every run |
| Map (click a tank) | Stage details: inputs/outputs, DQ counts, code, partitions. Tank gauges fill as each layer is written during a run |
| Processing units (click one) | Opens that script in the step explorer; its gear spins while it runs |
| Raw yard → *Injected quality problems* | Every kind of messy record in the source; click one to trace it |
| Record tracer (bottom left) | One review moving raw → bronze → silver → gold, using the same cast/date/lexicon rules as Spark |
| Gold → *Try the feature step* | Type a review and score it with the 03_features.py lexicon |
| Executors / Jobs / Driver log | What the cluster is doing during the replay, with the scripts' real log lines |
| Insights dashboard | Interactive charts from `output/`, plus the PNGs written by `05_dashboard.py` |

Files: `index.html`, `styles.css`, `app.js` (scene + UI), `explorer.js` (step explorer), `roadmap.js` (lectures, milestones, labs), `data.js` (generated). No dependencies.
Lecture links, figures and lab results assume the course repo layout (`notebooks/` and `notes/` two folders up); without them the labs show no numbers and the links are hidden or broken.
