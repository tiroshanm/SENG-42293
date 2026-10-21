/* Course roadmap - maps the 12 lectures and 5 project milestones onto the pipeline view.
   Lectures that live in the pipeline point at stages and explorer operations; the lab lectures
   (6, 7, 8, 10, 11) are pavilions fed by gold or by their own dataset. Lab numbers come from the
   weekly notebooks' results/*.json, bundled into data.js by build_data.py. */
(() => {
'use strict';
const RF = window.RF;
if (!RF) return;
const { D, icon, fmt, esc, pct } = RF;
const W = D.course?.weeks || {}, FIGS = new Set(D.course?.figures || []);
const ING = D.dq_ingest, CLN = D.dq_clean, SUM = D.summary, RAW = D.raw;
const $ = (s, r = document) => r.querySelector(s);
const pc = v => (100 * v).toFixed(1) + '%';
const L2 = n => 'L' + String(n).padStart(2, '0');

/* ---------- which lecture each explorer operation teaches ---------- */
const OP_LECTURE = [
  [4, 4, 4, 3, 3, 4, 3],                 // 01_ingest
  [4, 4, 3, 3, 3, 3, 3, 3, 3, 4, 3],     // 02_clean_flatten
  [4, 9, 9, 9, 5, 4, 3, 5, 5, 4],        // 03_features
  [5, 5, 5, 5, 5, 4],                    // 04_analytics
  [12, 12, 12, 12, 12, 12],              // 05_dashboard
];
(RF.OPS || []).forEach((ops, si) => ops.forEach((o, oi) => { o.L = OP_LECTURE[si]?.[oi]; }));
const opsFor = n => (RF.OPS || []).flatMap((ops, si) => ops.map((o, oi) => ({ o, si, oi })).filter(x => x.o.L === n));

/* ---------- small view helpers ---------- */
const tiles = list => `<div class="tiles">${list.map(([l, v, sub]) => `<div class="tile"><label>${l}</label><b>${v}</b>${sub ? `<div class="tsub">${sub}</div>` : ''}</div>`).join('')}</div>`;
const sec = (title, right, body) => `<div class="section"><div class="sec-head"><span>${title}</span><span>${right}</span></div>${body}</div>`;
const rows = list => `<div class="rows">${list.map(([a, b, c]) => `<div class="row"><div class="rt"><b>${a}</b>${b ? `<span>${b}</span>` : ''}</div>${c ? `<span class="rv">${c}</span>` : ''}</div>`).join('')}</div>`;
const figs = names => names.filter(n => FIGS.has(n)).map(n => `<a class="fig" href="../../notebooks/figures/${n}" target="_blank" rel="noopener"><img src="../../notebooks/figures/${n}" alt="${n}" loading="lazy" onerror="this.parentElement.remove()"></a>`).join('');
const fromNb = w => W[w] ? `<span class="chip">from ${w.replace('w', 'Week ').replace(' 0', ' ')} notebook</span>` : '';

/* ---------- the 12 lectures (titles and topics from the course page) ---------- */
const LECTURES = [
  { n: 1, part: 'I', short: "Big data V's", title: 'Course Introduction & Big Data Characteristics', lo: 'LO1, LO2', kind: 'frame', notes: 'LN01_Big_Data_Characteristics', lit: ['raw', 'quarantine', 'spark', 'dash'],
    topics: ['Traditional data vs big data', 'Volume, Velocity, Variety, Veracity, Value', 'Opportunities and challenges'],
    here: () => sec('The V\'s in this dataset', 'real numbers', rows([
      ['Volume', `${fmt(CLN.reviews_flattened)} reviews in ${fmt(RAW.files.reduce((a, f) => a + f.kb, 0))} KB, teaching scale; the same code runs on a Spark cluster`],
      ['Variety', 'nested JSON objects + arrays, free text, two date formats, mixed-type ratings'],
      ['Veracity', `${ING.lines_quarantined} corrupt lines, ${RAW.issues.duplicate} duplicates, ${fmt(['missing_key', 'null', 'na_text', 'out_of_range'].reduce((a, k) => a + (RAW.issues[k] || 0), 0))} unusable ratings`],
      ['Velocity', `batch: ${RAW.files.length} files per run (a streaming source would replace data/raw)`],
      ['Value', `r = ${SUM.corr_rating_sentiment} rating↔sentiment, city ranking, top-10 hotels`]])) },
  { n: 2, part: 'I', short: 'Collection & sampling', title: 'Data Collection & Sampling', lo: 'LO1, LO3', kind: 'frame', notes: 'LN02_Data_Collection_and_Sampling', nb: 'Week2_Data_Collection_Sampling', lit: ['raw'],
    topics: ['Batch vs streaming collection', 'Random, stratified, systematic, cluster sampling', 'Bias; sample size n = Z²p(1−p)/e² ≈ 385', 'Reservoir sampling, Count-Min, HyperLogLog'],
    here: () => sec('In this pipeline', 'milestone M1', rows([
      ['Collection', `batch: ${RAW.files.length} JSON-lines files land in data/raw/ (seeded generator, so every student gets identical data)`],
      ['Population vs sample', `the pipeline processes the whole source (${ING.lines_read} hotels); sampling matters when the source is too big or too costly to read in full`],
      ['Sample size', 'to estimate a share within ±5 points at 95% confidence: n = 1.96² × 0.5 × 0.5 / 0.05² ≈ 385 reviews'],
      ['The explorer\'s demo hotel', 'a purposive case (the hotel with the most problem types), not a random sample. Good for teaching, not for estimating'],
      ['Lab', 'the Week 2 notebook practises the techniques on a separate 10,000-student population']])) },
  { n: 3, part: 'I', short: 'Data quality I', title: 'Data Quality & Pre-processing I', lo: 'LO3', kind: 'pipe', notes: 'LN03_Data_Quality_Preprocessing_I', nb: 'Week3_Data_Quality_Preprocessing', lit: ['quarantine', 'silver'],
    topics: ['Six data-quality dimensions', 'MCAR / MAR / MNAR and imputation', 'Duplicates and outliers', 'A reusable cleaning pipeline'],
    here: () => sec('Quality dimensions → what the pipeline does', 'counts from data/raw', `<table class="tbl"><thead><tr><th>Dimension</th><th>Problem</th><th class="r">Count</th><th>Fix</th></tr></thead><tbody>${[
      ['Veracity', 'truncated JSON lines', ING.lines_quarantined, 'quarantine (01)'], ['Uniqueness', 'duplicate review_id', RAW.issues.duplicate, 'dropDuplicates (02)'],
      ['Validity', 'rating "N/A"', RAW.issues.na_text, 'cast → null (02)'], ['Accuracy', 'rating 0, 11, −1', RAW.issues.out_of_range, 'between(1, 5) → null (02)'],
      ['Completeness', 'rating null / key missing', (RAW.issues.null || 0) + (RAW.issues.missing_key || 0), 'kept as null, MCAR (02)'],
      ['Consistency', 'dd/MM/yyyy dates', RAW.issues.date_ddmmyyyy, 'coalesce two formats (02)']].map(r => `<tr><td><b>${r[0]}</b></td><td>${r[1]}</td><td class="r">${fmt(r[2])}</td><td>${r[3]}</td></tr>`).join('')}</tbody></table>`) },
  { n: 4, part: 'I', short: 'DQ II · Spark', title: 'Data Quality & Pre-processing II', lo: 'LO3, LO4', kind: 'pipe', notes: 'LN04_Data_Quality_Preprocessing_II', nb: 'Week4_Data_Quality_Preprocessing_II', lit: ['bronze', 'silver', 'spark'],
    topics: ['Schema-on-read vs schema-on-write', 'Flattening with explode; type drift; corrupt records', 'Driver, executors, lazy evaluation, shuffles', 'Parquet; feature engineering at scale'],
    results: () => W.w04 && tiles([['Lines in one file', W.w04.lines, `${W.w04.corrupt} corrupt`], ['Flattened → de-dup', `${fmt(W.w04.flat)} → ${fmt(W.w04.dedup)}`, `${fmt(W.w04.valid)} with a valid rating`],
      ['Word count (Map → Shuffle → Reduce)', W.w04.wc_total, `${W.w04.wc_distinct} distinct words`], ['Hotels parsed', W.w04.hotels, 'same rules as 01–02 on a single file']]), figs: ['w04_funnel.png'] },
  { n: 5, part: 'II', short: 'Descriptive', title: 'Descriptive Analytics', lo: 'LO3', kind: 'pipe', notes: 'LN05_Descriptive_Analytics', nb: 'Week5_Descriptive_Analytics', lit: ['gold', 'dash'],
    topics: ['Centre, spread and shape; group-by', 'Trends; correlation is not causation', 'The small-sample trap: shrinkage rankings', 'KPI dashboard and Spark approxQuantile'],
    results: () => W.w05 && tiles([['Mean ± SD', `${W.w05.mean} ± ${W.w05.std}`, `skew ${W.w05.skew} (slightly left)`], ['Best / worst city', `${W.w05.best_city} / ${W.w05.worst_city}`, `gap ${W.w05.city_gap} stars`],
      ['Rating ↔ hotel stars', 'r ' + W.w05.corr_rating_stars, 'stars do not predict ratings'], ['Spark approxQuantile', W.w05.spark_q.join(' · '), 'matches 04_analytics.py']]), figs: ['w05_dashboard.png', 'w05_funnel_plot.png'] },
  { n: 6, part: 'II', short: 'Predictive', title: 'Predictive Analytics', lo: 'LO3', kind: 'lab', stage: 'predict', notes: 'LN06_Predictive_Analytics', nb: 'Week6_Predictive_Analytics', lit: ['predict', 'gold'],
    feeds: 'gold/reviews (exported as reviews_clean.csv)', question: 'Can we flag reviews that will be rated 4★ or more, using only what is known when the review is posted?',
    results: () => W.w06 && tiles([['Baseline accuracy', pc(W.w06.base_acc), 'always predict the majority'], ['Gradient boosting (test)', `AUC ${W.w06.gb_test.auc}`, `accuracy ${pc(W.w06.gb_test.accuracy)} · recall ${pc(W.w06.gb_test.recall)}`],
      ['Cross-validated AUC', `GB ${W.w06.cv['Gradient boosting']}`, `RF ${W.w06.cv['Random forest']} · LR ${W.w06.cv['Logistic regression']}`], ['Leakage alarm', `AUC ${W.w06.auc_text}`, 'with review text, which encodes the rating: too good to be true']]),
    figs: ['w06_evaluation.png', 'w06_leakage.png'] },
  { n: 7, part: 'II', short: 'Prescriptive', title: 'Prescriptive Analytics', lo: 'LO3', kind: 'lab', stage: 'optimise', notes: 'LN07_Prescriptive_Analytics', nb: 'Week7_Prescriptive_Analytics', lit: ['optimise', 'gold'],
    feeds: 'gold/hotel_stats', question: 'Which hotels should a campaign feature with a budget of 25 units (2★ = 1 … 5★ = 4)?',
    results: () => W.w07 && tiles([...W.w07.plans.map(p => [p.plan, `${p['expected extra bookings']}`, `extra bookings · ${p['hotels chosen']} hotels`]), ['Shadow price of budget', W.w07.shadow_budget, `bookings per extra unit · optimal is +${W.w07.opt_gain_vs_naive_pct}% vs naive`]]),
    note: 'Uplift comes from an assumed model, as the notebook stresses. In practice it would come from Week 6 or a Week 11 A/B test.', figs: ['w07_plans.png', 'w07_frontier.png'] },
  { n: 8, part: 'II', short: 'Survival', title: 'Survival Analysis', lo: 'LO3', kind: 'lab', stage: 'survival', notes: 'LN08_Survival_Analysis', nb: 'Week8_Survival_Analysis', lit: ['survival'],
    feeds: 'notebooks/data/member_tenure.csv (its own loyalty-programme source)', question: 'How long do loyalty members stay before cancelling, and what keeps them?',
    results: () => W.w08 && tiles([['Members · censored', `${fmt(W.w08.n)} · ${W.w08.censored_pct}%`, `${fmt(W.w08.events)} cancellations observed`], ['Naive mean vs Kaplan–Meier median', `${W.w08.naive_all} vs ${W.w08.km_median} mo`, 'ignoring censoring under-states tenure'],
      ['Premium vs free (median)', `${W.w08.med_by_plan.premium} vs ${W.w08.med_by_plan.free} mo`, `Cox HR ${W.w08.cox[0].HR} for premium`], ['Model fit', `C-index ${W.w08.cindex}`, 'Cox proportional hazards']]), figs: ['w08_km.png', 'w08_forest.png'] },
  { n: 9, part: 'III', short: 'Sentiment & text', title: 'Sentiment & Text Analytics', lo: 'LO3', kind: 'pipe', notes: 'LN09_Sentiment_Text_Analytics', nb: 'Week9_Sentiment_Text_Analytics', lit: ['gold'],
    topics: ['Pre-processing, bag of words, TF-IDF, n-grams', 'Lexicon vs learned sentiment; negation', 'NMF topics and aspect scorecards', 'Spark ML text pipeline'],
    results: () => W.w09 && tiles([['Document-term matrix', `${fmt(W.w09.dtm[0])} × ${W.w09.dtm[1]}`, `${pc(W.w09.sparsity)} zeros`], ['Lexicon vs rating', `ρ ${W.w09.lex_spearman}`, 'Spearman, rated reviews'],
      ['Negation (hard cases)', `${pc(W.w09.hard_acc.lexicon)} → ${pc(W.w09.hard_acc['lexicon+negation'])}`, 'lexicon, then + negation rule'], ['Worst aspect', W.w09.aspects?.[0]?.aspect || '–', W.w09.aspects?.[0] ? `mean ★ ${W.w09.aspects[0]['mean rating when mentioned']} when mentioned` : '']]),
    note: 'The pipeline\'s lexicon lists "not" as negative, a crude negation rule. The notebook shows why proper negation handling matters.', figs: ['w09_topics.png', 'w09_aspects.png'] },
  { n: 10, part: 'III', short: 'Networks', title: 'Social Network Analytics', lo: 'LO3, LO4', kind: 'lab', stage: 'network', notes: 'LN10_Social_Network_Analytics', nb: 'Week10_Social_Network_Analytics', lit: ['network'],
    feeds: 'notebooks/data/follows.csv + traveller_profiles.csv (its own source)', question: 'Which travellers are influential, which groups exist, and how far would a recommendation spread?',
    results: () => W.w10 && tiles([['Graph', `${fmt(W.w10.graph.nodes)} · ${fmt(W.w10.graph.edges)}`, 'travellers · follow edges'], ['Degree mean / median / max', `${W.w10.deg.mean} / ${W.w10.deg.median} / ${W.w10.deg.max}`, 'long tail: the mean misleads'],
      ['Louvain communities', `${W.w10.louvain.k} · Q ${W.w10.louvain.modularity}`, `purity ${W.w10.louvain.purity} vs known interests`], ['Diffusion reach (mean)', `${W.w10.diffusion['top PageRank']} vs ${W.w10.diffusion['random traveller']}`, 'travellers reached: top-PageRank seed vs random']]),
    figs: ['w10_communities.png', 'w10_diffusion.png'] },
  { n: 11, part: 'III', short: 'Web analytics', title: 'Web Analytics', lo: 'LO3, LO4', kind: 'lab', stage: 'web', notes: 'LN11_Web_Analytics', nb: 'Week11_Web_Analytics', lit: ['web'],
    feeds: 'notebooks/data/clickstream.csv (its own event stream)', question: 'Where do visitors drop out of booking, and did page design B really help?',
    results: () => W.w11 && tiles([['Events → sessions', `${fmt(W.w11.events)} → ${fmt(W.w11.sessions)}`, `${fmt(W.w11.visitors)} visitors, 30-minute rule`], ['Bounce · conversion', `${pc(W.w11.metrics.bounce)} · ${pc(W.w11.metrics.conversion)}`, `worst step: ${W.w11.worst_step[0]} (${pc(W.w11.worst_step[1])} continue)`],
      ['A/B test: B vs A', `${pc(W.w11.ab.rate_B)} vs ${pc(W.w11.ab.rate_A)}`, `z ${W.w11.ab.z}, p ${W.w11.ab.p.toFixed(3)}`], ['Peeking false positives', `${pc(W.w11.peeking.peek)}`, `vs ${pc(W.w11.peeking.fixed)} with a fixed horizon`]]),
    figs: ['w11_funnel.png', 'w11_ab.png'] },
  { n: 12, part: 'III', short: 'Visualisation', title: 'Visualisation & the End-to-End Pipeline', lo: 'LO3, LO4', kind: 'pipe', notes: 'LN12_Visualization_Capstone', nb: 'Week12_Visualization_Capstone', lit: ['dash', 'spark'],
    topics: ['Misleading vs honest charts', 'Small multiples and a one-screen dashboard', 'Pipeline quality gates (7 checks)', 'Capstone report structure'],
    here: () => sec(`Quality gates · ${RF.GATES.filter(g => g.pass).length}/${RF.GATES.length} passed`, 'computed from this output/', RF.gatesHTML()) +
      `<button class="btn block" data-rm="insights">${icon('chart', 'sm')} Open the insights dashboard</button>`, figs: ['w12_dashboard.png', 'w12_truncation.png'] },
];
const MILESTONES = [
  { id: 'M1', after: 2, title: 'Data source and sampling plan', lit: ['raw'], lectures: [1, 2], steps: [], says: 'Choose a licensed source and justify how it is collected and sampled. In the demo: the raw JSON yard.' },
  { id: 'M2', after: 4, title: 'Ingest, flatten, clean, store as Parquet', lit: ['raw', 'bronze', 'quarantine', 'silver'], lectures: [3, 4], steps: [0, 1], says: 'Scripts 01 and 02: schema-on-read, quarantine, explode, de-duplicate, repair types, partitioned Parquet.' },
  { id: 'M3', after: 6, title: 'Descriptive dashboard + first predictive model', lit: ['gold', 'dash', 'predict'], lectures: [5, 6], steps: [3, 4], says: 'Scripts 04–05 for the descriptive dashboard, and the Prediction Lab for the first model on gold data.' },
  { id: 'M4', after: 9, title: 'Text and sentiment features', lit: ['gold'], lectures: [9], steps: [2], says: 'Script 03: tokenise, lexicon sentiment, labels and aggregates in the gold layer.' },
  { id: 'M5', after: 12, title: 'Full pipeline on a multi-node cluster + demo and report', lit: ['raw', 'bronze', 'quarantine', 'silver', 'gold', 'dash', 'spark'], lectures: [12], steps: [0, 1, 2, 3, 4], says: './run_cluster.sh: 1 master + 2 workers, quality gates passing, charts and a capstone report.' },
];
const LEC = Object.fromEntries(LECTURES.map(l => [L2(l.n), l]));
const KIND = { pipe: ['in the pipeline', 'brand'], lab: ['lab, builds on the pipeline', 'lab'], frame: ['framing & data source', ''] };

/* ---------- lecture / milestone panels ---------- */
function lecturePanel(l) {
  const id = L2(l.n), ops = opsFor(l.n), [kl, kc] = KIND[l.kind], st = l.stage && RF.STAGES[l.stage];
  const res = l.results?.();
  return `<div class="ph"><div class="ph-icon" style="--c:${st?.color || '#2f66f6'}">${icon(st?.icon || 'map')}</div>
      <div><div class="eyebrow">Lecture ${String(l.n).padStart(2, '0')} · Part ${l.part} · ${l.lo}</div><h2>${esc(l.title)}</h2><div class="sub">${st ? `${st.name} on the map` : 'Week ' + l.n}</div></div></div>
    <div class="chips"><span class="chip ${kc}">${kl}</span>${l.kind === 'lab' ? '<span class="chip">not in run_all.sh</span>' : ''}${l.nb ? fromNb('w' + String(l.n).padStart(2, '0')) : ''}</div>
    ${l.question ? `<div class="xnote"><b>Question:</b> ${esc(l.question)}</div>` : ''}
    ${l.feeds ? `<div class="lab-feed">${icon('db', 'sm')} <span>Input: <b>${esc(l.feeds)}</b></span></div>` : ''}
    ${l.here ? l.here() : ''}
    ${res ? sec('Results', 'real numbers from the lab', res) : ''}
    ${l.note ? `<div class="xnote warn">${icon('alert', 'sm')} ${esc(l.note)}</div>` : ''}
    ${ops.length ? sec('Inside the pipeline scripts', `${ops.length} operations`, `<div class="rows">${ops.map(x => `<button class="row" data-rm="op" data-si="${x.si}" data-oi="${x.oi}"><div class="ri">${x.si + 1}.${x.oi + 1}</div>
      <div class="rt"><b>${esc(x.o.t)}</b><span>${RF.STEPS[x.si].script} · ${esc(x.o.concept)}</span></div>${icon('search', 'sm')}</button>`).join('')}</div>`) : ''}
    ${l.topics ? sec('Lecture topics', '', `<ul class="topics">${l.topics.map(t => `<li>${esc(t)}</li>`).join('')}</ul>`) : ''}
    ${l.figs ? `<div class="figs">${figs(l.figs)}</div>` : ''}
    <div class="links"><a class="btn" href="../../notes/${l.notes}.html" target="_blank" rel="noopener">${icon('file', 'sm')} Notes</a>${l.nb ? `<a class="btn" href="../../notebooks/${l.nb}.html" target="_blank" rel="noopener">${icon('open', 'sm')} Notebook</a>` : ''}</div>`;
}
function milestonePanel(m) {
  return `<div class="ph"><div class="ph-icon" style="--c:#152033"><b>${m.id}</b></div><div><div class="eyebrow">Project milestone · after L${String(m.after).padStart(2, '0')}</div><h2>${esc(m.title)}</h2><div class="sub">Travel-review analytics pipeline · 20% of the grade</div></div></div>
    <p class="xp-say" style="margin:12px 0">${esc(m.says)}</p>
    ${sec('Built on', 'lectures', `<div class="rows">${m.lectures.map(n => `<button class="row" data-rm="lec" data-l="${L2(n)}"><div class="ri">${L2(n)}</div><div class="rt"><b>${esc(LEC[L2(n)].title)}</b><span>${KIND[LEC[L2(n)].kind][0]}</span></div></button>`).join('')}</div>`)}
    ${m.steps.length ? sec('Scripts to show', '', `<div class="rows">${m.steps.map(si => `<button class="row" data-rm="op" data-si="${si}" data-oi="0"><div class="ri">${si + 1}</div><div class="rt"><b>${RF.STEPS[si].script}</b><span>${RF.OPS?.[si]?.length || 0} operations · look inside</span></div>${icon('search', 'sm')}</button>`).join('')}</div>`) : ''}`;
}
LECTURES.forEach(l => { RF.PANELS[L2(l.n)] = () => lecturePanel(l); if (l.stage) RF.PANELS[l.stage] = () => lecturePanel(l); });
MILESTONES.forEach(m => { RF.PANELS[m.id] = () => milestonePanel(m); });
RF.litFor = id => LEC[id]?.lit || MILESTONES.find(m => m.id === id)?.lit || null;

/* ---------- the roadmap strip (replaces the KPI row while on) ---------- */
RF.renderRoadmap = () => {
  const sel = RF.state.selected;
  return `<div class="roadmap card"><div class="rm-parts">${['I', 'II', 'III'].map((p, i) => `<span style="grid-column:${i * 4 + 1} / span 4">Part ${p} · ${['Foundations', 'Analytics core', 'Specialised analytics'][i]}</span>`).join('')}</div>
    <div class="rm-lecs">${LECTURES.map(l => `<button class="lec ${l.kind} ${sel === L2(l.n) ? 'on' : ''}" data-rm="lec" data-l="${L2(l.n)}" title="${esc(l.title)}"><b>${L2(l.n)}</b><span>${esc(l.short)}</span></button>`).join('')}</div>
    <div class="rm-ms">${MILESTONES.map(m => `<button class="ms ${sel === m.id ? 'on' : ''}" style="grid-column:${m.after}" data-rm="ms" data-m="${m.id}" title="${esc(m.title)}">◆ ${m.id}</button>`).join('')}</div>
    <div class="rm-legend"><span><i class="pipe"></i>in the pipeline</span><span><i class="lab"></i>lab district</span><span><i class="frame"></i>framing</span><span>◆ project milestone</span></div></div>`;
};
document.addEventListener('click', e => {
  const b = e.target.closest('[data-rm]'); if (!b) return;
  const a = b.dataset.rm;
  if (a === 'lec') { RF.select(b.dataset.l, true); RF.renderKPIs(); }
  if (a === 'ms') { RF.select(b.dataset.m, true); RF.renderKPIs(); }
  if (a === 'op') RF.openExplorer?.(+b.dataset.si, +b.dataset.oi);
  if (a === 'insights') RF.openInsights();
});
LECTURES.forEach(l => RF.INDEX.push({ label: `${L2(l.n)} · ${l.title}`, kind: KIND[l.kind][0], icon: 'map', go: () => { RF.state.roadmap = true; $('#roadmap-btn')?.classList.add('on'); RF.select(L2(l.n), true); RF.renderKPIs(); } }));
const hash = /^#(L\d\d|M\d)$/.exec(location.hash);          // index.html#L06 or #M3 opens the roadmap on that item
if (hash && RF.PANELS[hash[1]]) { RF.state.roadmap = true; $('#roadmap-btn')?.classList.add('on'); RF.renderKPIs(); requestAnimationFrame(() => RF.select(hash[1], true)); }
else RF.select(RF.state.selected);
})();
