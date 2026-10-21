/* Step explorer - opens one pipeline script and walks through every operation inside it:
   real before/after rows for one hotel, the matching lines of the real script, and what Spark does
   (lazy plan vs. action vs. shuffle vs. write). Row data comes from build_data.py's plain-Python replay,
   which is cross-checked against output/ when data.js is built. */
(() => {
'use strict';
const RF = window.RF;
if (!RF) return;
const { D, icon, fmt, esc, pct, sentiment, POS, NEG } = RF;
const $ = (s, r = document) => r.querySelector(s);
const DEMO = D.replay.demo, H = DEMO.hotel, ING = D.dq_ingest, CLN = D.dq_clean, SUM = D.summary;
const goodLines = ING.lines_read - ING.lines_quarantined;
const srcFile = `hotels_${String(+H.hotel_id.slice(2) % 5).padStart(2, '0')}.jsonl`;   // generator writes hotel i to file i % 5
const SENT = Object.fromEntries(D.sentiment.map(r => [r.label, r]));

/* ---------- replay of the silver/gold rules for the demo hotel's reviews (same logic as the scripts) ---------- */
const NUM = /^\s*[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?\s*$/;
const seen = new Set();
const ROWS = DEMO.reviews.map(r => {
  const has = 'rating' in r, raw = has ? r.rating : undefined;
  const str = raw == null ? null : String(raw);
  const cast = str != null && NUM.test(str) ? Number(str) : null;
  const rating = cast != null && cast >= 1 && cast <= 5 ? cast : null;
  const d1 = /^\d{4}-\d{2}-\d{2}$/.test(r.date) ? r.date : null;
  const m = /^(\d{2})\/(\d{2})\/(\d{4})$/.exec(r.date), d2 = m ? `${m[3]}-${m[2]}-${m[1]}` : null;
  const date = d1 || d2, f = sentiment(r.text.trim()), dup = seen.has(r.review_id);
  seen.add(r.review_id);
  return { r, has, raw, str, cast, rating, d1, d2, date, f, dup,
    weekday: date ? ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'][new Date(date + 'T00:00:00Z').getUTCDay()] : null };
});
const KEPT = ROWS.filter(x => !x.dup);
const MIXED = KEPT.find(x => x.f.p.length && x.f.n.length) || KEPT[0];
const showRaw = x => !x.has ? '<i class="muted">absent</i>' : x.raw === null ? 'null' : typeof x.raw === 'string' ? `"${esc(x.raw)}"` : esc(x.raw);
const showStr = x => x.str == null ? 'null' : `"${esc(x.str)}"`;
const showNum = v => v == null ? 'null' : Number.isInteger(v) ? v.toFixed(1) : String(v);
const short = (s, n = 34) => s.length > n ? s.slice(0, n - 1) + '…' : s;

/* ---------- tiny view helpers ---------- */
const tbl = (cols, rows, cls = '') => `<div class="xt-wrap"><table class="xt ${cls}"><thead><tr>${cols.map(c => `<th>${c}</th>`).join('')}</tr></thead>
  <tbody>${rows.map(r => `<tr class="${r.cls || ''}" style="${r.style || ''}">${r.cells.map(c => typeof c === 'object' && c !== null ? `<td class="${c.c || ''}">${c.v}</td>` : `<td>${c}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
const ba = (before, after, label = '') => `<div class="ba"><div class="ba-col">${before}</div><div class="ba-arrow">${icon('route', 'sm')}<span>${label}</span></div><div class="ba-col">${after}</div></div>`;
const box = (title, body, cls = '') => `<div class="xbox ${cls}"><div class="xbox-h">${title}</div>${body}</div>`;
const tree = lines => `<div class="tree">${lines.map(([d, t, note, cls]) => `<div class="${cls || ''}" style="padding-left:${d * 16}px">${d ? '<span class="muted">└─ </span>' : ''}${t}${note ? ` <span class="muted">${note}</span>` : ''}</div>`).join('')}</div>`;
const bars = (items, color = 'var(--brand)') => { const max = Math.max(...items.map(i => i[1])); return `<div class="xbars">${items.map(([l, v, c]) => `<div class="xbar"><span>${l}</span><i style="--w:${100 * v / max}%;--c:${c || color}"></i><b>${fmt(v)}</b></div>`).join('')}</div>`; };
const note = (html, kind = '') => `<div class="xnote ${kind}">${html}</div>`;
const png = n => `<a class="png" href="../output/chart_${n}.png" target="_blank" rel="noopener"><img src="../output/chart_${n}.png" alt="output/chart_${n}.png" onerror="this.parentElement.classList.add('missing')"><span>output/chart_${n}.png</span></a>`;
const csvPreview = (name, rows, n = 4) => box(`${icon('file', 'sm')} output/${name}`, tbl(Object.keys(rows[0]), rows.slice(0, n).map(r => ({ cells: Object.values(r).map(v => esc(v)) }))) + (rows.length > n ? `<div class="muted xmore">… ${rows.length - n} more rows</div>` : ''));

/* ---------- operations inside each script ----------
   kind: lazy (adds to the plan) · action (runs a job) · write (action that writes files) · collect (action that brings rows to the driver) · host (plain Python)
   wide: the operation needs a shuffle (rows with the same key must meet in one partition) */
const OPS = [
  /* ===== 01_ingest.py ===== */
  [
    { t: 'Read with an explicit schema', k: 'lazy', concept: 'Schema-on-read', rows: [ING.lines_read, 'JSON lines'],
      marks: ['review = StructType', 'StructField(', 'schema = StructType', 'raw = (spark.read.schema(schema)'],
      say: `Spark is told the structure up front instead of scanning the files to guess it. <b>rating</b> is declared as a STRING on purpose: the source mixes <code>4.5</code>, <code>"4.5"</code>, <code>"N/A"</code> and <code>null</code>, and a numeric type would make those rows fail.`,
      view: () => ba(box(`${icon('file', 'sm')} data/raw/${srcFile} · one line = one hotel`, `<div class="code xs">${esc(DEMO.line)}…</div>`),
        box('Schema Spark applies', tree([[0, '<b>root</b>'], [1, 'hotel_id: string'], [1, 'name: string'], [1, 'location: struct', 'city, lat, lon'], [1, 'stars: int'],
          [1, 'reviews: array&lt;struct&gt;'], [2, 'review_id, user, text, date: string'], [2, '<b>rating: string</b>', '← on purpose', 'hl'], [2, 'tags: array&lt;string&gt;, helpful_votes: int'], [1, '_corrupt: string', '← broken lines land here', 'hl']])), 'schema') },
    { t: 'PERMISSIVE mode keeps broken lines', k: 'lazy', concept: 'Veracity · fault tolerance',
      marks: ['.option("mode", "PERMISSIVE")', '.json(RAW).cache()'],
      say: `In PERMISSIVE mode a line that is not valid JSON does not crash the job. Its fields become null and the whole raw text is copied into <code>_corrupt</code>, so nothing is silently lost.`,
      view: () => {
        const c = D.raw.corrupt[0];
        return box(`${icon('check', 'sm')} A good line → typed columns`, tbl(['hotel_id', 'name', 'location.city', 'stars', 'reviews', '_corrupt'],
            [{ cells: [H.hotel_id, esc(H.name), esc(H.location.city), H.stars, `array[${DEMO.n_reviews}]`, { v: 'null', c: 'good' }] }])) +
          (c ? box(`${icon('ban', 'sm')} A broken line from ${c.file} (${fmt(c.chars)} chars, cut off mid-object)`, `<div class="code xs">${esc(c.head)} … ${esc(c.tail.slice(-70))}<span class="hl"> ⟵ line ends here</span></div>` +
            tbl(['hotel_id', 'name', 'location.city', 'stars', 'reviews', '_corrupt'], [{ cells: ['null', 'null', 'null', 'null', 'null', { v: esc(short(c.head, 30)), c: 'bad' }] }]), 'bad') : '');
      } },
    { t: 'cache() and count() - the first action', k: 'action', concept: 'Lazy evaluation', rows: [ING.lines_read, 'lines'],
      marks: ['total = raw.count()'],
      say: `Everything so far only built a plan. <code>count()</code> is an <b>action</b>: Spark now reads all ${D.raw.files.length} files in parallel. <code>cache()</code> keeps the parsed rows in executor memory so the next two filters don't re-read the JSON.`,
      view: () => box('Lines read per file', bars(D.raw.files.map(f => [f.file, f.lines]))) + note(`total = <b>${ING.lines_read}</b> lines. The pipeline's own log prints the same number.`) },
    { t: 'Split good rows from corrupt rows', k: 'action', concept: 'Data quality gate', rows: [goodLines, 'good hotel rows'],
      marks: ['bad = raw.filter', 'good = raw.filter', 'n_bad = bad.count()'],
      say: `Two filters on the cached data: rows where <code>_corrupt</code> is null are good, the rest are quarantined. <code>bad.count()</code> is another action, cheap because the data is cached.`,
      view: () => ba(box('All lines', bars([['parsed', ING.lines_read]])), box('After the gate', bars([['good → bronze', goodLines, 'var(--good)'], ['bad → quarantine', ING.lines_quarantined, 'var(--bad)']]))) +
        box('Corrupt lines per file', bars(D.raw.files.filter(f => f.bad).map(f => [f.file, f.bad, 'var(--bad)']))) },
    { t: 'Write rejects to quarantine', k: 'write', concept: 'No silent data loss', rows: [ING.lines_quarantined, 'quarantined lines'], lake: 'quarantine',
      marks: ['bad.select("_corrupt").write'],
      say: `Only the <code>_corrupt</code> text is written, as JSON, so a person can open the folder and see why each line failed. In the lab students explain the failure.`,
      view: () => tree([[0, '<b>lake/quarantine/</b>'], [1, 'part-00000-…json', `${ING.lines_quarantined} lines`], [1, '_SUCCESS']]) +
        tbl(['file', 'length', 'end of the line'], D.raw.corrupt.map(c => ({ cells: [c.file, fmt(c.chars) + ' chars', { v: `<span class="mono">…${esc(c.tail.slice(-46))}</span>`, c: 'bad' }] }))) },
    { t: 'Add audit columns → bronze Parquet', k: 'write', concept: 'Medallion: bronze', rows: [goodLines, 'bronze rows'], lake: 'bronze',
      marks: ['good.withColumn("_ingested_at"', '.withColumn("_source_file"', '.write.mode("overwrite").parquet(BRONZE)'],
      say: `Good rows are stored <b>as received</b> (still nested) plus two lineage columns: when and from which file. Parquet is columnar and compressed, and bronze can always be replayed if a later rule changes.`,
      view: () => tbl(['hotel_id', 'name', 'location', 'stars', 'reviews', '_ingested_at', '_source_file'],
          [{ cells: [H.hotel_id, esc(H.name), `{${esc(H.location.city)}, ${H.location.lat}, ${H.location.lon}}`, H.stars, `array[${DEMO.n_reviews}]`, { v: 'run timestamp', c: 'chg' }, { v: `…/raw/${srcFile}`, c: 'chg' }] }]) +
        ba(box('Row layout (JSON)', `<div class="rowstore">${[1, 2, 3].map(i => `<div>${['id', 'name', 'loc', 'stars', 'reviews'].map(c => `<i>${c}${i}</i>`).join('')}</div>`).join('')}</div>`),
           box('Column layout (Parquet)', `<div class="colstore">${['id', 'name', 'loc', 'stars', 'reviews'].map(c => `<div>${[1, 2, 3].map(i => `<i>${c}${i}</i>`).join('')}</div>`).join('')}</div>`), 'columnar') +
        note('A query that needs only <code>stars</code> reads only that column from disk.') },
    { t: 'Record data-quality numbers', k: 'host', concept: 'Row-count discipline',
      marks: ['print(f"lines read', 'json.dump({"lines_read"'],
      say: `The driver prints the counts and saves them to <code>output/dq_ingest.json</code>. Every stage leaves numbers behind so the funnel can be checked later.`,
      view: () => `<div class="code">$ python pipeline/01_ingest.py\nlines read=${ING.lines_read}  good=${goodLines}  quarantined=${ING.lines_quarantined}</div>` + box(`${icon('file', 'sm')} output/dq_ingest.json`, `<div class="code xs">${esc(JSON.stringify(ING, null, 1))}</div>`) },
  ],
  /* ===== 02_clean_flatten.py ===== */
  [
    { t: 'Read bronze (nested)', k: 'lazy', concept: 'Semi-structured data', rows: [goodLines, 'hotel rows'],
      marks: ['bronze = spark.read.parquet(BRONZE)'],
      say: `Bronze still has one row per <b>hotel</b>, with all of its reviews packed inside an array. Analysis needs one row per <b>review</b>.`,
      view: () => tbl(['hotel_id', 'name', 'location.city', 'stars', 'reviews'], [{ cells: [H.hotel_id, esc(H.name), esc(H.location.city), H.stars,
        `<div class="arr">${ROWS.map(x => `<span>${x.r.review_id.slice(-4)}</span>`).join('')}<span class="muted">+${DEMO.n_reviews - ROWS.length}</span></div>`] }]) },
    { t: 'Flatten: explode(reviews) + r.*', k: 'lazy', concept: 'Flattening arrays', rows: null,
      marks: ['flat = (bronze.select', 'F.explode("reviews")', '.select("hotel_id", "hotel_name", "stars", "city", "r.*")'],
      say: `<code>explode</code> turns each element of the array into its own row and copies the hotel columns onto it. <code>location.city</code> is lifted out with dot notation and <code>r.*</code> spreads the review struct into columns.`,
      view: () => ba(box('1 hotel row', `<div class="hotelchip"><b>${H.hotel_id}</b> ${esc(H.name)}<br><span class="muted">reviews: array[${DEMO.n_reviews}]</span></div>`),
        box(`${DEMO.n_reviews} review rows (${ROWS.length} shown)`, tbl(['hotel_id', 'city', 'review_id', 'rating', 'date'],
          ROWS.map((x, i) => ({ style: `animation-delay:${i * 70}ms`, cls: 'pop', cells: [H.hotel_id, esc(H.location.city), x.r.review_id, showRaw(x), esc(x.r.date)] })))), 'explode') },
    { t: 'count() - check the row count', k: 'action', concept: 'Count after every structural change', rows: [CLN.reviews_flattened, 'review rows'],
      marks: ['n_flat = flat.count()'],
      say: `Exploding multiplies rows, so it is the place where a bug most easily creates or loses data. Counting right after it is the "row-count discipline" from the lecture.`,
      view: () => ba(box('Before', `<div class="big">${goodLines}</div><div class="muted">hotel rows</div>`), box('After', `<div class="big">${fmt(CLN.reviews_flattened)}</div><div class="muted">review rows · ≈${(CLN.reviews_flattened / goodLines).toFixed(1)} per hotel</div>`), '×') },
    { t: 'dropDuplicates(["review_id"])', k: 'lazy', wide: true, concept: 'Uniqueness · shuffle',
      marks: ['dedup = flat.dropDuplicates(["review_id"])'],
      say: `Duplicates are found on the <b>business key</b>, not the whole row. To compare keys Spark must send every row with the same <code>review_id</code> to the same partition. That network step is a <b>shuffle</b>, and it splits the job into two stages.`,
      view: () => tbl(['position in array', 'review_id', 'user', 'rating', 'text'], ROWS.filter(x => ROWS.some(y => y !== x && y.r.review_id === x.r.review_id)).map(x => ({
          cls: x.dup ? 'gone' : 'keep', cells: [`reviews[${x.r._pos}]`, x.r.review_id, x.r.user, showRaw(x), esc(short(x.r.text, 30)) + (x.dup ? ' <span class="chip bad">dropped</span>' : ' <span class="chip good">kept</span>')] }))) +
        `<div class="shuffle">${[0, 1].map(e => `<div class="exec-l"><b>executor ${e + 1}</b>${[0, 1].map(p => `<i>${e === 0 && p === 1 ? 'R038 ×1' : ''}${e === 1 && p === 0 ? 'R038 ×1' : ''}</i>`).join('')}</div>`).join('<div class="sh-arrow">hash(review_id)<br>⇄</div>')}<div class="exec-l to"><b>after shuffle</b><i class="hit">R038 ×2 → keep 1</i></div></div>` +
        note('Illustration: the two copies start in different partitions and meet after the shuffle.') },
    { t: 'count() again', k: 'action', concept: 'Data quality funnel', rows: [CLN.after_dedup, 'unique reviews'],
      marks: ['n_dedup = dedup.count()'],
      say: `This action runs the shuffle planned in the previous step. <b>${fmt(CLN.reviews_flattened - CLN.after_dedup)}</b> exact duplicates disappear.`,
      view: () => bars([['flattened', CLN.reviews_flattened, '#9db8fb'], ['after de-dup', CLN.after_dedup]]) + note(`For ${H.hotel_id}: ${DEMO.n_reviews} rows → ${DEMO.n_reviews - ROWS.filter(x => x.dup).length} rows.`) },
    { t: 'Repair the type: cast("double")', k: 'lazy', concept: 'Validity · type drift',
      marks: ['rating = F.col("rating").cast("double")', '.withColumn("rating_raw", F.col("rating"))'],
      say: `The string rating is cast to a number. Text that is not a number (<code>"N/A"</code>) becomes <b>null</b> instead of an error. The original value is kept in <code>rating_raw</code> so the change can be audited.`,
      view: () => tbl(['review_id', 'raw JSON', 'bronze (string)', 'cast("double")'], KEPT.map(x => ({ cells: [x.r.review_id, showRaw(x), showStr(x), { v: showNum(x.cast), c: x.cast == null ? 'bad' : typeof x.raw === 'string' ? 'chg' : '' }] }))) },
    { t: 'Validate the range: 1 to 5', k: 'lazy', concept: 'Accuracy · outliers',
      marks: ['.withColumn("rating", F.when(rating.between(1, 5), rating))'],
      say: `<code>F.when(...)</code> without <code>otherwise</code> returns null, so impossible values such as 0, 11 or -1 are set to null. The review is kept because its text is still useful for sentiment.`,
      view: () => tbl(['review_id', 'cast', 'between(1, 5)?', 'rating'], KEPT.map(x => ({ cells: [x.r.review_id, showNum(x.cast), x.cast == null ? '<span class="muted">-</span>' : x.rating != null ? '✓' : { v: '✗', c: 'bad' }, { v: showNum(x.rating), c: x.rating == null ? 'bad' : 'good' }] }))) +
        note(`Across the whole dataset <b>${fmt(CLN.rating_missing_or_invalid)}</b> ratings end up null (missing key, null, "N/A" or out of range).`) },
    { t: 'Parse two date formats', k: 'lazy', concept: 'Consistency',
      marks: ['d1 = F.to_date("date", "yyyy-MM-dd")', '.withColumn("review_date", F.coalesce(d1, d2))'],
      say: `Each pattern is tried in turn. <code>coalesce</code> takes the first that worked, so <code>20/05/2024</code> becomes <code>2024-05-20</code>. Afterwards <b>${CLN.date_unparsed}</b> dates are left unparsed.`,
      view: () => tbl(['review_id', 'date (string)', 'yyyy-MM-dd', 'dd/MM/yyyy', 'review_date'], KEPT.map(x => ({ cells: [x.r.review_id, esc(x.r.date), x.d1 || { v: 'null', c: 'muted' }, x.d2 || { v: 'null', c: 'muted' }, { v: x.date || 'null', c: x.d2 ? 'chg' : '' }] }))) },
    { t: 'Tidy: trim text, has_tags, drop date', k: 'lazy', concept: 'Silver = clean & typed',
      marks: ['.withColumn("text", F.trim(F.col("text")))', '.withColumn("has_tags"', '.drop("date"))'],
      say: `Final touches. The result is one clean, typed row per review: the <b>silver</b> layer. Orange cells were repaired in this stage.`,
      view: () => tbl(['review_id', 'city', 'rating_raw', 'rating', 'review_date', 'has_tags', 'text'], KEPT.map(x => ({ cells: [x.r.review_id, esc(H.location.city), showStr(x), { v: showNum(x.rating), c: x.rating == null ? 'bad' : typeof x.raw === 'string' ? 'chg' : '' }, { v: x.date, c: x.d2 ? 'chg' : '' }, String(!!x.r.tags), esc(short(x.r.text, 26))] }))) },
    { t: 'Write silver, partitioned by city', k: 'write', concept: 'Partition pruning', rows: [CLN.silver_rows, 'silver rows'], lake: 'silver',
      marks: ['silver.write.mode("overwrite").partitionBy("city").parquet(SILVER)'],
      say: `<code>partitionBy("city")</code> writes one folder per city. A query with <code>WHERE city = 'Ella'</code> opens only that folder and skips the rest (<b>partition pruning</b>).`,
      view: () => tree([[0, '<b>lake/silver/</b>'], ...Object.entries(D.replay.silver_by_city).map(([c, n]) => [1, `city=${esc(c)}/`, `${fmt(n)} rows · part-*.parquet`, c === H.location.city ? 'hl' : ''])]) +
        bars(Object.entries(D.replay.silver_by_city).map(([c, n]) => [c, n, c === H.location.city ? 'var(--brand)' : '#9db8fb'])) + note(`${H.hotel_id} lives in <code>city=${esc(H.location.city)}</code>. Row counts come from the Python replay, which matches the pipeline's ${fmt(CLN.silver_rows)} silver rows.`) },
    { t: 'Re-read and save quality stats', k: 'action', concept: 'Data quality funnel', rows: [CLN.silver_rows, 'silver rows'],
      marks: ['s = spark.read.parquet(SILVER)', 'stats = {', '"rating_missing_or_invalid"', '"date_unparsed"', '"silver_rows"', 'print(json.dumps(stats', 'json.dump(stats'],
      say: `Reading the written files back proves what actually landed on disk. Each <code>count()</code> is a small job.`,
      view: () => box(`${icon('file', 'sm')} output/dq_clean.json`, `<div class="code xs">${esc(JSON.stringify(CLN, null, 1))}</div>`) +
        bars([['flattened', CLN.reviews_flattened, '#9db8fb'], ['after de-dup', CLN.after_dedup, '#7fa1f9'], ['valid rating', CLN.after_dedup - CLN.rating_missing_or_invalid]]) },
  ],
  /* ===== 03_features.py ===== */
  [
    { t: 'Read silver', k: 'lazy', concept: 'Medallion: silver → gold', rows: [CLN.silver_rows, 'review rows'],
      marks: ['s = spark.read.parquet(SILVER)'],
      say: `Gold starts from clean silver rows. From here on, no row needs repairing, only enriching.`,
      view: () => tbl(['review_id', 'rating', 'review_date', 'text'], KEPT.map(x => ({ cells: [x.r.review_id, { v: showNum(x.rating), c: x.rating == null ? 'bad' : '' }, x.date, esc(x.r.text)] }))) },
    { t: 'Tokenise: lower → strip → split', k: 'lazy', concept: 'Text preprocessing',
      marks: ['words = F.split(F.regexp_replace(F.lower("text")'],
      say: `Three column functions turn free text into a list of words. They run inside the JVM on every executor, with no Python UDF.`,
      view: () => {
        const t = MIXED.r.text.trim(), lo = t.toLowerCase(), st = lo.replace(/[^a-z\s]/g, ''), w = st.split(/\s+/);
        return `<div class="tok">${[['text', esc(t)], ['lower()', esc(lo)], ['regexp_replace([^a-z\\s], "")', esc(st)]].map(([f, v], i) => `<div class="tok-row pop" style="animation-delay:${i * 250}ms"><span>${f}</span><code>${v}</code></div>`).join('')}
          <div class="tok-row pop" style="animation-delay:750ms"><span>split(\\s+)</span><div class="chips2">${w.map((x, i) => `<b class="pop" style="animation-delay:${900 + i * 90}ms">${esc(x)}</b>`).join('')}</div></div></div>` + note(`Review ${MIXED.r.review_id} from ${esc(H.name)}`);
      } },
    { t: 'Count words and lexicon hits', k: 'lazy', concept: 'Lexicon features',
      marks: ['.withColumn("words", words)', '.withColumn("n_words"', '.withColumn("pos_hits"', '.withColumn("neg_hits"'],
      say: `Each word is looked up in two small lists of ${POS.length} positive and ${NEG.length} negative words. <code>F.filter</code> keeps the matches and <code>size</code> counts them.`,
      view: () => {
        const w = MIXED.r.text.trim().toLowerCase().replace(/[^a-z\s]/g, '').split(/\s+/);
        return `<div class="chips2 big">${w.map(x => `<b class="${POS.includes(x) ? 'p' : NEG.includes(x) ? 'n' : ''}">${esc(x)}</b>`).join('')}</div>` +
          tbl(['n_words', 'pos_hits', 'neg_hits'], [{ cells: [MIXED.f.words, { v: MIXED.f.p.length + ' · ' + MIXED.f.p.join(', '), c: 'good' }, { v: MIXED.f.n.length + ' · ' + MIXED.f.n.join(', '), c: 'bad' }] }]) +
          `<div class="lex2">${POS.map(x => `<span class="p">${x}</span>`).join('')}${NEG.map(x => `<span class="n">${x}</span>`).join('')}</div>`;
      } },
    { t: 'Score sentiment and label it', k: 'lazy', concept: 'Feature engineering',
      marks: ['.withColumn("sentiment"', '.withColumn("label"'],
      say: `sentiment = (pos_hits − neg_hits) / n_words. Above 0 is positive, below 0 negative, exactly 0 neutral. Simple, explainable and fast, and later validated against the star ratings.`,
      view: () => `<div class="formula">(${MIXED.f.p.length} − ${MIXED.f.n.length}) / ${MIXED.f.words} = <b>${MIXED.f.s >= 0 ? '+' : ''}${MIXED.f.s.toFixed(3)}</b> → <span class="chip ${MIXED.f.label === 'positive' ? 'brand' : MIXED.f.label === 'negative' ? 'bad' : ''}">${MIXED.f.label}</span></div>` +
        tbl(['review_id', 'text', 'pos', 'neg', 'sentiment', 'label'], KEPT.map(x => ({ cells: [x.r.review_id, esc(short(x.r.text, 30)), x.f.p.length, x.f.n.length, (x.f.s >= 0 ? '+' : '') + x.f.s.toFixed(3), { v: x.f.label, c: x.f.label === 'positive' ? 'good' : x.f.label === 'negative' ? 'bad' : '' }] }))) },
    { t: 'Date parts: year, month, weekday', k: 'lazy', concept: 'Time features',
      marks: ['.withColumn("year"', '.withColumn("weekday"', '.drop("words"))'],
      say: `Grouping by month or weekday later is much easier with ready-made columns. The temporary <code>words</code> array is dropped before writing.`,
      view: () => tbl(['review_id', 'review_date', 'year', 'month', 'weekday'], KEPT.map(x => ({ cells: [x.r.review_id, x.date, x.date.slice(0, 4), x.date.slice(0, 7), x.weekday] }))) },
    { t: 'Write gold/reviews', k: 'write', concept: 'Medallion: gold', rows: [CLN.silver_rows, 'feature rows'], lake: 'gold',
      marks: ['f.write.mode("overwrite").parquet(f"{GOLD}/reviews")'],
      say: `The first action in this script. Spark executes every column expression above in a <b>single pass</b> over the data and writes the review-level feature table.`,
      view: () => tree([[0, '<b>lake/gold/</b>'], [1, 'reviews/', `${fmt(CLN.silver_rows)} rows · silver columns + n_words, pos_hits, neg_hits, sentiment, label, year, month, weekday`, 'hl'], [1, 'hotel_stats/', 'next'], [1, 'city_month/', 'next']]) },
    { t: 'Keep only rated reviews', k: 'lazy', concept: 'Missing data handling', rows: [SUM.n_rated_reviews, 'rated reviews'],
      marks: ['rated = f.filter(F.col("rating").isNotNull())'],
      say: `Rating averages can only use reviews that have a rating. Unrated reviews stay in <code>gold/reviews</code> for text analysis, so nothing is thrown away early.`,
      view: () => tbl(['review_id', 'rating', 'label'], KEPT.map(x => ({ cls: x.rating == null ? 'gone' : '', cells: [x.r.review_id, showNum(x.rating), x.f.label] }))) + bars([['all reviews', CLN.silver_rows, '#9db8fb'], ['rated', SUM.n_rated_reviews]]) },
    { t: 'groupBy(hotel) → hotel_stats', k: 'write', wide: true, concept: 'Aggregation · shuffle', lake: 'gold',
      marks: ['hotel = (rated.groupBy(', '.alias("avg_rating"),', 'F.round(F.avg("sentiment"), 4).alias("avg_sentiment"),', 'pct_positive', 'F.max("review_date")', 'hotel.write'],
      say: `All rated reviews of a hotel are shuffled to one partition and folded into a single row. Each executor first builds partial sums locally, so only small partial results cross the network.`,
      view: () => {
        const s = DEMO.hotel_stats;
        return ba(box(`${s.n_reviews} rated reviews of ${H.hotel_id}`, `<div class="stackrows">${Array.from({ length: Math.min(s.n_reviews, 55) }, (_, i) => `<i style="animation-delay:${i * 15}ms"></i>`).join('')}</div>`),
          box('1 row in gold/hotel_stats', tbl(['column', 'value', 'how'], [['n_reviews', s.n_reviews, 'count(*)'], ['avg_rating', s.avg_rating, 'avg(rating)'], ['avg_sentiment', s.avg_sentiment, 'avg(sentiment)'],
            ['pct_positive', s.pct_positive, 'avg(label = positive)'], ['last_review', s.last_review, 'max(review_date)']].map(r => ({ cells: [r[0], { v: `<b>${r[1]}</b>`, c: 'good' }, `<code>${r[2]}</code>`] })))), 'groupBy') +
          note('These are real values for this hotel from the Python replay. The same replay reproduces every row of output/top_hotels.csv.');
      } },
    { t: 'groupBy(city, month) → city_month', k: 'write', wide: true, concept: 'Aggregation · time series', lake: 'gold',
      marks: ['citym = (rated.groupBy("city", "month")', 'citym.write'],
      say: `The second aggregate counts reviews and averages ratings per city per month. It feeds the "reviews per month" trend.`,
      view: () => {
        const cm = DEMO.city_month, max = Math.max(...cm.map(r => r.n_reviews));
        return tbl(['city', 'month', 'n_reviews', 'avg_rating'], cm.slice(0, 5).map(r => ({ cells: [esc(r.city), r.month, r.n_reviews, r.avg_rating] }))) +
          `<div class="muted xmore">… ${cm.length - 5} more months for ${esc(H.location.city)}</div><div class="spark">${cm.map(r => `<i title="${r.month}: ${r.n_reviews}" style="height:${100 * r.n_reviews / max}%"></i>`).join('')}</div>`;
      } },
    { t: 'Log what was written', k: 'action', concept: 'Row-count discipline',
      marks: ['print("gold tables written:"'],
      say: `Three more counts confirm the sizes of the gold tables.`,
      view: () => `<div class="code">$ python pipeline/03_features.py\ngold tables written: ${CLN.silver_rows} review rows; … hotels; … city-months</div>` +
        tree([[0, '<b>lake/gold/</b>'], [1, 'reviews/', fmt(CLN.silver_rows) + ' rows'], [1, 'hotel_stats/', 'one row per hotel'], [1, 'city_month/', 'one row per city × month']]) },
  ],
  /* ===== 04_analytics.py ===== */
  [
    { t: 'Load gold tables, keep rated reviews', k: 'lazy', concept: 'Descriptive analytics', rows: [SUM.n_rated_reviews, 'rated reviews'],
      marks: ['r = spark.read.parquet(f"{GOLD}/reviews")', 'h = spark.read.parquet', 'cm = spark.read.parquet'],
      say: `Analytics reads only from gold, never from raw. The three tables are opened lazily.`,
      view: () => tree([[0, '<b>lake/gold/</b>'], [1, 'reviews/', `→ r (${fmt(SUM.n_rated_reviews)} rated)`], [1, 'hotel_stats/', '→ h'], [1, 'city_month/', '→ cm']]) },
    { t: 'approxQuantile: Q1, median, Q3', k: 'action', concept: 'Approximate algorithms',
      marks: ['q1, med, q3 = r.approxQuantile'],
      say: `Exact quantiles need a full sort, which is expensive at scale. <code>approxQuantile(..., 0.01)</code> uses a compact sketch (Greenwald–Khanna) that is guaranteed within 1% rank error and needs a single pass.`,
      view: () => {
        const rd = D.rating_dist, max = Math.max(...rd.map(r => r.count)), mark = v => v === SUM.q1 ? 'Q1' : v === SUM.median ? 'median' : v === SUM.q3 ? 'Q3' : '';
        return `<div class="qbars">${rd.map(r => `<div class="${mark(r.rating) ? 'm' : ''}"><i style="height:${100 * r.count / max}%"></i><span>${r.rating.toFixed(1)}</span>${mark(r.rating) ? `<em>${mark(r.rating)}</em>` : ''}</div>`).join('')}</div>` +
          tbl(['Q1', 'median', 'Q3', 'mean'], [{ cells: [SUM.q1, SUM.median, SUM.q3, SUM.mean_rating] }]);
      } },
    { t: 'Mean and correlation', k: 'action', concept: 'Model validation',
      marks: ['summary = {"n_rated_reviews"', '"q1": q1', 'corr_rating_sentiment', 'print(json.dumps(summary'],
      say: `Pearson correlation between star rating and lexicon sentiment is <b>r = ${SUM.corr_rating_sentiment}</b>, strong evidence that the simple text feature from step 3 tracks real opinion.`,
      view: () => bars(['negative', 'neutral', 'positive'].filter(l => SENT[l]).map(l => [`${l} reviews · avg ★`, SENT[l].avg_rating, l === 'negative' ? '#e0703f' : l === 'neutral' ? '#98a3b5' : 'var(--brand)'])) +
        box(`${icon('file', 'sm')} output/summary.json`, `<div class="code xs">${esc(JSON.stringify(SUM, null, 1))}</div>`) },
    { t: 'groupBy(city) → city_summary.csv', k: 'collect', wide: true, concept: 'Aggregation',
      marks: ['to_csv(r.groupBy("city")', 'F.round(F.avg("sentiment"), 4).alias("avg_sentiment")).orderBy("city")'],
      say: `${fmt(SUM.n_rated_reviews)} rows are shuffled by city and reduced to ${D.city_summary.length}. Only that tiny result is collected to the driver and written as CSV.`,
      view: () => ba(box('rated reviews', `<div class="big">${fmt(SUM.n_rated_reviews)}</div><div class="muted">rows on the executors</div>`), csvPreview('city_summary.csv', D.city_summary, 6), 'groupBy') },
    { t: 'Four more summaries', k: 'collect', wide: true, concept: 'Serving layer',
      marks: ['rating_dist.csv', 'sentiment_vs_rating.csv', 'monthly_volume.csv', 'top_hotels.csv'],
      say: `Rating distribution, sentiment vs rating, monthly volume and the top-10 hotels (with at least 30 rated reviews). Each is a small table the chart step can load without Spark.`,
      view: () => `<div class="grid2">${csvPreview('rating_dist.csv', D.rating_dist, 3)}${csvPreview('sentiment_vs_rating.csv', D.sentiment, 3)}${csvPreview('monthly_volume.csv', D.monthly_volume, 3)}${csvPreview('top_hotels.csv', D.top_hotels.map(h => ({ hotel_id: h.hotel_id, city: h.city, n: h.n_reviews, avg_rating: h.avg_rating })), 3)}</div>` },
    { t: 'to_csv: collect() to the driver', k: 'collect', concept: 'Driver vs executors', script: 'common.py',
      marks: ['def to_csv', 'rows = df.collect()', 'w = csv.writer(f)'],
      say: `<code>collect()</code> copies every row of a DataFrame into the driver's memory. That is fine for a ${D.city_summary.length}-row summary and would crash on the full ${fmt(CLN.silver_rows)}-row table. Rule: aggregate on the cluster, collect only the small result.`,
      view: () => note(`${icon('alert', 'sm')} Never call <code>collect()</code> or <code>toPandas()</code> on a big table.`, 'warn') + `<div class="code">rows = df.collect()          <span class="c"># executors → driver</span>\ncsv.writer(f).writerows(rows) <span class="c"># driver → output/*.csv</span></div>` },
  ],
  /* ===== 05_dashboard.py ===== */
  [
    { t: 'Plain Python on the host', k: 'host', concept: 'Serving / visualisation',
      marks: ['import pandas as pd, matplotlib', 'matplotlib.use("Agg")', 'def save(name)'],
      say: `The charts don't need a cluster: the inputs are a handful of tiny CSVs. pandas reads them and matplotlib draws PNGs. On Docker this step runs on your laptop, outside the containers.`,
      view: () => tree([[0, '<b>output/</b>'], ...['dq_ingest.json', 'dq_clean.json', 'summary.json', 'city_summary.csv', 'rating_dist.csv', 'sentiment_vs_rating.csv', 'monthly_volume.csv', 'top_hotels.csv'].map(f => [1, f, 'input'])]) },
    { t: 'Data-quality funnel chart', k: 'host', concept: 'Communicating data quality',
      marks: ['dq = {**json.load', 'steps = [("Flattened', '("Valid\\nrating"', 'ax.bar([s[0] for s in steps]', 'for i, s in enumerate(steps)', 'chart_dq_funnel.png'],
      say: `The funnel is built from the two JSON files the Spark steps saved, the row-count discipline paying off.`,
      view: () => ba(box(`${icon('file', 'sm')} dq_ingest.json + dq_clean.json`, `<div class="code xs">${esc(JSON.stringify({ ...ING, ...CLN }, null, 1))}</div>`), png('dq_funnel'), 'plot') },
    { t: 'Average rating by city', k: 'host', concept: 'Comparison',
      marks: ['c = pd.read_csv', 'ax.barh(c.city', 'chart_city_rating.png'],
      say: `city_summary.csv → horizontal bar chart. Note <code>set_xlim(3, 4.5)</code>: a truncated axis exaggerates differences, a good discussion point.`,
      view: () => ba(csvPreview('city_summary.csv', D.city_summary, 6), png('city_rating'), 'plot') },
    { t: 'Reviews per month', k: 'host', concept: 'Time series',
      marks: ['m = pd.read_csv', 'iloc[:-1]', 'ax.plot(m.month', 'ax.set_xticks', 'chart_monthly_volume.png'],
      say: `The last month in the data is only partly covered, so it is dropped with <code>iloc[:-1]</code>. Otherwise the line would plunge at the end for no real reason.`,
      view: () => ba(box(`${icon('file', 'sm')} monthly_volume.csv (last rows)`, tbl(['month', 'n_reviews'], D.monthly_volume.slice(-4).map((r, i, a) => ({ cls: i === a.length - 1 ? 'gone' : '', cells: [r.month, r.n_reviews] })))), png('monthly_volume'), 'plot') },
    { t: 'Rating distribution', k: 'host', concept: 'Distribution',
      marks: ['d = pd.read_csv', 'ax.bar(d.rating', 'chart_rating_dist.png'],
      say: `rating_dist.csv → bar chart of the half-star buckets. The median (${SUM.median}) is the tallest bar.`,
      view: () => ba(csvPreview('rating_dist.csv', D.rating_dist, 9), png('rating_dist'), 'plot') },
    { t: 'Mean rating by sentiment label', k: 'host', concept: 'Validation chart',
      marks: ['s = pd.read_csv', 'ax.bar(s.index', 'chart_sentiment.png', 'print("charts written")'],
      say: `The picture version of r = ${SUM.corr_rating_sentiment}: negative-labelled reviews average ★${SENT.negative?.avg_rating}, positive ones ★${SENT.positive?.avg_rating}.`,
      view: () => ba(csvPreview('sentiment_vs_rating.csv', D.sentiment, 3), png('sentiment'), 'plot') },
  ],
];
const STEP_META = [
  { title: 'Ingest: raw JSON → bronze', from: 'data/raw', to: 'lake/bronze + quarantine' },
  { title: 'Clean & flatten: bronze → silver', from: 'lake/bronze', to: 'lake/silver' },
  { title: 'Features: silver → gold', from: 'lake/silver', to: 'lake/gold' },
  { title: 'Analytics: gold → summaries', from: 'lake/gold', to: 'output/*.csv' },
  { title: 'Serve: summaries → charts', from: 'output/*.csv', to: 'output/*.png' },
];
const KIND = {
  lazy: ['Transformation', 'lazy - added to the plan', 'brand'], action: ['Action', 'runs a Spark job', 'good'], write: ['Action · write', 'job writes files', 'good'],
  collect: ['Action · collect', 'rows go to the driver', 'warn'], host: ['Host Python', 'no Spark', ''],
};

/* ---------- Spark execution mini-diagram ---------- */
function planBefore(si, oi) {     // lazy ops waiting for the next action
  const pend = [];
  for (let i = 0; i <= oi; i++) { const o = OPS[si][i]; if (o.k === 'lazy') pend.push(o); else if (i < oi) pend.length = 0; }
  return pend;
}
function sparkView(si, oi) {
  const op = OPS[si][oi], pend = planBefore(si, oi), runs = op.k !== 'lazy' && op.k !== 'host';
  const shuffle = runs && (op.wide || pend.some(p => p.wide));
  const parts = (e, cls) => [0, 1, 2, 3].map(p => `<rect class="part ${cls}" x="${24 + e * 150 + p * 26}" y="98" width="20" height="20" rx="4" style="animation-delay:${(p + e * 2) * 120}ms"/>`).join('');
  let s = `<rect x="110" y="8" width="100" height="34" rx="9" class="drv ${op.k === 'host' || op.k === 'collect' ? 'on' : ''}"/><text x="160" y="24" text-anchor="middle" class="t1">driver</text><text x="160" y="36" text-anchor="middle" class="t2">${op.k === 'host' ? 'python + pandas' : 'your script'}</text>`;
  [0, 1].forEach(e => {
    s += `<rect x="${14 + e * 150}" y="76" width="124" height="54" rx="9" class="exe ${op.k === 'host' ? 'off' : ''}"/><text x="${76 + e * 150}" y="90" text-anchor="middle" class="t2">executor ${e + 1}</text>`;
    s += parts(e, runs ? (shuffle ? 'run s1' : 'run') : '');
    s += `<path class="wire ${runs ? 'on' : ''}" d="M160 42 L${76 + e * 150} 76"/>`;
  });
  if (shuffle) s += `<path class="xch" d="M128 108 C 150 92, 150 92, 172 108"/><path class="xch" d="M172 116 C 150 132, 150 132, 128 116"/><text x="150" y="146" text-anchor="middle" class="t2 xt2">shuffle</text>`;
  if (op.k === 'write') s += `<path class="wire on dn" d="M76 130 L140 160 M226 130 L180 160"/><rect x="110" y="158" width="100" height="22" rx="6" class="lake"/><text x="160" y="173" text-anchor="middle" class="t2 w">lake/${op.lake}</text>`;
  if (op.k === 'collect') s += `<path class="wire on up" d="M76 76 L130 42 M226 76 L190 42"/><text x="160" y="173" text-anchor="middle" class="t2">small result → driver → CSV</text>`;
  const cap = op.k === 'lazy' ? (op.wide ? 'Adds a <b>shuffle boundary</b> to the plan. Nothing runs yet.' : 'Added to the logical plan. <b>Nothing runs yet.</b>')
    : op.k === 'host' ? 'Runs on the host in plain Python. The cluster is not used.'
    : `Spark runs a <b>job</b>${shuffle ? ' in <b>2 stages</b> split by a shuffle' : ''}${pend.length ? ` covering ${pend.length} queued transformation${pend.length > 1 ? 's' : ''}` : ''}.`;
  const planChips = op.k === 'host' ? '' : `<div class="plan"><span class="muted">${op.k === 'lazy' ? 'plan so far' : pend.length ? 'executes' : 'plan'}:</span>${(op.k === 'lazy' ? pend : [...pend, op]).map((p, i, a) => `<b class="${p === op ? 'cur' : ''} ${p.wide ? 'wide' : ''}">${esc(p.t.split(':')[0].split(' (')[0].split(' →')[0])}</b>${i < a.length - 1 ? '→' : ''}`).join('')}</div>`;
  return `<svg viewBox="0 0 320 186" class="sparksvg k-${op.k}">${s}</svg><div class="spcap">${cap}</div>${planChips}`;
}

/* ---------- script view with highlighted lines ---------- */
function codeView(src, marks) {
  const KW = /\b(from|import|def|return|for|in|if|else|lambda|with|as|open|print)\b/g;
  return src.split('\n').map((line, i) => {
    const hl = marks.some(m => line.includes(m));
    const html = line.split(/("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|#.*$)/).map((tok, j) =>
      j % 2 ? `<span class="${tok.startsWith('#') ? 'c' : 's'}">${esc(tok)}</span>` : esc(tok).replace(KW, '<span class="k">$1</span>')).join('');
    return `<div class="ln ${hl ? 'hl' : ''}"><span class="no">${i + 1}</span><span>${html || ' '}</span></div>`;
  }).join('');
}

/* ---------- explorer UI ---------- */
const X = { si: 0, oi: 0, playing: false, timer: 0, done: null, guided: false };
const el = document.createElement('div');
el.className = 'modal xp'; el.id = 'explorer'; el.hidden = true;
el.innerHTML = `<div class="xp-sheet" role="dialog" aria-modal="true" aria-labelledby="xp-title">
  <header class="xp-head"><div class="ph-icon" id="xp-icon"></div><div class="xp-tt"><div class="eyebrow" id="xp-eyebrow"></div><h2 id="xp-title"></h2></div>
    <div class="xp-steps" id="xp-steps"></div><button class="icon-btn" id="xp-close" aria-label="Close">${icon('x')}</button></header>
  <nav class="xp-rail" id="xp-rail"></nav>
  <div class="xp-main">
    <section class="card xp-data"><div class="xp-op" id="xp-op"></div><div class="xp-view" id="xp-view"></div></section>
    <aside class="xp-side">
      <div class="xp-row2"><div class="card xp-count" id="xp-count"></div><div class="card xp-spark" id="xp-spark"></div></div>
      <div class="card xp-code"><div class="xp-code-h" id="xp-code-h"></div><div class="xp-code-b" id="xp-code"></div></div>
    </aside>
  </div>
  <footer class="xp-foot">
    <button class="btn" id="xp-prev">← Previous</button>
    <button class="btn primary" id="xp-play"></button>
    <button class="btn" id="xp-next">Next →</button>
    <div class="xp-prog" id="xp-prog"></div>
    <span class="muted xp-keys">← → to step · space to play · Esc to close</span>
  </footer></div>`;
document.body.appendChild(el);

function rowsAt(si, oi) {       // most recent known row count up to this op
  for (let i = oi; i >= 0; i--) if (OPS[si][i].rows) return { v: OPS[si][i].rows, changed: i === oi };
  return null;
}
function render() {
  const ops = OPS[X.si], op = ops[X.oi], step = RF.STEPS[X.si], meta = STEP_META[X.si];
  $('#xp-icon').innerHTML = icon(['layers', 'filter', 'sparkle', 'chart', 'chart'][X.si]);
  $('#xp-icon').style.setProperty('--c', ['#c27c45', '#8692a8', '#d29d18', '#2f66f6', '#2f66f6'][X.si]);
  $('#xp-eyebrow').textContent = `Inside step ${X.si + 1} of 5 · pipeline/${step.script} · ${meta.from} → ${meta.to}`;
  $('#xp-title').textContent = meta.title;
  $('#xp-steps').innerHTML = RF.STEPS.map((s, i) => `<button class="${i === X.si ? 'on' : ''}" data-si="${i}" title="${s.script}">${i + 1}<span>${s.label}</span></button>`).join('');
  $('#xp-rail').innerHTML = ops.map((o, i) => `<button class="xr ${i === X.oi ? 'on' : ''} ${i < X.oi ? 'past' : ''} k-${o.k}" data-oi="${i}"><i>${i + 1}</i><span>${esc(o.t)}</span>${o.wide ? '<em>shuffle</em>' : ''}</button>`).join('');
  $('#xp-rail .on')?.scrollIntoView({ block: 'nearest', inline: 'center', behavior: 'smooth' });
  const [kl, kd, kc] = KIND[op.k];
  $('#xp-op').innerHTML = `<div class="xp-op-t"><span class="xp-n">${X.oi + 1}/${ops.length}</span><h3>${esc(op.t)}</h3></div>
    <div class="chips"><span class="chip ${kc}">${kl}</span><span class="chip">${kd}</span>${op.wide ? '<span class="chip warn">wide · shuffle</span>' : ''}<span class="chip brand">${icon('sparkle', 'sm')} ${op.L ? `L${String(op.L).padStart(2, '0')} · ` : ''}${esc(op.concept)}</span></div><p class="xp-say">${op.say}</p>`;
  $('#xp-view').innerHTML = op.view();
  const rc = rowsAt(X.si, X.oi), prev = X.oi ? rowsAt(X.si, X.oi - 1) : null;
  $('#xp-count').innerHTML = `<div class="eyebrow">Rows at this point</div>${rc ? `<div class="xp-big ${rc.changed ? 'chg' : ''}">${fmt(rc.v[0])}</div><div class="muted">${rc.v[1]}</div>
    ${rc.changed && prev && prev.v[0] !== rc.v[0] ? `<span class="chip ${rc.v[0] > prev.v[0] ? 'brand' : 'warn'}">${rc.v[0] > prev.v[0] ? '▲' : '▼'} from ${fmt(prev.v[0])}</span>` : ''}` : `<div class="xp-big">—</div><div class="muted">${op.k === 'host' ? 'small files, no Spark' : 'not counted yet'}</div>`}
    <div class="xp-funnel">${OPS[X.si].filter(o => o.rows).map(o => `<i title="${esc(o.t)}: ${fmt(o.rows[0])}" class="${o === op ? 'on' : ''}" style="height:${18 + 82 * Math.log10(o.rows[0]) / Math.log10(CLN.reviews_flattened)}%"></i>`).join('')}</div>`;
  $('#xp-spark').innerHTML = `<div class="eyebrow">What Spark does</div>${sparkView(X.si, X.oi)}`;
  const file = op.script || step.script;
  $('#xp-code-h').innerHTML = `${icon('file', 'sm')} <b>pipeline/${file}</b><span class="muted">highlighted = this operation</span>`;
  const code = $('#xp-code');
  code.innerHTML = codeView(D.scripts[file] || '', op.marks);
  const first = code.querySelector('.hl');
  if (first) code.scrollTo({ top: first.offsetTop - code.clientHeight / 3, behavior: 'smooth' });
  $('#xp-prev').disabled = X.si === 0 && X.oi === 0;
  const last = X.oi === ops.length - 1;
  $('#xp-next').innerHTML = last ? (X.si < 4 ? `Next step: ${RF.STEPS[X.si + 1].label} →` : 'Finish ✓') : 'Next →';
  $('#xp-play').innerHTML = X.playing ? `${icon('stop', 'sm')} Pause` : `${icon('play', 'sm')} Play`;
  $('#xp-prog').innerHTML = `<i style="width:${100 * (X.oi + 1) / ops.length}%"></i>`;
}
function go(si, oi) { X.si = si; X.oi = oi; render(); if (X.playing) schedule(); }
function next() {
  const ops = OPS[X.si];
  if (X.oi < ops.length - 1) return go(X.si, X.oi + 1);
  if (X.guided) return finish();
  if (X.si < 4) return go(X.si + 1, 0);
  close();
}
function prev() { if (X.oi > 0) go(X.si, X.oi - 1); else if (X.si > 0) go(X.si - 1, OPS[X.si - 1].length - 1); }
function schedule() { clearTimeout(X.timer); X.timer = setTimeout(next, 5200 / (RF.state.speed || 1)); }
function setPlay(p) { X.playing = p; clearTimeout(X.timer); if (p) schedule(); render(); }
function finish() { const d = X.done; X.done = null; X.guided = false; clearTimeout(X.timer); X.playing = false; el.hidden = true; d?.(); }
function close() { finish(); }
function open(si = 0, oi = 0, play = false) {
  X.guided = false; X.done = null;
  el.hidden = false; X.playing = play; go(si, oi); if (play) schedule();
  $('#xp-close').focus({ preventScroll: true });
}
// guided mode for the run: plays one step's operations, resolves when done or closed
function playStep(si) {
  return new Promise(res => { X.guided = true; X.done = res; el.hidden = false; X.playing = true; go(si, 0); schedule(); });
}

el.addEventListener('click', e => {
  if (e.target === el) return close();
  const t = e.target.closest('[data-si],[data-oi]');
  if (t?.dataset.si) { X.guided = false; go(+t.dataset.si, 0); }
  else if (t?.dataset.oi) go(X.si, +t.dataset.oi);
});
$('#xp-close').addEventListener('click', close);
$('#xp-next').addEventListener('click', next);
$('#xp-prev').addEventListener('click', prev);
$('#xp-play').addEventListener('click', () => setPlay(!X.playing));
document.addEventListener('keydown', e => {
  if (el.hidden || /input|select|textarea/i.test(document.activeElement.tagName)) return;
  if (e.key === 'ArrowRight') { e.preventDefault(); next(); }
  if (e.key === 'ArrowLeft') { e.preventDefault(); prev(); }
  if (e.key === ' ') { e.preventDefault(); setPlay(!X.playing); }
  if (e.key === 'Escape') close();
});

Object.assign(RF, { openExplorer: open, closeExplorer: close, playStep, OPS });
RF.select?.(RF.state.selected);   // re-render the panel so its 'look inside' buttons show operation counts
if (location.hash.startsWith('#explore')) {            // #explore2 opens step 2, #explore2.5 opens its 5th operation
  const [a, b] = location.hash.slice(8).split('.').map(Number);
  const si = Math.max(0, Math.min(4, (a || 1) - 1));
  open(si, Math.max(0, Math.min(OPS[si].length - 1, (b || 1) - 1)));
}
})();
