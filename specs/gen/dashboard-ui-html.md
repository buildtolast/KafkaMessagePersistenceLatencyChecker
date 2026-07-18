Write the dashboard's single static HTML page.

ABSOLUTE OUTPUT RULE: your ENTIRE reply is ONE ```html fenced code block containing
the complete file, with NO text and NO additional code blocks before or after it.

Output path: dashboard-service/src/main/resources/static/index.html

A LIGHT-theme comparison dashboard, "INLINE vs CLAIM_CHECK" Kafka payload paths.
Max 140 lines. Vanilla HTML + one embedded <style> block. NO external CSS, NO CDN.
Scripts at end of body, in this order:
  <script src="vendor/chart.umd.js"></script>
  <script src="app.js"></script>

Layout top to bottom (ids EXACTLY as given — app.js depends on them):

1. Header bar: <header> with left side title block (<h1>Claim check vs inline</h1>
   and a subtitle <div class="sub">2 MB pairs · live comparison · window 5 min</div>)
   and right side: <span id="live-dot"></span><span class="live-label">live</span>
   inside a pill <div id="live-pill">, plus <button id="pause-btn">Pause</button>.
   live-dot is a 10px round dot, green (#2f9e44) when it has class "on", gray otherwise;
   live-pill has a pale green background (#e6f7ec) and rounded-full corners.

2. Hero comparison strip <section id="hero">, 3-column grid, center column narrow:
   - left card: <div class="path-card"><h2 class="inline-color">INLINE · Kafka only</h2>
     <div class="big" id="inline-p95">–</div>
     <div class="metric-label">e2e p95 · <span id="inline-rate">–</span></div>
     <canvas id="inline-spark" height="28"></canvas></div>
   - center: <div id="delta-badge"><div class="delta-label">overhead</div>
     <div class="big" id="delta-ms">–</div><div id="delta-pct">–</div></div>
     delta-ms is dark red (#c92a2a) on a pale red rounded background (#fdecec).
   - right card: same structure as left with heading "CLAIM CHECK · Kafka + Mongo"
     (class cc-color) and ids "cc-p95", "cc-rate", "cc-spark".

3. Two-column section: left <section id="chart-panel"> with <h3>Latency percentiles</h3>
   <div class="hint">p50 solid · p99 dashed</div> and
   <canvas id="lat-chart" height="120"></canvas>;
   right <aside id="time-panel"> with <h3>Where the time goes</h3>, then
   two rows, each: <div class="seg-row"><span class="seg-label inline-color">inline</span>
   <div class="seg-bar" id="seg-inline"></div></div> and the same with label
   "claim check" (class cc-color) and id "seg-cc"; then
   <div class="hint">bar width ∝ avg ms per stage</div>; then a storage card
   <div id="mongo-storage-card">stored in Mongo <strong id="mongo-storage">–</strong></div>.
   .seg-bar is a horizontal flexbox with 28px height; app.js fills it with colored
   <span class="seg"> children whose flex-grow is the segment ms value; .seg text is
   white, 0.72rem, centered, nowrap, hidden on overflow.

4. Cluster status strip: <footer><span class="strip-label">cluster</span>
   <span id="cluster-strip"></span></footer>. app.js fills cluster-strip with
   <span class="chip"> elements: pale gray background (#f1f3f5), rounded-full,
   0.8rem, inline-flex with 6px gap. A chip may carry class "warn" → pale amber
   background (#fff3bf).

Styling: background #f7f8fa, text #212529, cards white with 1px #e3e6ea border,
10px radius and a subtle shadow. Accents: INLINE #3b7dd8, CLAIM_CHECK #7048e8.
.big is 2.6rem bold. Headings h2 0.95rem uppercase-free. Keep CSS terse.
