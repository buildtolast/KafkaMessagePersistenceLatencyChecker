Write the dashboard's single static HTML page.

ABSOLUTE OUTPUT RULE: your ENTIRE reply is ONE ```html fenced code block containing
the complete file, with NO text and NO additional code blocks before or after it.

Output path: dashboard-service/src/main/resources/static/index.html

A dark-theme comparison dashboard, "INLINE vs CLAIM_CHECK" Kafka payload paths.
Max 120 lines. Vanilla HTML + one embedded <style> block. NO external CSS, NO CDN.
Scripts at end of body, in this order:
  <script src="vendor/chart.umd.js"></script>
  <script src="app.js"></script>

Layout top to bottom (ids EXACTLY as given — app.js depends on them):

1. Header bar: <header> with title "Claim-Check Latency Dashboard", a live
   indicator <span id="live-dot"> (styled as a 10px round dot, green when class
   "on", gray otherwise) and a <button id="pause-btn">Pause</button>.

2. Hero comparison strip: a 3-column grid <section id="hero">:
   - left card (INLINE): <div class="path-card"> with <h2>INLINE</h2>,
     headline <div class="big" id="inline-p95">–</div> labeled "e2e p95 (ms)",
     subline <div id="inline-rate">–</div>.
   - center: <div id="delta-badge"><div class="big" id="delta-ms">–</div>
     <div id="delta-pct">–</div></div> labeled "claim-check overhead".
   - right card (CLAIM_CHECK): same structure with ids "cc-p95", "cc-rate".

3. Percentiles chart: <section><canvas id="lat-chart" height="90"></canvas></section>

4. "Where the time goes": <section id="segments">
   two rows, each: <div class="seg-row"><span class="seg-label">INLINE</span>
   <div class="seg-bar" id="seg-inline"></div></div> and the same with label
   CLAIM_CHECK and id "seg-cc". Below them a storage line:
   <div id="mongo-storage">Mongo: – docs, – GB</div>.
   .seg-bar is a horizontal flexbox; app.js fills it with colored
   <span class="seg"> children whose flex-grow is the segment ms value.

5. Cluster status strip: <footer id="cluster-strip">–</footer> single line.

Styling: dark background (#111418), light text (#e6e6e6), cards #1b2027 with
1px #2a2f36 border and 8px radius, accent colors: INLINE #4dabf7,
CLAIM_CHECK #f59f00. .big is 2.2rem bold. Keep CSS terse.
