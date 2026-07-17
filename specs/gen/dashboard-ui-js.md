Write the dashboard's client script.

ABSOLUTE OUTPUT RULE: your ENTIRE reply is ONE ```javascript fenced code block
containing the complete file, with NO text and NO additional code blocks before
or after it.

Output path: dashboard-service/src/main/resources/static/app.js
Max 150 lines. Vanilla ES6, no modules, no frameworks. Chart is the global from
vendor/chart.umd.js. The page (index.html) provides these element ids:
live-dot, pause-btn, inline-p95, inline-rate, cc-p95, cc-rate, delta-ms,
delta-pct, lat-chart, seg-inline, seg-cc, mongo-storage, cluster-strip.

Server contract:
- SSE endpoint `/api/stream` emits events with EVENT NAME "stats" — you MUST use
  `es.addEventListener('stats', handler)`, NOT onmessage. Each event's data is
  JSON:
  `{ byPath: { INLINE: P, CLAIM_CHECK: P }, instances: [{name, up, msgPerSec, lag}], mongoDocs, mongoBytes }`
  where P = `{ msgPerSec, mbPerSec, e2eP50Ms, e2eP95Ms, e2eP99Ms, mongoInsertAvgMs, kafkaSendAvgMs, mongoFetchAvgMs, processingAvgMs }`.
- byPath may be missing either key early on; guard with a default P of all zeros.

Behavior:
1. `const es = new EventSource('/api/stream');` on open → live-dot gets class
   "on"; on error → class removed.
2. A `paused` boolean; pause-btn toggles it and its own label Pause/Resume.
   When paused, incoming events are ignored (connection stays open).
3. On each stats event (when not paused):
   - hero: inline-p95 / cc-p95 = e2eP95Ms with 1 decimal; inline-rate / cc-rate =
     `${msgPerSec.toFixed(1)} msg/s · ${mbPerSec.toFixed(2)} MB/s`.
   - delta badge: dMs = cc.e2eP95Ms - inline.e2eP95Ms → delta-ms text
     `+${dMs.toFixed(1)} ms` ; delta-pct `+${pct.toFixed(0)}% p95` where
     pct = inline.e2eP95Ms > 0 ? dMs / inline.e2eP95Ms * 100 : 0.
   - chart: rolling window of the last 150 points (5 min at 2 s). One Chart.js
     line chart on #lat-chart, 4 datasets: "INLINE p50" (#4dabf7 solid),
     "INLINE p99" (#4dabf7 dashed [5,5]), "CLAIM p50" (#f59f00 solid),
     "CLAIM p99" (#f59f00 dashed). Labels are `new Date().toLocaleTimeString()`.
     animation:false, pointRadius:0, y title "ms", x ticks maxTicksLimit 8,
     legend labels color #e6e6e6. Push+shift arrays then chart.update('none').
   - segments: fill seg-inline with spans for [kafkaSendAvgMs, processingAvgMs]
     and seg-cc with [mongoInsertAvgMs, kafkaSendAvgMs, mongoFetchAvgMs,
     processingAvgMs]. Build with a helper `renderSegs(el, parts)` where parts is
     an array of {label, ms, color}; each span gets style
     `flex-grow:${ms};background:${color}`, title `${label} ${ms.toFixed(1)} ms`,
     and textContent `${label} ${ms.toFixed(1)}` only when ms is at least 15% of
     the row total (else empty). Colors: kafka-send #748ffc, processing #63e6be,
     mongo-insert #f59f00, mongo-fetch #ffd43b.
   - mongo-storage: `Mongo: ${mongoDocs} docs, ${(mongoBytes/1e9).toFixed(2)} GB`.
   - cluster-strip: join instances with " | ": each as
     `${up ? '●' : '○'} ${name} ${msgPerSec.toFixed(1)} msg/s` plus
     ` ⚠ lag ${lag}` appended only when lag > 0.
4. Everything wrapped in an IIFE. No console.log.
