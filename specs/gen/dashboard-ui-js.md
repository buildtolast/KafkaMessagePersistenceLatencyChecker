Write the dashboard's client script.

ABSOLUTE OUTPUT RULE: your ENTIRE reply is ONE ```javascript fenced code block
containing the complete file, with NO text and NO additional code blocks before
or after it.

Output path: dashboard-service/src/main/resources/static/app.js
Max 170 lines. Vanilla ES6, no modules, no frameworks. Chart is the global from
vendor/chart.umd.js. The page (index.html) provides these element ids:
live-dot, pause-btn, inline-p95, inline-rate, cc-p95, cc-rate, inline-spark,
cc-spark, delta-ms, delta-pct, lat-chart, seg-inline, seg-cc, mongo-storage,
cluster-strip.

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
   - hero: inline-p95 / cc-p95 = `${e2eP95Ms.toFixed(1)} ms`; inline-rate /
     cc-rate = `${msgPerSec.toFixed(1)} msg/s · ${mbPerSec.toFixed(2)} MB/s`.
   - delta badge: dMs = cc.e2eP95Ms - inline.e2eP95Ms → delta-ms text
     `+${dMs.toFixed(1)} ms` ; delta-pct `+${pct.toFixed(0)}% p95` where
     pct = inline.e2eP95Ms > 0 ? dMs / inline.e2eP95Ms * 100 : 0.
   - sparklines: keep rolling arrays (last 60 p95 values per path). Draw each on
     its canvas (inline-spark #3b7dd8, cc-spark #7048e8) with the raw 2D context
     (NOT Chart.js): clear, autoscale to min/max with 2px padding, single 2px
     polyline across the full width. Skip drawing until 2+ points.
   - chart: rolling window of the last 150 points (5 min at 2 s). One Chart.js
     line chart on #lat-chart, 4 datasets: "INLINE p50" (#3b7dd8 solid),
     "INLINE p99" (#3b7dd8 dashed [5,5]), "CLAIM p50" (#7048e8 solid),
     "CLAIM p99" (#7048e8 dashed). Labels are `new Date().toLocaleTimeString()`.
     animation:false, pointRadius:0, y title "ms", x ticks maxTicksLimit 8,
     legend labels color #495057. Push+shift arrays then chart.update('none').
   - segments: fill seg-inline with parts [send, proc] and seg-cc with parts
     [insert, send, fetch, proc] where send=kafkaSendAvgMs, insert=mongoInsertAvgMs,
     fetch=mongoFetchAvgMs, proc=processingAvgMs. Helper `renderSegs(el, parts)`,
     parts = array of {label, ms, color}; each span gets style
     `flex-grow:${Math.max(ms,0.1)};background:${color}`, title
     `${label} ${ms.toFixed(1)} ms`, textContent `${label} ${ms.toFixed(0)}` when
     ms >= 12% of the row total, else `${ms.toFixed(0)}` when ms >= 4%, else empty.
     Colors: send #4c86dd, insert #7048e8, fetch #9775fa, proc #74c0fc.
   - mongo-storage: `${mongoDocs.toLocaleString()} docs · ${(mongoBytes/1e9).toFixed(1)} GB`.
   - cluster-strip: rebuild as chips (innerHTML). Each instance → one
     `<span class="chip">` containing `${up ? '✓' : '✗'} ${shortName} ${msgPerSec.toFixed(1)}/s`
     where shortName strips "http://" and the port (e.g. "consumer1"). Append
     ` · lag ${lag}` and add class "warn" when lag > 0; add class "warn" when
     up is false too.
4. Everything wrapped in an IIFE. No console.log.
