(() => {
  const zeroP = () => ({ msgPerSec: 0, mbPerSec: 0, e2eP50Ms: 0, e2eP95Ms: 0, e2eP99Ms: 0,
    mongoInsertAvgMs: 0, kafkaSendAvgMs: 0, mongoFetchAvgMs: 0, hopLatencyAvgMsByStage: {} });
  const getP = (obj) => obj || zeroP();

  let paused = false;
  const es = new EventSource('/api/stream');
  const liveDot = document.getElementById('live-dot');
  const pauseBtn = document.getElementById('pause-btn');

  es.addEventListener('open', () => liveDot.classList.add('on'));
  es.addEventListener('error', () => liveDot.classList.remove('on'));
  pauseBtn.onclick = () => {
    paused = !paused;
    pauseBtn.textContent = paused ? 'Resume' : 'Pause';
  };

  const ctx = document.getElementById('lat-chart').getContext('2d');
  const chart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: [],
      datasets: [
        { label: 'INLINE p50', borderColor: '#3b7dd8', borderWidth: 2, borderDash: [], data: [], pointRadius: 0 },
        { label: 'INLINE p99', borderColor: '#3b7dd8', borderWidth: 2, borderDash: [5, 5], data: [], pointRadius: 0 },
        { label: 'CLAIM p50', borderColor: '#7048e8', borderWidth: 2, borderDash: [], data: [], pointRadius: 0 },
        { label: 'CLAIM p99', borderColor: '#7048e8', borderWidth: 2, borderDash: [5, 5], data: [], pointRadius: 0 }
      ]
    },
    options: {
      animation: false,
      responsive: true,
      scales: {
        y: { title: { display: true, text: 'ms', color: '#495057' } },
        x: { ticks: { maxTicksLimit: 8, color: '#495057' } }
      },
      plugins: { legend: { labels: { color: '#495057' } } }
    }
  });

  const sparkData = { INLINE: [], CLAIM_CHECK: [] };
  const drawSpark = (canvasId, values, color) => {
    const canvas = document.getElementById(canvasId);
    const c = canvas.getContext('2d');
    const w = canvas.clientWidth || canvas.width;
    const h = canvas.height;
    canvas.width = w;
    c.clearRect(0, 0, w, h);
    if (values.length < 2) return;
    const min = Math.min(...values), max = Math.max(...values);
    const range = max - min || 1;
    const pad = 2;
    const step = (w - pad * 2) / (values.length - 1);
    c.beginPath();
    c.strokeStyle = color;
    c.lineWidth = 2;
    values.forEach((v, idx) => {
      const x = pad + idx * step;
      const y = pad + (1 - (v - min) / range) * (h - pad * 2);
      if (idx === 0) c.moveTo(x, y); else c.lineTo(x, y);
    });
    c.stroke();
  };

  const renderSegs = (el, parts) => {
    const total = parts.reduce((s, p) => s + p.ms, 0) || 1;
    el.innerHTML = '';
    parts.forEach((p) => {
      const span = document.createElement('span');
      span.className = 'seg';
      span.style.cssText = `flex-grow:${Math.max(p.ms, 0.1)};background:${p.color}`;
      span.title = `${p.label} ${p.ms.toFixed(1)} ms`;
      const ratio = p.ms / total;
      span.textContent = ratio >= 0.12 ? `${p.label} ${p.ms.toFixed(0)}` : ratio >= 0.04 ? `${p.ms.toFixed(0)}` : '';
      el.appendChild(span);
    });
  };

  const shortName = (name) => name.replace(/^https?:\/\//, '').replace(/:\d+$/, '');

  const renderLane = (el, hopLatencyAvgMsByStage, color) => {
    const stages = Object.keys(hopLatencyAvgMsByStage || {})
      .map(Number)
      .sort((a, b) => a - b);
    el.innerHTML = '';
    if (stages.length === 0) {
      el.innerHTML = '<span class="wf-empty">no hops yet</span>';
      return;
    }
    const max = Math.max(...stages.map((s) => hopLatencyAvgMsByStage[s]), 0.001);
    stages.forEach((stage) => {
      const ms = hopLatencyAvgMsByStage[stage];
      const div = document.createElement('div');
      div.className = 'lane-stage';
      const intensity = Math.min(1, ms / max);
      div.style.background = color;
      div.style.opacity = String(0.35 + intensity * 0.65);
      div.title = `stage ${stage}: ${ms.toFixed(1)} ms`;
      div.textContent = ms >= 1 ? ms.toFixed(0) : '';
      el.appendChild(div);
    });
  };

  const renderWaterfall = (el, trace, color) => {
    el.innerHTML = '';
    if (!trace || !trace.hopTrace || trace.hopTrace.length === 0) {
      el.innerHTML = '<span class="wf-empty">no sampled trace yet</span>';
      return;
    }
    const hops = trace.hopTrace;
    const origin = hops[0].consumedAtEpochNanos;
    const durations = hops.map((h, idx) => {
      const end = h.publishedAtEpochNanos != null ? h.publishedAtEpochNanos : h.consumedAtEpochNanos;
      const next = hops[idx + 1];
      const nextStart = next ? next.consumedAtEpochNanos : end;
      return Math.max(nextStart - h.consumedAtEpochNanos, 1);
    });
    hops.forEach((h, idx) => {
      const div = document.createElement('div');
      div.className = 'wf-hop';
      div.style.background = color;
      div.style.opacity = String(0.4 + (idx / hops.length) * 0.6);
      div.style.flexGrow = String(durations[idx]);
      const ms = durations[idx] / 1_000_000;
      div.title = `stage ${h.stage}: +${ms.toFixed(1)} ms`;
      el.appendChild(div);
    });
  };

  es.addEventListener('stats', (e) => {
    if (paused) return;
    const d = JSON.parse(e.data);
    const i = getP(d.byPath && d.byPath.INLINE);
    const c = getP(d.byPath && d.byPath.CLAIM_CHECK);

    document.getElementById('inline-p95').textContent = `${i.e2eP95Ms.toFixed(1)} ms`;
    document.getElementById('cc-p95').textContent = `${c.e2eP95Ms.toFixed(1)} ms`;
    document.getElementById('inline-rate').textContent = `${i.msgPerSec.toFixed(1)} msg/s · ${i.mbPerSec.toFixed(2)} MB/s`;
    document.getElementById('cc-rate').textContent = `${c.msgPerSec.toFixed(1)} msg/s · ${c.mbPerSec.toFixed(2)} MB/s`;

    const dMs = c.e2eP95Ms - i.e2eP95Ms;
    const pct = i.e2eP95Ms > 0 ? (dMs / i.e2eP95Ms) * 100 : 0;
    const sign = dMs >= 0 ? '+' : '';
    document.getElementById('delta-ms').textContent = `${sign}${dMs.toFixed(1)} ms`;
    document.getElementById('delta-pct').textContent = `${sign}${pct.toFixed(0)}% p95`;

    sparkData.INLINE.push(i.e2eP95Ms);
    sparkData.CLAIM_CHECK.push(c.e2eP95Ms);
    if (sparkData.INLINE.length > 60) sparkData.INLINE.shift();
    if (sparkData.CLAIM_CHECK.length > 60) sparkData.CLAIM_CHECK.shift();
    drawSpark('inline-spark', sparkData.INLINE, '#3b7dd8');
    drawSpark('cc-spark', sparkData.CLAIM_CHECK, '#7048e8');

    const time = new Date().toLocaleTimeString();
    chart.data.labels.push(time);
    chart.data.datasets[0].data.push(i.e2eP50Ms);
    chart.data.datasets[1].data.push(i.e2eP99Ms);
    chart.data.datasets[2].data.push(c.e2eP50Ms);
    chart.data.datasets[3].data.push(c.e2eP99Ms);
    if (chart.data.labels.length > 150) {
      chart.data.labels.shift();
      chart.data.datasets.forEach((ds) => ds.data.shift());
    }
    chart.update('none');

    renderSegs(document.getElementById('seg-inline'), [
      { label: 'send', ms: i.kafkaSendAvgMs, color: '#4c86dd' }
    ]);
    renderSegs(document.getElementById('seg-cc'), [
      { label: 'insert', ms: c.mongoInsertAvgMs, color: '#7048e8' },
      { label: 'send', ms: c.kafkaSendAvgMs, color: '#4c86dd' },
      { label: 'fetch', ms: c.mongoFetchAvgMs, color: '#9775fa' }
    ]);

    renderLane(document.getElementById('lanes-inline'), i.hopLatencyAvgMsByStage, '#3b7dd8');
    renderLane(document.getElementById('lanes-cc'), c.hopLatencyAvgMsByStage, '#7048e8');

    renderWaterfall(document.getElementById('wf-inline'), d.waterfall && d.waterfall.inline, '#3b7dd8');
    renderWaterfall(document.getElementById('wf-cc'), d.waterfall && d.waterfall.claimCheck, '#7048e8');

    document.getElementById('mongo-storage').textContent =
      `${(d.mongoDocs || 0).toLocaleString()} docs · ${((d.mongoBytes || 0) / 1e9).toFixed(1)} GB`;

    const chips = (d.instances || []).map((inst) => {
      const warn = inst.lag > 0 || !inst.up;
      let text = `${inst.up ? '✓' : '✗'} ${shortName(inst.name)} ${inst.msgPerSec.toFixed(1)}/s`;
      if (inst.lag > 0) text += ` · lag ${inst.lag}`;
      return `<span class="chip${warn ? ' warn' : ''}">${text}</span>`;
    });
    document.getElementById('cluster-strip').innerHTML = chips.join('');
  });
})();
