(() => {
  let paused = false;
  const es = new EventSource('/api/stream');
  const ctx = document.getElementById('lat-chart').getContext('2d');
  const chart = new Chart(ctx, {
    type: 'line',
    data: { labels: [], datasets: [
      { label: 'INLINE p50', borderColor: '#4dabf7', borderWidth: 2, data: [], tension: 0.1, pointRadius: 0, borderDash: [] },
      { label: 'INLINE p99', borderColor: '#4dabf7', borderWidth: 2, data: [], tension: 0.1, pointRadius: 0, borderDash: [5, 5] },
      { label: 'CLAIM p50', borderColor: '#f59f00', borderWidth: 2, data: [], tension: 0.1, pointRadius: 0, borderDash: [] },
      { label: 'CLAIM p99', borderColor: '#f59f00', borderWidth: 2, data: [], tension: 0.1, pointRadius: 0, borderDash: [5, 5] }
    ]},
    options: { animation: false, responsive: true, scales: { y: { title: { display: true, text: 'ms', color: '#e6e6e6' } }, x: { ticks: { maxTicksLimit: 8, color: '#e6e6e6' } } }, plugins: { legend: { labels: { color: '#e6e6e6' } } } }
  });

  const getP = (obj) => obj || { msgPerSec: 0, mbPerSec: 0, e2eP50Ms: 0, e2eP95Ms: 0, e2eP99Ms: 0, mongoInsertAvgMs: 0, kafkaSendAvgMs: 0, mongoFetchAvgMs: 0, processingAvgMs: 0 };
  const renderSegs = (el, parts) => {
    const total = parts.reduce((s, p) => s + p.ms, 0);
    el.innerHTML = '';
    parts.forEach(p => {
      const span = document.createElement('span');
      span.style.flexGrow = p.ms;
      span.style.backgroundColor = p.color;
      span.title = `${p.label} ${p.ms.toFixed(1)} ms`;
      if (total > 0 && p.ms / total >= 0.15) span.textContent = `${p.label} ${p.ms.toFixed(1)}`;
      el.appendChild(span);
    });
  };

  es.addEventListener('open', () => document.getElementById('live-dot').classList.add('on'));
  es.addEventListener('error', () => document.getElementById('live-dot').classList.remove('on'));
  document.getElementById('pause-btn').onclick = (e) => {
    paused = !paused;
    e.target.textContent = paused ? 'Resume' : 'Pause';
  };

  es.addEventListener('stats', (e) => {
    if (paused) return;
    const d = JSON.parse(e.data);
    const i = getP(d.byPath?.INLINE), c = getP(d.byPath?.CLAIM_CHECK);
    
    document.getElementById('inline-p95').textContent = i.e2eP95Ms.toFixed(1);
    document.getElementById('cc-p95').textContent = c.e2eP95Ms.toFixed(1);
    document.getElementById('inline-rate').textContent = `${i.msgPerSec.toFixed(1)} msg/s · ${i.mbPerSec.toFixed(2)} MB/s`;
    document.getElementById('cc-rate').textContent = `${c.msgPerSec.toFixed(1)} msg/s · ${c.mbPerSec.toFixed(2)} MB/s`;

    const dMs = c.e2eP95Ms - i.e2eP95Ms;
    const pct = i.e2eP95Ms > 0 ? (dMs / i.e2eP95Ms) * 100 : 0;
    document.getElementById('delta-ms').textContent = `${dMs >= 0 ? '+' : ''}${dMs.toFixed(1)} ms`;
    document.getElementById('delta-pct').textContent = `${pct >= 0 ? '+' : ''}${pct.toFixed(0)}% p95`;

    const time = new Date().toLocaleTimeString();
    chart.data.labels.push(time);
    chart.data.datasets[0].data.push(i.e2eP50Ms);
    chart.data.datasets[1].data.push(i.e2eP99Ms);
    chart.data.datasets[2].data.push(c.e2eP50Ms);
    chart.data.datasets[3].data.push(c.e2eP99Ms);
    if (chart.data.labels.length > 150) {
      chart.data.labels.shift();
      chart.data.datasets.forEach(ds => ds.data.shift());
    }
    chart.update('none');

    renderSegs(document.getElementById('seg-inline'), [
      { label: 'KAFKA', ms: i.kafkaSendAvgMs, color: '#748ffc' },
      { label: 'PROC', ms: i.processingAvgMs, color: '#63e6be' }
    ]);
    renderSegs(document.getElementById('seg-cc'), [
      { label: 'INS', ms: c.mongoInsertAvgMs, color: '#f59f00' },
      { label: 'KAFKA', ms: c.kafkaSendAvgMs, color: '#748ffc' },
      { label: 'FETCH', ms: c.mongoFetchAvgMs, color: '#ffd43b' },
      { label: 'PROC', ms: c.processingAvgMs, color: '#63e6be' }
    ]);

    document.getElementById('mongo-storage').textContent = `Mongo: ${d.mongoDocs} docs, ${(d.mongoBytes / 1e9).toFixed(2)} GB`;
    document.getElementById('cluster-strip').textContent = (d.instances || []).map(inst => 
      `${inst.up ? '●' : '○'} ${inst.name} ${inst.msgPerSec.toFixed(1)} msg/s${inst.lag > 0 ? ` ⚠ lag ${inst.lag}` : ''}`
    ).join(' | ');
  });
})();
