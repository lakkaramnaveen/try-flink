// Control panel behaviour: buttons, status polling, the Flink job summary, and the traffic chart.
import {decodeTraffic} from './traffic-metrics.mjs';

const REFRESH_MS = 2000;
const CHART_HEIGHT = 260;
const CHART_SAMPLES = 900;  // 15 minutes at one sample per second.
const SERIES = [
  ['expected', '#7464a2'],
  ['acked', '#47719b'],
  ['attempted', '#a96565'],
];

const $ = selector => document.querySelector(selector);

function toast(message) {
  const element = $('#toast');
  element.textContent = message;
  element.classList.add('show');
  setTimeout(() => element.classList.remove('show'), 2600);
}

async function getJson(path) {
  const response = await fetch(path, {cache: 'no-store'});
  return response.json();
}

async function getTraffic() {
  const response = await fetch('/api/traffic', {cache: 'no-store'});
  if (!response.ok) throw Error('Could not read traffic samples');
  return decodeTraffic(await response.arrayBuffer());
}

async function post(path, body) {
  const response = await fetch(path, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body || {}),
  });
  const data = await response.json();
  if (!response.ok) throw Error(data.error || data.message || 'Request failed');
  showStatus(data);
  return data;
}

function showStatus(status) {
  const live = status.running && !status.paused;
  $('#message').textContent = status.message;
  $('#dot').className = 'dot ' + (status.running ? (status.paused ? 'paused' : 'live') : '');
  $('#speed').value = status.speed;
  $('#play-icon').className = 'icon ' + (live ? 'pause-icon' : 'play-icon');
  $('#play').setAttribute('aria-label', live ? 'Pause traffic' : status.running ? 'Resume traffic' : 'Start everything');
}

function showFlink(job) {
  const element = $('#flink');
  if (job.state !== 'RUNNING') {
    element.textContent = job.state === 'NO_JOB' ? 'Flink is up · no job running' : '';
    return;
  }
  const number = value => value.toLocaleString();
  // A task that is almost always busy, or sustained backpressure, is the signal to raise parallelism or simplify the query.
  const warn = (percent, threshold) => (percent >= threshold ? ' class="warn"' : '');
  element.innerHTML = `
    <a href="http://localhost:8081" target="_blank" rel="noopener">Flink</a>
    <span>parallelism <strong>${job.parallelism}</strong></span>
    <span>consumed <strong>${number(job.records_per_second)}</strong> rec/s</span>
    <span${warn(job.busy_percent, 80)}>busiest task <strong>${job.busy_percent}%</strong></span>
    <span${warn(job.backpressure_percent, 10)}>backpressure <strong>${job.backpressure_percent}%</strong></span>`;
}

function drawTraffic(samples) {
  const canvas = $('#traffic-chart');
  const ratio = window.devicePixelRatio || 1;
  const width = Math.max(canvas.getBoundingClientRect().width, 320);
  const height = CHART_HEIGHT;
  canvas.width = width * ratio;
  canvas.height = height * ratio;
  const ctx = canvas.getContext('2d');
  ctx.scale(ratio, ratio);
  ctx.clearRect(0, 0, width, height);

  const left = 42, right = 14, top = 18, bottom = 28;
  const plotW = width - left - right, plotH = height - top - bottom;
  const visible = samples.slice(-CHART_SAMPLES);
  const value = (sample, key) => Number(sample[key] || 0);
  const max = Math.max(1, ...visible.map(sample => Math.max(...SERIES.map(([key]) => value(sample, key)))));

  // Horizontal grid lines with y-axis labels.
  ctx.strokeStyle = '#e7ecec';
  ctx.fillStyle = '#7d8588';
  ctx.font = '12px system-ui';
  ctx.textAlign = 'right';
  for (let i = 0; i <= 3; i++) {
    const y = top + plotH * i / 3;
    ctx.beginPath();
    ctx.moveTo(left, y);
    ctx.lineTo(width - right, y);
    ctx.stroke();
    ctx.fillText(Math.round(max * (3 - i) / 3), left - 8, y + 4);
  }

  for (const [key, color] of SERIES) {
    if (!visible.length) break;
    ctx.strokeStyle = color;
    ctx.lineWidth = 2;
    ctx.beginPath();
    visible.forEach((sample, index) => {
      const x = left + (visible.length === 1 ? plotW / 2 : plotW * index / (visible.length - 1));
      const y = top + plotH * (1 - value(sample, key) / max);
      index ? ctx.lineTo(x, y) : ctx.moveTo(x, y);
    });
    ctx.stroke();
  }

  const recent = visible.slice(-10);
  const rps = recent.length ? Math.round(recent.reduce((sum, sample) => sum + value(sample, 'acked'), 0) / recent.length) : 0;
  $('#current-rps').textContent = `${rps} RPS`;
}

// Wraps a button's click handler so the button is disabled while its request is in flight.
function onClick(selector, handler) {
  const button = $(selector);
  button.addEventListener('click', async () => {
    button.disabled = true;
    try {
      await handler();
    } catch (error) {
      toast(error.message);
    } finally {
      button.disabled = false;
    }
  });
}

onClick('#play', async () => {
  const status = await getJson('/api/status');
  if (status.running) await post('/api/traffic', {paused: !status.paused});
  else await post('/api/start');
});
onClick('#stop', async () => { await post('/api/stop'); toast('Stopped'); });
onClick('#restart', async () => { await post('/api/restart'); toast('Restarted'); });

$('#speed').addEventListener('change', async event => {
  try {
    await post('/api/speed', {speed: Number(event.target.value)});
  } catch (error) {
    toast(error.message);
  }
});

async function refresh() {
  try {
    showStatus(await getJson('/api/status'));
    drawTraffic(await getTraffic());
    showFlink(await getJson('/api/flink'));
  } catch (_) {
    $('#message').textContent = 'Backend unavailable';
    $('#dot').className = 'dot';
  }
}

refresh();
setInterval(refresh, REFRESH_MS);
window.addEventListener('resize', async () => {
  try {
    drawTraffic(await getTraffic());
  } catch (error) {
    toast(error.message);
  }
});
