// Control panel behaviour: buttons, status polling, the Flink job summary, the traffic chart and the top videos.
import {decodeTraffic} from './traffic-metrics.mjs';

const REFRESH_MS = 2000;
const CHART_HEIGHT = 260;
const CHART_SECONDS = 900;  // 15 minutes.
// [line key, color]: the first three come from the simulator's samples, the last from Flink's history.
const SERIES = [
  ['expected', '#7464a2'],
  ['acked', '#47719b'],
  ['attempted', '#a96565'],
  ['flink', '#3f9a6b'],
];

const $ = selector => document.querySelector(selector);
const number = value => Number(value || 0).toLocaleString();
let latest = {traffic: [], flink: {history: []}};

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
  $('#parallelism').value = status.parallelism;
  $('#parallelism').disabled = status.rescaling;
  $('#late').value = status.late_percent;
  $('#burst').disabled = !status.running;
  $('#burst').textContent = status.burst_seconds_left ? `Spiking · ${status.burst_seconds_left}s` : 'Spike ×10';
  $('#play-icon').className = 'icon ' + (live ? 'pause-icon' : 'play-icon');
  $('#play').setAttribute('aria-label', live ? 'Pause traffic' : status.running ? 'Resume traffic' : 'Start everything');
}

function showFlink(job) {
  const element = $('#flink');
  if (job.state !== 'RUNNING') {
    element.textContent = job.state === 'NO_JOB' ? 'Flink is up · no job running' : '';
    return;
  }
  // A task that is almost always busy, sustained backpressure, or growing lag is the signal to raise parallelism.
  const warn = (value, threshold) => (value >= threshold ? ' class="warn"' : '');
  element.innerHTML = `
    <a href="http://localhost:8081" target="_blank" rel="noopener">Flink</a>
    <span>parallelism <strong>${job.parallelism}</strong> on <strong>${job.taskmanagers}</strong> TaskManager${job.taskmanagers === 1 ? '' : 's'}</span>
    <span>consumed <strong>${number(job.records_per_second)}</strong> rec/s</span>
    <span${warn(job.pending_records, 10000)}>lag <strong>${number(job.pending_records)}</strong></span>
    <span${warn(job.busy_percent, 80)}>busiest task <strong>${job.busy_percent}%</strong></span>
    <span${warn(job.backpressure_percent, 10)}>backpressure <strong>${job.backpressure_percent}%</strong></span>
    <span${warn(job.late_records_dropped, 1)}>late dropped <strong>${number(job.late_records_dropped)}</strong></span>`;
}

function showTopVideos(top) {
  const list = $('#top-list');
  if (!top.window_start) {
    list.replaceChildren();
    return;
  }
  $('#top-window').textContent = `minute ${top.window_start.slice(11, 16)} · ${number(top.total)} clicks in total`;
  const max = Math.max(1, ...top.videos.map(video => video.click_count));
  list.replaceChildren(...top.videos.map(video => {
    const item = document.createElement('li');
    const name = document.createElement('span');
    const bar = document.createElement('div');
    const count = document.createElement('span');
    name.textContent = video.videoid;
    bar.className = 'bar';
    bar.style.width = `${(100 * video.click_count) / max}%`;
    count.className = 'count';
    count.textContent = number(video.click_count);
    item.append(name, bar, count);
    return item;
  }));
}

// Every line as [[epoch seconds, value], ...], so the simulator's and Flink's samples share a time axis.
function chartLines(traffic, flinkHistory) {
  const lines = {};
  for (const key of ['expected', 'acked', 'attempted']) {
    lines[key] = traffic.map(sample => [Date.parse(sample.wall_time) / 1000, Number(sample[key] || 0)]);
  }
  lines.flink = flinkHistory.map(([time, recordsPerSecond]) => [time, recordsPerSecond]);
  return lines;
}

function drawTraffic(traffic, flinkHistory) {
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
  // The axis follows the simulator's samples, which restart with each run.  Flink's history outlives runs
  // (the panel keeps sampling), so it's clipped to the same range rather than stretching the axis.
  const lines = chartLines(traffic, traffic.length ? flinkHistory : []);
  const trafficTimes = lines.expected.map(([time]) => time);
  const end = Math.max(...Object.values(lines).flat().map(([time]) => time), 0);
  const start = Math.max(end - CHART_SECONDS, Math.min(...trafficTimes, end));
  const visible = Object.fromEntries(Object.entries(lines).map(([key, line]) => [key, line.filter(([time]) => time >= start)]));
  const max = Math.max(1, ...Object.values(visible).flat().map(([, value]) => value));
  const x = time => left + (end === start ? plotW / 2 : (plotW * (time - start)) / (end - start));

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
    if (!visible[key].length) continue;
    ctx.strokeStyle = color;
    ctx.lineWidth = 2;
    ctx.beginPath();
    visible[key].forEach(([time, value], index) => {
      const y = top + plotH * (1 - value / max);
      index ? ctx.lineTo(x(time), y) : ctx.moveTo(x(time), y);
    });
    ctx.stroke();
  }

  const recent = traffic.slice(-10);
  const rps = recent.length ? Math.round(recent.reduce((sum, sample) => sum + Number(sample.acked || 0), 0) / recent.length) : 0;
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

// Posts {key: number} to path whenever the <select> changes.
function onSelect(selector, path, key, describe) {
  $(selector).addEventListener('change', async event => {
    try {
      await post(path, {[key]: Number(event.target.value)});
      if (describe) toast(describe(event.target.value));
    } catch (error) {
      toast(error.message);
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
onClick('#burst', async () => { await post('/api/burst'); });
onSelect('#speed', '/api/speed', 'speed');
onSelect('#parallelism', '/api/parallelism', 'parallelism');
onSelect('#late', '/api/late', 'late_percent', value => (Number(value) ? `${value}% of clicks will arrive 5 simulated minutes late` : 'Late clicks off'));

async function refresh() {
  try {
    showStatus(await getJson('/api/status'));
    const [traffic, flink, top] = await Promise.all([getTraffic(), getJson('/api/flink'), getJson('/api/top-videos')]);
    latest = {traffic, flink};
    drawTraffic(traffic, flink.history);
    showFlink(flink);
    showTopVideos(top);
  } catch (_) {
    $('#message').textContent = 'Backend unavailable';
    $('#dot').className = 'dot';
  }
}

refresh();
setInterval(refresh, REFRESH_MS);
window.addEventListener('resize', () => drawTraffic(latest.traffic, latest.flink.history));
