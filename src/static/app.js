// Pizza Order Estimator — KDS Frontend
// Polls /api/state every second and updates the DOM.

'use strict';

// ── Pizza status sets ─────────────────────────────────────────────────────────
const MAKELINE_STATUSES = new Set(['QUEUED', 'MAKING']);
const OVEN_STATUSES     = new Set(['WAITING_OVEN', 'BAKING']);

// ── Helpers ───────────────────────────────────────────────────────────────────

function fmtMin(val, opts) {
  if (val === null || val === undefined) return '—';
  const sign = (opts && opts.sign && val > 0) ? '+' : '';
  return `${sign}${val.toFixed(1)}m`;
}

/** Format elapsed sim-minutes as M:SS */
function fmtAge(minutes) {
  const totalSec = Math.floor((minutes || 0) * 60);
  const m = Math.floor(totalSec / 60);
  const s = totalSec % 60;
  return `${m}:${s.toString().padStart(2, '0')}`;
}

/** Badge colour by order age */
function ageBadgeClass(ageMinutes) {
  if (ageMinutes < 10) return 'badge-green';
  if (ageMinutes < 20) return 'badge-yellow';
  return 'badge-red';
}

function el(id) { return document.getElementById(id); }
function setText(id, value) { const n = el(id); if (n) n.textContent = value; }

// ── API call ──────────────────────────────────────────────────────────────────

async function apiPost(path) {
  try {
    const res = await fetch(path, { method: 'POST' });
    if (!res.ok) console.error(`${path} → ${res.status}`);
    fetchState();
  } catch (e) { console.error(e); }
}

async function startDay() {
  try {
    const res = await fetch('/api/day-start', { method: 'POST' });
    if (!res.ok) console.error(`/api/day-start → ${res.status}`);
    fetchState();
  } catch (e) { console.error(e); }
}

// ── Day-complete banner ───────────────────────────────────────────────────────

let _dayCompleteBannerShown = false;

function showDayCompleteBanner() {
  if (_dayCompleteBannerShown) return;
  _dayCompleteBannerShown = true;
  const banner = document.createElement('div');
  banner.className = 'day-complete-banner';
  banner.textContent = '✓ Day simulation complete — check logs/ for the full run report';
  document.body.appendChild(banner);
  setTimeout(() => {
    banner.style.transition = 'opacity 0.5s';
    banner.style.opacity = '0';
    setTimeout(() => banner.remove(), 600);
    _dayCompleteBannerShown = false;
  }, 6000);
}

// ── Pizza pill builder ────────────────────────────────────────────────────────

/** Render one pill per pizza that has left the make line. */
function pizzaPills(items) {
  return (items || [])
    .filter(i => !MAKELINE_STATUSES.has(i.pizza_status))
    .map(i => {
      const done = i.pizza_status === 'DONE';
      return `<span class="pizza-pill ${done ? 'ready' : 'baking'}">${done ? '✓' : '🔥'}</span>`;
    }).join('');
}

// ── KDS card — Incoming panel ─────────────────────────────────────────────────

function renderOrderCard(order) {
  const allItems  = order.items || [];
  const makeItems = allItems.filter(i => MAKELINE_STATUSES.has(i.pizza_status));
  const total     = allItems.length;
  const remaining = makeItems.length;

  const age      = order.age_minutes || 0;
  const badgeCls = ageBadgeClass(age);
  const typeIcon = order.order_type === 'delivery' ? '🛵' : '🏠';

  const hasActive  = makeItems.some(i => i.pizza_status === 'MAKING');
  const statusLabel = hasActive ? 'Making' : 'Queued';
  const sCls        = hasActive ? 'making' : 'queued';

  const countLabel = total > remaining
    ? `${remaining} of ${total} pizza${total !== 1 ? 's' : ''} making`
    : `${remaining} pizza${remaining !== 1 ? 's' : ''}`;

  const itemHtml = makeItems.map(i => {
    const name = i.type === 'Custom' ? `Custom (${i.topping_count} toppings)` : i.name;
    return `<div>${name}</div>`;
  }).join('');

  const estStr = `D: ${fmtMin(order.dynamic_estimate)} / N: ${fmtMin(order.naive_estimate)}`;

  return `
    <div class="kds-card" data-id="${order.order_id}">
      <div class="kds-card-body">
        <div class="kds-card-id">#${order.order_id} <span style="font-weight:400;font-size:11px;color:#555">${typeIcon} ${countLabel}</span></div>
        <div class="kds-card-items">${itemHtml}</div>
        <div class="kds-card-meta">${estStr}</div>
        <div class="kds-card-status ${sCls}">${statusLabel}</div>
      </div>
      <div class="kds-card-badge ${badgeCls}">
        <span class="badge-time">${fmtAge(age)}</span>
        <span class="badge-unit">age</span>
      </div>
    </div>`;
}

// ── KDS card — Oven panel ─────────────────────────────────────────────────────

function renderOvenCard(order, nowMin, ovenTime) {
  const allItems = order.items || [];
  const age      = order.age_minutes || 0;
  const badgeCls = ageBadgeClass(age);
  const typeIcon = order.order_type === 'delivery' ? '🛵' : '🏠';

  // Oven countdown: use the latest oven_end across all baking pizzas
  let ovenRemaining = null;
  if (order.oven_end_max != null) {
    ovenRemaining = order.oven_end_max - nowMin;
  }

  // Status line: check if any pizzas are still waiting for an oven slot
  const waitingForSlot = allItems.some(i => i.pizza_status === 'WAITING_OVEN');
  const statusLabel    = waitingForSlot ? 'Waiting for slot' : 'Baking';
  const sCls           = 'in-oven';

  const itemHtml = allItems.map(i => {
    const name = i.type === 'Custom' ? `Custom (${i.topping_count} toppings)` : i.name;
    const statusIcon = i.pizza_status === 'DONE' ? '✓' : (i.pizza_status === 'BAKING' ? '🔥' : '⏳');
    return `<div>${statusIcon} ${name}</div>`;
  }).join('');

  let timerHtml = '';
  if (ovenRemaining != null && ovenRemaining > 0) {
    timerHtml = `<div class="kds-card-oven-timer">🔥 ${fmtAge(ovenRemaining)} left</div>`;
  } else if (ovenRemaining != null && ovenRemaining <= 0) {
    timerHtml = `<div class="kds-card-oven-timer" style="color:#22c55e">✓ Done — cooling</div>`;
  } else {
    // WAITING_OVEN, no oven_end yet — show estimated bake time
    timerHtml = `<div class="kds-card-oven-timer">⏳ ~${fmtAge(ovenTime || 8)}</div>`;
  }

  return `
    <div class="kds-card oven-card" data-id="${order.order_id}">
      <div class="kds-card-body">
        <div class="kds-card-id">#${order.order_id} <span style="font-weight:400;font-size:11px;color:#555">${typeIcon} ${allItems.length} pizza${allItems.length !== 1 ? 's' : ''}</span></div>
        <div class="kds-card-items">${itemHtml}</div>
        ${timerHtml}
        <div class="kds-card-status ${sCls}">${statusLabel}</div>
      </div>
      <div class="kds-card-badge ${badgeCls}">
        <span class="badge-time">${fmtAge(age)}</span>
        <span class="badge-unit">age</span>
      </div>
    </div>`;
}

// ── Carryout row ──────────────────────────────────────────────────────────────

function renderCarryoutRow(order, nowMin) {
  const age      = order.age_minutes || 0;
  const readyAt  = order.ready_at;
  const timeInReady = readyAt != null ? nowMin - readyAt : null;
  const pickupRemaining = timeInReady != null ? 5.0 - timeInReady : null;

  let countdownHtml = '';
  let opacityStyle  = '';
  let extraCls      = '';

  if (pickupRemaining != null) {
    if (pickupRemaining > 0) {
      countdownHtml = `<div class="row-countdown pickup">Pickup in ${fmtAge(pickupRemaining)}</div>`;
      extraCls = 'pickup-soon';
      // Fade the row as it approaches removal
      if (pickupRemaining < 0.5) {
        const opacity = Math.max(0.15, pickupRemaining / 0.5);
        opacityStyle = `style="opacity:${opacity.toFixed(2)}"`;
      }
    } else {
      countdownHtml = `<div class="row-countdown pickup">✓ Picked up</div>`;
      opacityStyle = `style="opacity:0.3"`;
    }
  }

  return `
    <div class="carryout-row ${extraCls}" data-id="${order.order_id}" ${opacityStyle}>
      <span class="row-id">#${order.order_id}</span>
      <div class="row-meta">
        <div class="row-meta-main">Ready for Pickup · waited ${fmtAge(readyAt != null ? readyAt - order.placed_at : age)}</div>
        ${countdownHtml}
      </div>
      <span class="row-time">${fmtAge(age)}</span>
    </div>`;
}

// ── Delivery row ──────────────────────────────────────────────────────────────

function renderDeliveryRow(order, drivers, nowMin) {
  const age    = order.age_minutes || 0;
  const driver = (drivers || []).find(d => d.current_order_id === order.order_id);
  const isDelivered = order.status === 'Delivered';

  let driverBadge   = '';
  let countdownHtml = '';

  if (isDelivered) {
    driverBadge   = `<span class="row-driver" style="background:#1a3a1a;color:#22c55e;border-color:#166534">DONE</span>`;
    countdownHtml = `<div class="row-countdown pickup">Delivered</div>`;
  } else if (driver && driver.status === 'delivering') {
    driverBadge = `<span class="row-driver">${driver.driver_id}</span>`;
    const eta   = driver.dropoff_at != null ? driver.dropoff_at - nowMin : null;
    countdownHtml = eta != null && eta > 0
      ? `<div class="row-countdown dropoff">Drop-off in ${fmtAge(eta)}</div>`
      : `<div class="row-countdown dropoff">Arriving now</div>`;
  } else if (order.status === 'Waiting for Driver') {
    driverBadge   = `<span class="row-driver" style="background:#2a1a3a;color:#a855f7;border-color:#6b21a8">WAIT</span>`;
    countdownHtml = `<div class="row-countdown wait">Waiting for driver</div>`;
  } else if (driver && driver.status === 'returning') {
    driverBadge   = `<span class="row-driver" style="background:#2a2a1a;color:#f59e0b;border-color:#5a5a1a">${driver.driver_id}</span>`;
    countdownHtml = `<div class="row-countdown pickup">✓ Delivered</div>`;
  }

  const extraCls = isDelivered ? 'delivered' : '';

  return `
    <div class="delivery-row ${extraCls}" data-id="${order.order_id}">
      <span class="row-id">#${order.order_id}</span>
      <div class="row-meta">
        <div class="row-meta-main">${order.pizza_count} pizza${order.pizza_count !== 1 ? 's' : ''} · age ${fmtAge(age)}</div>
        ${countdownHtml}
      </div>
      ${driverBadge}
    </div>`;
}

// ── Driver badges (status strip) ──────────────────────────────────────────────

function renderDriverBadges(drivers) {
  return (drivers || []).map(d =>
    `<span class="driver-badge ${d.status}" title="${d.status}${d.current_order_id ? ' → #' + d.current_order_id : ''}">${d.driver_id}</span>`
  ).join('');
}

// ── Main UI update ────────────────────────────────────────────────────────────

function updateUI(data) {
  const nowMin      = data.now_min   || 0;
  const dayMode     = !!data.day_mode;
  const dayComplete = !!data.day_complete;
  const ovenTime    = data.oven_time || 8;

  // ── Sim time
  setText('sim-time', `sim: ${fmtAge(nowMin)}`);

  // ── Day-complete banner
  if (dayComplete) showDayCompleteBanner();

  // ── Rush pill / day clock
  const rushPill = el('rush-pill');
  const dayClock = el('day-clock');
  if (dayMode) {
    if (rushPill) rushPill.style.display = 'none';
    if (dayClock) {
      dayClock.style.display = '';
      setText('day-clock-time',  data.day_clock     || '—');
      setText('day-clock-label', data.traffic_label  || '');
    }
  } else {
    if (rushPill) {
      rushPill.style.display = '';
      rushPill.textContent   = data.rush_active ? '🚨 Rush Mode' : 'Normal Traffic';
      rushPill.className     = `rush-pill ${data.rush_active ? 'rush' : 'normal'}`;
    }
    if (dayClock) dayClock.style.display = 'none';
  }

  // ── Button state during day mode
  const btnStart   = el('btn-start');
  const btnStop    = el('btn-stop');
  const btnRush    = el('btn-rush');
  const btnDay     = el('btn-day');
  const btnDayStop = el('btn-day-stop');
  if (dayMode) {
    if (btnStart)   btnStart.disabled       = true;
    if (btnStop)    btnStop.disabled        = true;
    if (btnRush)    btnRush.disabled        = true;
    if (btnDay)     btnDay.style.display    = 'none';
    if (btnDayStop) btnDayStop.style.display = '';
  } else {
    if (btnStart)   btnStart.disabled       = false;
    if (btnStop)    btnStop.disabled        = false;
    if (btnRush)    btnRush.disabled        = false;
    if (btnDay)     btnDay.style.display    = '';
    if (btnDayStop) btnDayStop.style.display = 'none';
  }

  // ── Status strip
  setText('throughput', data.throughput.toFixed(2));
  const k = data.kitchen;
  const busyStations = (k.stations || []).filter(s => s.busy).length;
  setText('make-line-status', `${busyStations}/${(k.stations || []).length} busy, ${k.make_queue_depth} queued`);
  setText('oven-status',      `${k.oven_active}/${k.oven_capacity} slots`);
  setText('queue-depth',      `${data.active_orders.length} active`);

  const badgesEl = el('driver-badges');
  if (badgesEl) badgesEl.innerHTML = renderDriverBadges(data.drivers);

  // ── Partition active orders into pipeline stages ───────────────────────────
  const active = data.active_orders || [];
  const recent = data.recent_completed || [];

  // Incoming: any pizza still on the make line
  const incoming = active.filter(o =>
    (o.items || []).some(i => MAKELINE_STATUSES.has(i.pizza_status))
  );

  // Oven: all pizzas off make line but not all done
  const ovenOrders = active.filter(o =>
    !(o.items || []).some(i => MAKELINE_STATUSES.has(i.pizza_status)) &&
     (o.items || []).some(i => OVEN_STATUSES.has(i.pizza_status))
  );

  // Carryout ready: active carryout with all pizzas done
  //   + recently completed carryout within 5 sim-minutes of oven exit
  const carryoutActive = active.filter(o =>
    o.order_type === 'carryout' &&
    (o.items || []).every(i => i.pizza_status === 'DONE')
  );
  const carryoutActiveIds = new Set(carryoutActive.map(o => o.order_id));
  const carryoutRecent = recent.filter(o =>
    o.order_type === 'carryout' &&
    o.ready_at != null &&
    nowMin - o.ready_at < 5.0 &&
    !carryoutActiveIds.has(o.order_id)
  );
  const carryoutPanel = [...carryoutActive, ...carryoutRecent];

  // Deliveries: active delivery orders past the oven stage
  //   (WAITING_FOR_DRIVER or OUT_FOR_DELIVERY — all pizzas done)
  //   + recently delivered within 3 sim-minutes
  const deliveryActive = active.filter(o =>
    o.order_type === 'delivery' &&
    !(o.items || []).some(i => MAKELINE_STATUSES.has(i.pizza_status) || OVEN_STATUSES.has(i.pizza_status))
  );
  const deliveryActiveIds = new Set(deliveryActive.map(o => o.order_id));
  const deliveryRecent = recent.filter(o =>
    o.order_type === 'delivery' &&
    o.ready_at != null &&
    nowMin - o.ready_at < 3.0 &&
    !deliveryActiveIds.has(o.order_id)
  );

  // ── Incoming KDS cards
  const incomingEl = el('incoming-cards');
  setText('incoming-count', incoming.length);
  incomingEl.innerHTML = incoming.length
    ? incoming.map(o => renderOrderCard(o)).join('')
    : '<div class="empty-state">No active orders</div>';

  // ── Oven cards
  const ovenEl = el('oven-cards');
  setText('oven-count', ovenOrders.length);
  ovenEl.innerHTML = ovenOrders.length
    ? ovenOrders.map(o => renderOvenCard(o, nowMin, ovenTime)).join('')
    : '<div class="empty-state">No orders in oven</div>';

  // ── Carryout ready
  const carryoutEl = el('carryout-list');
  setText('carryout-count', carryoutPanel.length);
  carryoutEl.innerHTML = carryoutPanel.length
    ? carryoutPanel.map(o => renderCarryoutRow(o, nowMin)).join('')
    : '<div class="empty-state">No orders ready</div>';

  // ── Deliveries (active + recently delivered)
  const deliveryEl = el('delivery-list');
  const totalDelivery = deliveryActive.length + deliveryRecent.length;
  setText('delivery-count', deliveryActive.length);

  let deliveryHtml = '';
  if (deliveryActive.length) {
    deliveryHtml += deliveryActive.map(o => renderDeliveryRow(o, data.drivers, nowMin)).join('');
  }
  if (deliveryRecent.length) {
    if (deliveryActive.length) {
      deliveryHtml += `<div class="recently-delivered-header">Recently Delivered</div>`;
    }
    deliveryHtml += deliveryRecent.map(o => renderDeliveryRow(o, data.drivers, nowMin)).join('');
  }
  if (!totalDelivery) {
    deliveryHtml = '<div class="empty-state">No deliveries active</div>';
  }
  deliveryEl.innerHTML = deliveryHtml;

  // ── Footer metrics — estimates
  const ce = data.current_estimates || {};
  setText('est-dyn-carryout',   fmtMin(ce.dynamic_carryout));
  setText('est-dyn-delivery',   fmtMin(ce.dynamic_delivery));
  setText('est-naive-carryout', fmtMin(ce.naive_carryout));
  setText('est-naive-delivery', fmtMin(ce.naive_delivery));

  // ── Footer metrics — accuracy
  const acc = data.accuracy || {};
  setText('acc-dyn-mae',    fmtMin(acc.dynamic_mae));
  setText('acc-naive-mae',  fmtMin(acc.naive_mae));
  setText('acc-dyn-wins',   acc.dynamic_wins  ?? 0);
  setText('acc-naive-wins', acc.naive_wins    ?? 0);
  setText('acc-samples',    acc.sample_count  ?? 0);
}

// ── Poll loop ─────────────────────────────────────────────────────────────────

async function fetchState() {
  try {
    const res = await fetch('/api/state');
    if (!res.ok) return;
    const data = await res.json();
    updateUI(data);
  } catch (e) { /* server not ready */ }
}

fetchState();
setInterval(fetchState, 1000);
