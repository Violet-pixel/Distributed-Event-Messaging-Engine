(() => {
  'use strict';

  const $ = (selector, root = document) => root.querySelector(selector);
  const nf = new Intl.NumberFormat('pt-BR');
  const nf1 = new Intl.NumberFormat('pt-BR', { maximumFractionDigits: 1 });
  const brl = new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' });
  const time = new Intl.DateTimeFormat('pt-BR', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  const params = new URLSearchParams(location.search);

  const store = {
    get(key) { try { return localStorage.getItem(key); } catch { return null; } },
    set(key, value) { try { localStorage.setItem(key, value); } catch { } }
  };

  const state = {
    demo: params.has('demo'),
    connected: false,
    errors: 0,
    history: { in: [], out: [], connections: [], events: [], outbox: [], lag: [] },
    seenEvents: new Set(),
    replay: { steps: [], index: -1, timer: null, stream: null, prev: null }
  };

  const darkQuery = matchMedia('(prefers-color-scheme: dark)');
  document.documentElement.dataset.theme = store.get('brado-theme') || (darkQuery.matches ? 'dark' : 'light');
  darkQuery.addEventListener?.('change', (e) => {
    if (!store.get('brado-theme')) document.documentElement.dataset.theme = e.matches ? 'dark' : 'light';
  });
  $('#btn-theme').addEventListener('click', () => {
    const next = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
    document.documentElement.dataset.theme = next;
    store.set('brado-theme', next);
  });

  const el = (tag, className, html) => {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (html !== undefined) node.innerHTML = html;
    return node;
  };
  const esc = (value) => String(value ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const push = (list, value, max = 60) => { list.push(value); if (list.length > max) list.shift(); };

  function toast(message) {
    const node = $('#toast');
    node.textContent = message;
    node.classList.add('show');
    clearTimeout(toast.timer);
    toast.timer = setTimeout(() => node.classList.remove('show'), 2600);
  }

  function uptime(seconds) {
    const s = Math.max(0, Math.floor(seconds));
    const d = Math.floor(s / 86400), h = Math.floor((s % 86400) / 3600), m = Math.floor((s % 3600) / 60);
    if (d) return `no ar há ${d}d ${h}h`;
    if (h) return `no ar há ${h}h ${m}min`;
    if (m) return `no ar há ${m}min ${s % 60}s`;
    return `no ar há ${s}s`;
  }

  function keyed(container, items, keyOf, create, update) {
    if (!container.dataset.emptyHtml) container.dataset.emptyHtml = container.innerHTML;
    const existing = new Map();
    for (const node of [...container.children]) if (node.dataset.key) existing.set(node.dataset.key, node);

    if (items.length === 0) {
      if (!container.querySelector('.empty')) container.innerHTML = container.dataset.emptyHtml;
      return;
    }
    container.querySelector('.empty')?.remove();

    let previous = null;
    for (const item of items) {
      const key = String(keyOf(item));
      let node = existing.get(key);
      if (!node) { node = create(item); node.dataset.key = key; }
      existing.delete(key);
      update(node, item);
      const expected = previous ? previous.nextSibling : container.firstChild;
      if (node !== expected) container.insertBefore(node, expected);
      previous = node;
    }
    existing.forEach((node) => node.remove());
  }

  const kpiState = new Map();
  function setKpi(name, value, decimals = 0) {
    const node = document.querySelector(`[data-kpi="${name}"]`);
    if (!node) return;
    const current = kpiState.get(name) ?? { shown: 0, raf: 0 };
    const from = current.shown, to = Number(value) || 0, started = performance.now(), duration = 650;
    cancelAnimationFrame(current.raf);
    if (Math.round(from) !== Math.round(to)) {
      node.closest('.kpi')?.classList.remove('bump');
      void node.offsetWidth;
      node.closest('.kpi')?.classList.add('bump');
    }
    const step = (now) => {
      const t = Math.min(1, (now - started) / duration);
      const eased = 1 - Math.pow(1 - t, 3);
      current.shown = from + (to - from) * eased;
      node.textContent = decimals ? nf1.format(current.shown) : nf.format(Math.round(current.shown));
      if (t < 1) current.raf = requestAnimationFrame(step);
    };
    current.raf = requestAnimationFrame(step);
    kpiState.set(name, current);
  }

  function spark(name, values) {
    const svg = document.querySelector(`[data-spark="${name}"]`);
    if (!svg || values.length < 2) return;
    const max = Math.max(1, ...values), min = Math.min(...values);
    const range = Math.max(1, max - min);
    const points = values.map((v, i) => [(i / (values.length - 1)) * 100, 26 - ((v - min) / range) * 20]);
    const line = smooth(points);
    svg.innerHTML = `<path class="fill" d="${line} L100 28 L0 28 Z"/><path class="stroke" d="${line}"/>`;
  }

  function smooth(points) {
    if (points.length === 0) return '';
    let d = `M${points[0][0].toFixed(2)} ${points[0][1].toFixed(2)}`;
    for (let i = 0; i < points.length - 1; i++) {
      const p0 = points[i - 1] ?? points[i], p1 = points[i], p2 = points[i + 1], p3 = points[i + 2] ?? p2;
      const c1x = p1[0] + (p2[0] - p0[0]) / 6, c1y = p1[1] + (p2[1] - p0[1]) / 6;
      const c2x = p2[0] - (p3[0] - p1[0]) / 6, c2y = p2[1] - (p3[1] - p1[1]) / 6;
      d += ` C${c1x.toFixed(2)} ${c1y.toFixed(2)} ${c2x.toFixed(2)} ${c2y.toFixed(2)} ${p2[0].toFixed(2)} ${p2[1].toFixed(2)}`;
    }
    return d;
  }

  const chart = { inData: [], outData: [], max: 10, targetMax: 10, receivedAt: performance.now(), points: 90 };
  const W = 1000, H = 240, TOP = 18, BOTTOM = 10;

  function setSeries(series) {
    const inData = series.map((p) => p.inPerSecond);
    const outData = series.map((p) => p.outPerSecond);
    const last = chart.inData.at(-1);
    chart.inData = inData.slice(-(chart.points + 1));
    chart.outData = outData.slice(-(chart.points + 1));
    if (last === undefined || chart.inData.length && chart.inData.at(-1) !== last) chart.receivedAt = performance.now();
    const peak = Math.max(5, ...chart.inData, ...chart.outData);
    chart.targetMax = niceCeil(peak * 1.15);
  }

  function niceCeil(value) {
    const exp = Math.pow(10, Math.floor(Math.log10(value)));
    const f = value / exp;
    return (f <= 1 ? 1 : f <= 2 ? 2 : f <= 2.5 ? 2.5 : f <= 5 ? 5 : 10) * exp;
  }

  function drawGrid() {
    const grid = $('#chart-grid');
    const scale = $('#chart-scale');
    grid.innerHTML = '';
    scale.innerHTML = '';
    for (let i = 0; i <= 4; i++) {
      const y = TOP + (H - TOP - BOTTOM) * (i / 4);
      grid.insertAdjacentHTML('beforeend', `<line x1="0" x2="${W}" y1="${y}" y2="${y}"/>`);
      const value = chart.max * (1 - i / 4);
      const label = el('span', '', nf.format(Math.round(value)));
      label.style.top = `${(y / H) * 100}%`;
      scale.appendChild(label);
    }
  }

  let gridMax = -1;
  function frame(now) {
    chart.max += (chart.targetMax - chart.max) * 0.08;
    if (Math.abs(chart.max - gridMax) / Math.max(1, chart.max) > 0.02) { drawGrid(); gridMax = chart.max; }

    const progress = Math.min(1, (now - chart.receivedAt) / 1000);
    const step = W / (chart.points - 1);
    const toY = (v) => H - BOTTOM - (Math.max(0, v) / chart.max) * (H - TOP - BOTTOM);
    const project = (data) => data.map((v, i) => [(i - (data.length - 1)) * step + W + (1 - progress) * step, toY(v)]);

    if (chart.inData.length > 1) {
      const inPts = project(chart.inData);
      const outPts = project(chart.outData);
      const inLine = smooth(inPts);
      $('#line-in').setAttribute('d', inLine);
      $('#line-out').setAttribute('d', smooth(outPts));
      $('#area-in').setAttribute('d', `${inLine} L${inPts.at(-1)[0]} ${H} L${inPts[0][0]} ${H} Z`);

      const a = chart.inData.at(-2) ?? 0, b = chart.inData.at(-1) ?? 0;
      const head = $('#head-in');
      head.setAttribute('cx', W);
      head.setAttribute('cy', toY(a + (b - a) * progress));
    }
    requestAnimationFrame(frame);
  }
  requestAnimationFrame(frame);

  function renderTopics(topics) {
    $('#topic-count').textContent = `${topics.length} ${topics.length === 1 ? 'tópico' : 'tópicos'}`;
    const maxLag = Math.max(1, ...topics.flatMap((t) => t.groups.map((g) => g.lag)));

    keyed($('#topics'), topics, (t) => t.name, (t) => {
      const node = el('article', 'topic');
      node.innerHTML = `<div class="topic-head"><span class="topic-name mono"></span><span class="topic-meta"></span></div><div class="groups"></div>`;
      if (t.name.endsWith('.dlq')) node.querySelector('.topic-name').classList.add('dlq');
      return node;
    }, (node, t) => {
      node.querySelector('.topic-name').textContent = t.name;
      node.querySelector('.topic-meta').textContent = `offset ${nf.format(t.nextOffset)} · retidas ${nf.format(t.retained)}`;
      const groups = node.querySelector('.groups');
      if (t.groups.length === 0) {
        groups.innerHTML = '<span class="group-name">sem consumidores — mensagens ficam retidas para replay</span>';
        return;
      }
      if (groups.querySelector(':scope > span')) groups.innerHTML = '';
      keyed(groups, t.groups, (g) => g.name, () => {
        const row = el('div', 'group');
        row.innerHTML = `<span class="group-name mono"></span><div class="lagbar"><i></i></div><span class="group-stats"></span>`;
        return row;
      }, (row, g) => {
        row.querySelector('.group-name').textContent = g.name;
        const bar = row.querySelector('.lagbar i');
        bar.style.width = `${Math.max(g.lag > 0 ? 3 : 0, Math.min(100, (g.lag / maxLag) * 100))}%`;
        bar.classList.toggle('calm', g.lag < 50);
        row.querySelector('.group-stats').innerHTML =
          `lag <b>${nf.format(g.lag)}</b> · voo <b>${nf.format(g.inFlight)}</b> · ${g.members} ${g.members === 1 ? 'membro' : 'membros'}`;
      });
    });
  }

  function renderConnections(connections, port) {
    $('#conn-port').textContent = `porta ${port ?? '—'}`;
    keyed($('#connections'), connections, (c) => c.id, (c) => {
      const node = el('div', 'conn');
      node.innerHTML = `<span class="avatar"></span><div><div class="conn-name"></div><div class="conn-sub mono"></div></div><span class="role"></span>`;
      return node;
    }, (node, c) => {
      node.querySelector('.avatar').textContent = (c.clientName || '?').slice(0, 2).toUpperCase();
      node.querySelector('.conn-name').textContent = c.clientName;
      node.querySelector('.conn-sub').textContent =
        `#${c.id} · ↑${nf.format(c.published)} ↓${nf.format(c.delivered)} · ${c.subscriptions} assin. · fila ${c.queuedFrames}`;
      const role = node.querySelector('.role');
      role.className = `role ${c.role}`;
      role.textContent = { Publisher: 'publisher', Subscriber: 'subscriber', Both: 'pub + sub', Unknown: 'ocioso' }[c.role] ?? c.role;
    });
  }

  function renderEventTypes(stats, provider) {
    $('#store-provider').textContent = `${nf.format(stats.events)} eventos em ${nf.format(stats.streams)} streams · ${provider}`;
    const types = [...stats.byType].sort((a, b) => b.count - a.count).slice(0, 9);
    const max = Math.max(1, ...types.map((t) => t.count));
    const container = $('#event-types');
    if (!container.dataset.emptyHtml) container.dataset.emptyHtml = '<p class="empty">Nenhum evento gravado ainda.</p>';
    keyed(container, types, (t) => t.eventType, () => {
      const row = el('div', 'bar-row');
      row.innerHTML = `<span class="mono"></span><b></b><div class="track"><i></i></div>`;
      return row;
    }, (row, t) => {
      row.querySelector('span').textContent = t.eventType;
      row.querySelector('b').textContent = nf.format(t.count);
      requestAnimationFrame(() => { row.querySelector('i').style.width = `${(t.count / max) * 100}%`; });
    });
  }

  const sagaColors = {
    Completed: 'var(--saga-done)', Shipping: 'var(--clay)', ReservingStock: 'var(--taupe)', AwaitingPayment: 'var(--sage)',
    Compensating: '#5C5348', Compensated: '#A69C8F', Cancelled: 'var(--stone)'
  };
  const sagaNames = {
    Completed: 'concluídas', Shipping: 'enviando', ReservingStock: 'reservando', AwaitingPayment: 'aguardando pagamento',
    Compensating: 'compensando', Compensated: 'compensadas', Cancelled: 'canceladas'
  };

  function renderSagas(sagas, outbox) {
    const entries = Object.entries(sagas ?? {}).sort((a, b) => b[1] - a[1]);
    const total = entries.reduce((sum, [, n]) => sum + n, 0);
    const bar = $('#sagas');
    bar.innerHTML = '';
    if (total === 0) bar.insertAdjacentHTML('beforeend', '<i style="flex-grow:1;background:var(--surface-2)"></i>');
    for (const [name, count] of entries) {
      const seg = el('i');
      seg.style.background = sagaColors[name] ?? 'var(--clay)';
      seg.style.flexGrow = String(count);
      seg.title = `${sagaNames[name] ?? name}: ${count}`;
      bar.appendChild(seg);
    }

    let legend = bar.nextElementSibling;
    if (!legend || !legend.classList.contains('saga-legend')) {
      legend = el('div', 'saga-legend');
      bar.after(legend);
    }
    legend.innerHTML = total === 0
      ? '<span>Nenhuma saga ainda — envie um pedido.</span>'
      : entries.map(([name, n]) => `<span><i style="background:${sagaColors[name] ?? 'var(--clay)'}"></i>${sagaNames[name] ?? name} <b>${nf.format(n)}</b></span>`).join('');

    const failing = outbox.failing > 0;
    $('#outbox').innerHTML = `
      <div><span>pendentes</span><b>${nf.format(outbox.pending)}</b></div>
      <div><span>entregues</span><b>${nf.format(outbox.dispatchedTotal ?? outbox.dispatched)}</b></div>
      <div class="${failing ? 'warn' : ''}"><span>reagendadas</span><b>${nf.format(outbox.failing)}</b></div>
      <p class="outbox-note">${outbox.lastError ? `última falha: ${esc(outbox.lastError)}` : 'nenhuma falha de entrega — at-least-once com backoff exponencial'}</p>`;
  }

  function renderFeed(events) {
    const feed = $('#feed');
    const firstRender = state.seenEvents.size === 0;
    keyed(feed, events, (e) => e.position, (e) => {
      const li = el('li', firstRender ? '' : 'new');
      li.innerHTML = `<span class="pos mono">#${nf.format(e.position)}</span><div><div class="type"></div><div class="stream mono"></div></div><span class="pos"></span>`;
      return li;
    }, (li, e) => {
      li.querySelector('.type').textContent = e.eventType;
      li.querySelector('.stream').textContent = `${e.streamId} · v${e.version}`;
      li.querySelectorAll('.pos')[1].textContent = time.format(new Date(e.occurredAt));
      state.seenEvents.add(e.position);
    });
  }

  function apply(snapshot) {
    const broker = snapshot.broker;
    const metrics = broker.metrics;
    const lag = broker.topics.reduce((sum, t) => sum + t.groups.reduce((s, g) => s + g.lag, 0), 0);

    push(state.history.in, metrics.inPerSecond);
    push(state.history.out, metrics.outPerSecond);
    push(state.history.connections, broker.connections.length);
    push(state.history.events, snapshot.eventStore.events);
    push(state.history.outbox, snapshot.outbox.pending);
    push(state.history.lag, lag);

    setKpi('in', metrics.inPerSecond, 1);
    setKpi('out', metrics.outPerSecond, 1);
    setKpi('connections', broker.connections.length);
    setKpi('events', snapshot.eventStore.events);
    setKpi('outbox', snapshot.outbox.pending);
    setKpi('lag', lag);
    for (const key of Object.keys(state.history)) spark(key, state.history[key]);

    setSeries(metrics.series);
    $('#totals').textContent =
      `${nf.format(metrics.messagesIn)} publicadas · ${nf.format(metrics.messagesOut)} entregues · ${nf.format(metrics.redelivered)} reentregas · ${nf.format(metrics.deadLettered)} DLQ`;
    $('#uptime').textContent = uptime(broker.uptimeSeconds);

    renderTopics(broker.topics);
    renderConnections(broker.connections, broker.port);
    renderEventTypes(snapshot.eventStore, snapshot.eventStoreProvider);
    renderSagas(snapshot.sagas, snapshot.outbox);
    renderFeed(snapshot.recentEvents);

    $('#foot-info').textContent =
      `event store ${snapshot.eventStoreProvider} · leitura ${snapshot.readModelProvider} · checkpoint #${nf.format(snapshot.projectionPosition)} · ${nf.format(snapshot.readModel.totalOrders)} pedidos`;
  }

  function setLive(mode) {
    const live = $('#live');
    live.classList.toggle('on', mode === 'live' || mode === 'demo');
    $('#live-label').textContent = { live: 'ao vivo', demo: 'demonstração', retry: 'reconectando', idle: 'conectando' }[mode];
  }

  function connect() {
    if (state.demo || !('EventSource' in window)) return startDemo();
    const source = new EventSource('api/stream');
    source.addEventListener('snapshot', (event) => {
      state.connected = true;
      state.errors = 0;
      setLive('live');
      apply(JSON.parse(event.data));
    });
    source.onerror = () => {
      state.errors++;
      if (!state.connected && state.errors >= 2) {
        source.close();
        startDemo();
      } else {
        setLive('retry');
      }
    };
  }

  function startDemo() {
    state.demo = true;
    setLive('demo');
    const demo = createDemo();
    apply(demo.next());
    setInterval(() => apply(demo.next()), 1000);
    renderStreams(demo.streams());
    state.demoSource = demo;
  }

  function createDemo() {
    let t = 0, messagesIn = 0, messagesOut = 0, events = 2140, position = 2140, redelivered = 0, dead = 0, boost = 0;
    const series = [];
    const types = { OrderCreated: 320, OrderItemAdded: 702, OrderSubmitted: 318, PaymentProcessed: 291, PaymentFailed: 27, StockReserved: 262, StockReservationFailed: 29, PaymentRefunded: 29, OrderShipped: 255, OrderCancelled: 56 };
    const sagas = { Completed: 255, Shipping: 4, ReservingStock: 3, AwaitingPayment: 2, Compensated: 29, Cancelled: 27 };
    const topics = [
      { name: 'auditoria.acessos', offset: 18420, groups: [['siem', 1]] },
      { name: 'notificacoes.email', offset: 9312, groups: [['mailer', 2]] },
      { name: 'notificacoes.email.dlq', offset: 41, groups: [] },
      { name: 'orders.events', offset: 2140, groups: [['notificador-pedidos', 1], ['saga.order-fulfillment', 1]] },
      { name: 'telemetria.sensores', offset: 51230, groups: [['analytics', 3], ['arquivo-frio', 1]] }
    ].map((topic) => ({ ...topic, lags: Object.fromEntries(topic.groups.map(([g]) => [g, Math.floor(Math.random() * 40)])) }));
    const clients = [
      { id: 1, clientName: 'checkout-api', role: 'Publisher' }, { id: 2, clientName: 'sensor-gateway', role: 'Publisher' },
      { id: 3, clientName: 'analytics-worker', role: 'Subscriber' }, { id: 4, clientName: 'analytics-worker', role: 'Subscriber' },
      { id: 5, clientName: 'mailer', role: 'Subscriber' }, { id: 6, clientName: 'billing-bridge', role: 'Both' }
    ].map((c) => ({ ...c, published: 0, delivered: 0, subscriptions: c.role === 'Publisher' ? 0 : 1 }));
    const recent = [];
    const names = Object.keys(types);
    const startedAt = Date.now() - 3 * 3600_000 - 17 * 60_000;

    for (let i = -110; i < 0; i++) {
      const wave = 0.65 + 0.35 * Math.sin(i / 8) + 0.12 * Math.sin(i / 2.7);
      const rate = Math.max(5, Math.round(260 * wave + Math.random() * 40));
      series.push({ at: new Date(Date.now() + i * 1000).toISOString(), inPerSecond: rate, outPerSecond: Math.round(rate * (0.9 + Math.random() * 0.18)) });
    }

    function next() {
      t++;
      const wave = 0.65 + 0.35 * Math.sin(t / 8) + 0.12 * Math.sin(t / 2.7);
      const rateIn = Math.max(5, Math.round((260 + boost) * wave + Math.random() * 40));
      const rateOut = Math.max(0, Math.round(rateIn * (0.9 + Math.random() * 0.18)));
      boost = Math.max(0, boost * 0.92);
      messagesIn += rateIn; messagesOut += rateOut;
      redelivered += Math.random() < 0.4 ? 1 : 0; dead += Math.random() < 0.05 ? 1 : 0;
      series.push({ at: new Date().toISOString(), inPerSecond: rateIn, outPerSecond: rateOut });
      if (series.length > 120) series.shift();

      const newEvents = Math.floor(Math.random() * 6);
      for (let i = 0; i < newEvents; i++) {
        position++; events++;
        const type = names[Math.floor(Math.random() * names.length)];
        types[type]++;
        recent.unshift({ position, streamId: `order-${Math.random().toString(16).slice(2, 10)}${Math.random().toString(16).slice(2, 10)}`, version: 1 + Math.floor(Math.random() * 7), eventType: type, occurredAt: new Date().toISOString() });
      }
      recent.length = Math.min(recent.length, 14);
      if (Math.random() < 0.3) sagas.Completed++;

      for (const topic of topics) {
        topic.offset += Math.floor(rateIn / topics.length * (0.5 + Math.random()));
        for (const [g] of topic.groups) topic.lags[g] = Math.max(0, Math.round(topic.lags[g] + (Math.random() - 0.52) * 30));
      }
      for (const c of clients) {
        if (c.role !== 'Subscriber') c.published += Math.floor(rateIn / 3 * Math.random());
        if (c.role !== 'Publisher') c.delivered += Math.floor(rateOut / 4 * Math.random());
      }

      return {
        broker: {
          at: new Date().toISOString(), uptimeSeconds: (Date.now() - startedAt) / 1000, port: 7420,
          metrics: { messagesIn, messagesOut, redelivered, deadLettered: dead, inPerSecond: rateIn, outPerSecond: rateOut, series: [...series] },
          topics: topics.map((topic) => ({
            name: topic.name, nextOffset: topic.offset, firstOffset: 0, retained: Math.min(topic.offset, 100000),
            groups: topic.groups.map(([name, members]) => ({ name, members, committed: topic.offset - topic.lags[name], lag: topic.lags[name], inFlight: Math.min(topic.lags[name], 8 * members), pendingRedelivery: 0 }))
          })),
          connections: clients.map((c) => ({ ...c, remoteEndPoint: `10.0.0.${10 + c.id}:5${c.id}112`, connectedAt: new Date(startedAt).toISOString(), lastSeen: new Date().toISOString(), queuedFrames: Math.floor(Math.random() * 4) }))
        },
        eventStore: { events, streams: 320 + Math.floor(t / 10), lastPosition: position, byType: Object.entries(types).map(([eventType, count]) => ({ eventType, count })) },
        recentEvents: [...recent],
        outbox: { pending: Math.floor(Math.random() * 6), dispatched: position, failing: 0, dispatchedTotal: position, failures: 3 },
        readModel: { totalOrders: 320 + Math.floor(t / 10), byStatus: {}, revenue: 0, checkpoint: position },
        sagas: { ...sagas },
        projectionPosition: position,
        eventStoreProvider: 'Postgres',
        readModelProvider: 'Postgres'
      };
    }

    const demoStreams = Array.from({ length: 12 }, (_, i) => {
      const id = `order-${(0x3f2a91c4 + i * 7919).toString(16)}${(0x8b1e44d0 + i * 104729).toString(16)}e1c0ffee`;
      const failed = i % 5 === 3;
      return { streamId: id, streamType: 'order', version: failed ? 7 : 7, updatedAt: new Date(Date.now() - i * 61_000).toISOString(), failed };
    });

    function replay(streamId) {
      const s = demoStreams.find((x) => x.streamId === streamId) ?? demoStreams[0];
      const at = Date.now() - 50_000;
      const total = 129.9 * 2 + 99.5;
      const steps = [
        ['OrderCreated', { customerRef: 'CLI-4821', currency: 'BRL' }, { status: 'Draft', total: 0, lines: 0 }],
        ['OrderItemAdded', { sku: 'LIV-ARQ-001', quantity: 2, unitPrice: 129.9 }, { status: 'Draft', total: 259.8, lines: 1 }],
        ['OrderItemAdded', { sku: 'LIV-ALG-003', quantity: 1, unitPrice: 99.5 }, { status: 'Draft', total, lines: 2 }],
        ['OrderSubmitted', { total, itemCount: 2 }, { status: 'Submitted', total, lines: 2 }],
        ['PaymentProcessed', { paymentRef: 'PAY-7C1E09A2B4', amount: total }, { status: 'Paid', total, lines: 2, paymentRef: 'PAY-7C1E09A2B4' }]
      ];
      if (s.failed) {
        steps.push(['StockReservationFailed', { reason: 'Item sem estoque no centro de distribuição' }, { status: 'Paid', total, lines: 2, paymentRef: 'PAY-7C1E09A2B4' }]);
        steps.push(['PaymentRefunded', { refundRef: 'REF-3F2A91C48B', amount: total }, { status: 'Paid', total, lines: 2, paymentRef: 'PAY-7C1E09A2B4', refunded: true }]);
        steps.push(['OrderCancelled', { reason: 'Estoque indisponível — pagamento estornado' }, { status: 'Cancelled', total, lines: 2, paymentRef: 'PAY-7C1E09A2B4', refunded: true, cancellationReason: 'Estoque indisponível — pagamento estornado' }]);
      } else {
        steps.push(['StockReserved', { reservationRef: 'RSV-91D2C0AA17' }, { status: 'StockReserved', total, lines: 2, paymentRef: 'PAY-7C1E09A2B4' }]);
        steps.push(['OrderShipped', { trackingCode: 'BR482193775BD' }, { status: 'Shipped', total, lines: 2, paymentRef: 'PAY-7C1E09A2B4', trackingCode: 'BR482193775BD' }]);
      }
      return {
        streamId: s.streamId,
        steps: steps.map(([eventType, data, st], i) => ({
          version: i + 1, position: 2000 + i * 3, eventType, occurredAt: new Date(at + i * 4200).toISOString(), data,
          state: { paymentRef: null, trackingCode: null, refunded: false, cancellationReason: null, ...st }
        }))
      };
    }

    return {
      next,
      streams: () => demoStreams,
      replay,
      boost: () => { boost = 900; }
    };
  }

  async function loadStreams(search = '') {
    if (state.demo) {
      const list = state.demoSource?.streams() ?? [];
      return renderStreams(list.filter((s) => s.streamId.includes(search)));
    }
    try {
      const response = await fetch(`api/streams?take=40&search=${encodeURIComponent(search)}`);
      if (response.ok) renderStreams(await response.json());
    } catch { }
  }

  function renderStreams(streams) {
    const list = $('#streams');
    if (!list.dataset.emptyHtml) list.dataset.emptyHtml = '<li class="empty">Nenhum stream encontrado.</li>';
    keyed(list, streams, (s) => s.streamId, (s) => {
      const li = el('li');
      li.innerHTML = `<div class="sid mono"></div><div class="sver"></div>`;
      li.addEventListener('click', () => selectStream(s.streamId));
      return li;
    }, (li, s) => {
      li.querySelector('.sid').textContent = s.streamId;
      li.querySelector('.sver').textContent = `${s.streamType} · v${s.version} · ${time.format(new Date(s.updatedAt))}`;
      li.classList.toggle('active', s.streamId === state.replay.stream);
    });
  }

  async function selectStream(streamId) {
    stopPlayback();
    state.replay.stream = streamId;
    document.querySelectorAll('#streams li').forEach((li) => li.classList.toggle('active', li.dataset.key === streamId));

    let data;
    if (state.demo) data = state.demoSource.replay(streamId);
    else {
      const response = await fetch(`api/streams/${encodeURIComponent(streamId)}/replay`);
      if (!response.ok) return toast('Não foi possível carregar o stream');
      data = await response.json();
    }

    state.replay.steps = data.steps;
    state.replay.index = -1;
    state.replay.prev = null;
    $('#replay-title').textContent = `${streamId} · ${data.steps.length} eventos`;

    const timeline = $('#timeline');
    timeline.innerHTML = '';
    data.steps.forEach((step, i) => {
      const li = el('li');
      li.innerHTML = `<div class="tl-type">${esc(step.eventType)}</div><div class="tl-meta mono">v${step.version} · #${nf.format(step.position)} · ${time.format(new Date(step.occurredAt))}</div>`;
      li.addEventListener('click', () => { stopPlayback(); showStep(i); });
      timeline.appendChild(li);
    });
    ['#btn-play', '#btn-step', '#btn-step-back'].forEach((id) => { $(id).disabled = data.steps.length === 0; });
    showStep(0);
  }

  function showStep(index) {
    const steps = state.replay.steps;
    if (index < 0 || index >= steps.length) return;
    state.replay.index = index;
    const items = [...$('#timeline').children];
    items.forEach((li, i) => {
      li.classList.toggle('done', i < index);
      li.classList.toggle('current', i === index);
    });
    const timeline = $('#timeline');
    const item = items[index];
    if (item) timeline.scrollTo({ top: item.offsetTop - timeline.clientHeight / 2 + item.clientHeight / 2, behavior: 'smooth' });

    const step = steps[index];
    const s = step.state;
    const prev = state.replay.prev;
    const rows = s ? [
      ['Status', `<span class="status-pill ${esc(s.status)}">${esc(s.status)}</span>`, s.status],
      ['Total', brl.format(s.total ?? 0), s.total],
      ['Itens', nf.format(s.lines ?? 0), s.lines],
      ['Pagamento', esc(s.paymentRef ?? '—'), s.paymentRef],
      ['Rastreio', esc(s.trackingCode ?? '—'), s.trackingCode],
      ['Estornado', s.refunded ? 'sim' : 'não', s.refunded],
      ['Motivo', esc(s.cancellationReason ?? '—'), s.cancellationReason]
    ] : [['Versão', `v${step.version}`, step.version]];

    $('#state-grid').innerHTML = rows.map(([label, html, raw], i) => {
      const changed = prev && prev[i] !== undefined && prev[i] !== raw;
      return `<dt>${label}</dt><dd class="${changed ? 'flash' : ''}">${html}</dd>`;
    }).join('');
    state.replay.prev = rows.map((r) => r[2]);
    $('#state-data').textContent = `${step.eventType} v${step.version}\n${JSON.stringify(step.data, null, 2)}`;
  }

  function stopPlayback() {
    clearInterval(state.replay.timer);
    state.replay.timer = null;
    $('#btn-play').textContent = '▶ Reproduzir';
  }

  $('#btn-play').addEventListener('click', () => {
    if (state.replay.timer) return stopPlayback();
    if (state.replay.index >= state.replay.steps.length - 1) { state.replay.prev = null; showStep(0); }
    $('#btn-play').textContent = '❚❚ Pausar';
    state.replay.timer = setInterval(() => {
      if (state.replay.index >= state.replay.steps.length - 1) return stopPlayback();
      showStep(state.replay.index + 1);
    }, 750);
  });
  $('#btn-step').addEventListener('click', () => { stopPlayback(); showStep(state.replay.index + 1); });
  $('#btn-step-back').addEventListener('click', () => { stopPlayback(); showStep(state.replay.index - 1); });

  let searchTimer;
  $('#stream-search').addEventListener('input', (e) => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => loadStreams(e.target.value.trim()), 250);
  });

  $('#btn-simulate').addEventListener('click', async () => {
    if (state.demo) { state.demoSource?.boost(); return toast('Demonstração: pico de tráfego simulado'); }
    try {
      const response = await fetch('api/simulate?orders=30&messages=1500', { method: 'POST' });
      toast(response.ok ? 'Simulação iniciada: 30 pedidos e 1.500 mensagens' : 'Não foi possível iniciar a simulação');
      setTimeout(() => loadStreams($('#stream-search').value.trim()), 4000);
    } catch { toast('Servidor indisponível'); }
  });

  $('#btn-rebuild').addEventListener('click', async () => {
    if (state.demo) return toast('Demonstração: replay das projeções simulado');
    try {
      const response = await fetch('api/projections/rebuild', { method: 'POST' });
      toast(response.ok ? 'Modelo de leitura sendo reconstruído a partir do log' : 'Falha ao reconstruir projeções');
    } catch { toast('Servidor indisponível'); }
  });

  connect();
  loadStreams();
  setInterval(() => { if (!state.demo && !$('#stream-search').value) loadStreams(); }, 15000);
})();
