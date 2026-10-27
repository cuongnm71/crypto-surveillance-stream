/**
 * Crypto Market Surveillance — Live Command Center (Frontend Application Orchestrator)
 * Vanilla JavaScript implementation strictly compliant with secure DOM standards (ADR D1).
 * Zero unsafe HTML string sinks / Zero external UI frameworks / Sub-100ms real-time event pipeline.
 */

/* global SurveillanceChart */

(function () {
  'use strict';

  // Application State
  const state = {
    activeSymbol: 'BTCUSDT',
    activeTimeframe: '1m',
    activeFilter: 'ALL',
    activeTab: 'tab-trades',
    lastPrice: null,
    allSymbols: [],
    alerts: [],
    ws: null,
    wsReconnectTimeout: null,
    chart: null,
  };

  // DOM Elements Cache
  const el = {
    statusIndicator: document.getElementById('status-indicator'),
    statusText: document.getElementById('status-text'),
    metricIngestRate: document.getElementById('metric-ingest-rate'),
    metricChRows: document.getElementById('metric-ch-rows'),
    metricChRatio: document.getElementById('metric-ch-ratio'),
    metricRedisMem: document.getElementById('metric-redis-mem'),
    metricRedisKeys: document.getElementById('metric-redis-keys'),
    metricWsClients: document.getElementById('metric-ws-clients'),
    activeSymbolDisplay: document.getElementById('active-symbol-display'),
    activeSymbolTier: document.getElementById('active-symbol-tier'),
    activePriceDisplay: document.getElementById('active-price-display'),
    priceChangePill: document.getElementById('price-change-pill'),
    stat24hHigh: document.getElementById('stat-24h-high'),
    stat24hLow: document.getElementById('stat-24h-low'),
    chartLoadingOverlay: document.getElementById('chart-loading-overlay'),
    alertsList: document.getElementById('alerts-list'),
    alertsCountBadge: document.getElementById('alerts-count-badge'),
    quickSymbols: document.getElementById('quick-symbols'),
    symbolSearchInput: document.getElementById('symbol-search-input'),
    symbolDropdown: document.getElementById('symbol-dropdown'),
    tradesTableBody: document.getElementById('trades-table-body'),
    btnRefreshDrawer: document.getElementById('btn-refresh-drawer'),
  };

  /**
   * Format number as currency (e.g. $67,420.50 or $0.00012)
   */
  function formatPrice(num) {
    if (num === null || num === undefined || isNaN(num)) {
      return '$—.——';
    }
    const val = Number(num);
    if (val >= 1000) {
      return '$' + val.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    } else if (val >= 1) {
      return '$' + val.toFixed(4);
    } else {
      return '$' + val.toFixed(6);
    }
  }

  /**
   * Format numbers into compact units (e.g. 1.2M, 450k)
   */
  function formatCompactNumber(num) {
    if (!num || isNaN(num)) return '0';
    const n = Number(num);
    if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B';
    if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(1) + 'k';
    return n.toLocaleString();
  }

  /**
   * Format ISO date/time into HH:MM:SS
   */
  function formatTime(isoOrTimestamp) {
    if (!isoOrTimestamp) return '--:--:--';
    const d = new Date(isoOrTimestamp);
    return isNaN(d.getTime()) ? String(isoOrTimestamp) : d.toTimeString().split(' ')[0];
  }

  /**
   * Safely set status badge UI
   */
  function setConnectionStatus(status) {
    if (!el.statusIndicator || !el.statusText) return;
    el.statusIndicator.classList.remove('online', 'connecting', 'offline');
    if (status === 'online') {
      el.statusIndicator.classList.add('online');
      el.statusText.textContent = 'ONLINE';
    } else if (status === 'connecting') {
      el.statusIndicator.classList.add('connecting');
      el.statusText.textContent = 'CONNECTING';
    } else {
      el.statusIndicator.classList.add('offline');
      el.statusText.textContent = 'OFFLINE';
    }
  }

  /**
   * Determine WebSocket endpoint URL
   */
  function getWebSocketUrl() {
    const loc = window.location;
    if (loc.protocol === 'file:') {
      return 'ws://127.0.0.1:8000/ws';
    }
    const protocol = loc.protocol === 'https:' ? 'wss:' : 'ws:';
    return `${protocol}//${loc.host}/ws`;
  }

  /**
   * Connect to WebSocket Broadcast Hub with exponential backoff
   */
  function connectWebSocket() {
    if (state.ws) {
      try {
        state.ws.close();
      } catch (e) {
        // ignore
      }
    }

    setConnectionStatus('connecting');
    const wsUrl = getWebSocketUrl();

    try {
      state.ws = new WebSocket(wsUrl);
    } catch (e) {
      scheduleReconnect();
      return;
    }

    state.ws.onopen = function () {
      setConnectionStatus('online');
    };

    state.ws.onmessage = function (event) {
      try {
        const payload = JSON.parse(event.data);
        handleIncomingMessage(payload);
      } catch (err) {
        // Skip malformed frame
      }
    };

    state.ws.onerror = function () {
      setConnectionStatus('offline');
    };

    state.ws.onclose = function () {
      setConnectionStatus('offline');
      scheduleReconnect();
    };
  }

  function scheduleReconnect() {
    if (state.wsReconnectTimeout) {
      clearTimeout(state.wsReconnectTimeout);
    }
    state.wsReconnectTimeout = setTimeout(connectWebSocket, 3000);
  }

  /**
   * Handle incoming WebSocket messages (tickers, alerts, telemetry)
   */
  function handleIncomingMessage(msg) {
    if (!msg || typeof msg !== 'object') return;

    if (msg.type === 'ticker') {
      handleTickerMessage(msg);
    } else if (msg.type === 'alert') {
      handleAlertMessage(msg.data);
    } else if (msg.type === 'telemetry') {
      handleTelemetryMessage(msg.data);
    }
  }

  /**
   * Update live price on 4Hz ticker arrival
   */
  function handleTickerMessage(ticker) {
    if (!ticker || ticker.symbol !== state.activeSymbol) return;

    const price = Number(ticker.price);
    if (isNaN(price)) return;

    // Trigger visual tick animation
    if (el.activePriceDisplay) {
      el.activePriceDisplay.textContent = formatPrice(price);
      el.activePriceDisplay.classList.remove('tick-up', 'tick-down');

      if (state.lastPrice !== null) {
        if (price > state.lastPrice) {
          el.activePriceDisplay.classList.add('tick-up');
        } else if (price < state.lastPrice) {
          el.activePriceDisplay.classList.add('tick-down');
        }
      }
      setTimeout(() => {
        if (el.activePriceDisplay) {
          el.activePriceDisplay.classList.remove('tick-up', 'tick-down');
        }
      }, 250);
    }

    state.lastPrice = price;

    // Update Chart Bar in real time
    if (state.chart) {
      state.chart.updatePriceTick(price, ticker.timestamp);
    }
  }

  /**
   * Handle incoming Anomaly Alert (Whale / Liquidation / Volatility)
   */
  function handleAlertMessage(alertData) {
    if (!alertData) return;

    state.alerts.unshift(alertData);
    if (state.alerts.length > 200) {
      state.alerts.pop();
    }

    if (el.alertsCountBadge) {
      el.alertsCountBadge.textContent = String(state.alerts.length);
    }

    // Render alert card if matches current filter
    if (shouldDisplayAlert(alertData, state.activeFilter)) {
      renderAlertCard(alertData, true);
    }

    // If matches active symbol, pin marker onto candle
    if (alertData.symbol === state.activeSymbol && state.chart) {
      state.chart.addAnomalyMarker(alertData);
    }
  }

  function handleTelemetryMessage(data) {
    if (!data) return;
    if (data.active_ws_connections !== undefined && el.metricWsClients) {
      el.metricWsClients.textContent = String(data.active_ws_connections);
    }
  }

  /**
   * Filter check for alert card
   */
  function shouldDisplayAlert(alert, filter) {
    if (filter === 'ALL') return true;
    const typeStr = String(alert.alert_type || alert.type || '').toUpperCase();
    if (filter === 'WHALE' && typeStr.includes('WHALE')) return true;
    if (filter === 'LIQUIDATION' && typeStr.includes('LIQ')) return true;
    if (filter === 'VOLATILITY' && (typeStr.includes('VOL') || typeStr.includes('ZSCORE'))) return true;
    return false;
  }

  /**
   * Build and append an Alert Card using strictly safe DOM methods (Zero raw HTML sinks)
   */
  function renderAlertCard(alert, prepend) {
    if (!el.alertsList) return;

    // Remove empty placeholder if present
    const placeholder = el.alertsList.querySelector('.empty-feed-placeholder');
    if (placeholder) {
      placeholder.remove();
    }

    const card = document.createElement('div');
    card.className = 'alert-card';

    const severity = String(alert.severity || 'HIGH').toLowerCase();
    card.classList.add(severity);

    // Card Top Row
    const topRow = document.createElement('div');
    topRow.className = 'card-top';

    const typeSpan = document.createElement('span');
    const rawType = String(alert.alert_type || alert.type || 'ANOMALY');
    typeSpan.className = 'card-type';
    if (rawType.includes('WHALE')) typeSpan.classList.add('whale');
    else if (rawType.includes('LIQ')) typeSpan.classList.add('liquidation');
    else typeSpan.classList.add('volatility');
    typeSpan.textContent = rawType.replace(/_/g, ' ');

    const timeSpan = document.createElement('span');
    timeSpan.className = 'card-time';
    timeSpan.textContent = formatTime(alert.event_time || alert.emitted_at);

    topRow.appendChild(typeSpan);
    topRow.appendChild(timeSpan);

    // Card Main Row
    const mainRow = document.createElement('div');
    mainRow.className = 'card-main';

    const symWrap = document.createElement('div');
    const symSpan = document.createElement('span');
    symSpan.className = 'card-symbol';
    symSpan.textContent = alert.symbol || 'MARKET';

    const sideSpan = document.createElement('span');
    const side = String(alert.side || (alert.message && alert.message.includes('SELL') ? 'SELL' : 'BUY')).toUpperCase();
    sideSpan.className = `card-side ${side.toLowerCase()}`;
    sideSpan.textContent = side;

    symWrap.appendChild(symSpan);
    symWrap.appendChild(sideSpan);

    const notionalSpan = document.createElement('span');
    notionalSpan.className = 'card-notional num-mono';
    const notional = alert.notional_usd || (alert.price && alert.qty ? alert.price * alert.qty : 0);
    notionalSpan.textContent = notional ? `$${formatCompactNumber(notional)}` : '—';

    mainRow.appendChild(symWrap);
    mainRow.appendChild(notionalSpan);

    // Card Footer Row
    const footerRow = document.createElement('div');
    footerRow.className = 'card-footer';

    const triggerSpan = document.createElement('span');
    triggerSpan.className = 'num-mono';
    triggerSpan.textContent = alert.price ? `Px: ${formatPrice(alert.price)}` : 'Trigger Event';

    const statSpan = document.createElement('span');
    statSpan.className = 'num-mono text-accent';
    if (alert.zscore) {
      statSpan.textContent = `Z: ${Number(alert.zscore).toFixed(1)}σ`;
    } else if (alert.return_pct) {
      statSpan.textContent = `Δ: ${(Number(alert.return_pct) * 100).toFixed(2)}%`;
    } else {
      statSpan.textContent = 'Sub-30ms Edge';
    }

    footerRow.appendChild(triggerSpan);
    footerRow.appendChild(statSpan);

    // Assemble Card
    card.appendChild(topRow);
    card.appendChild(mainRow);
    card.appendChild(footerRow);

    // Click on card to select that symbol on the chart!
    card.addEventListener('click', () => {
      if (alert.symbol) {
        selectSymbol(alert.symbol);
      }
    });

    if (prepend && el.alertsList.firstChild) {
      el.alertsList.insertBefore(card, el.alertsList.firstChild);
    } else {
      el.alertsList.appendChild(card);
    }

    // Limit cards in DOM to 60 for 60fps performance
    while (el.alertsList.children.length > 60) {
      el.alertsList.lastChild.remove();
    }
  }

  /**
   * Re-render all alert cards matching active filter
   */
  function refreshAlertsList() {
    if (!el.alertsList) return;
    el.alertsList.replaceChildren();

    const filtered = state.alerts.filter((a) => shouldDisplayAlert(a, state.activeFilter));
    if (filtered.length === 0) {
      const emptyDiv = document.createElement('div');
      emptyDiv.className = 'empty-feed-placeholder';

      const scanDiv = document.createElement('div');
      scanDiv.className = 'radar-scan';

      const p = document.createElement('p');
      p.textContent = 'No alerts matching the selected filter.';

      emptyDiv.appendChild(scanDiv);
      emptyDiv.appendChild(p);
      el.alertsList.appendChild(emptyDiv);
      return;
    }

    filtered.forEach((a) => renderAlertCard(a, false));
  }

  /**
   * Load Candles from REST API
   */
  async function loadCandles(symbol, tf) {
    if (el.chartLoadingOverlay) {
      el.chartLoadingOverlay.classList.remove('hidden');
    }

    try {
      const resp = await fetch(`/api/v1/candles/${encodeURIComponent(symbol)}?tf=${encodeURIComponent(tf)}&limit=150`);
      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}`);
      }
      const candles = await resp.json();
      if (state.chart) {
        state.chart.setTimeframe(tf);
        state.chart.setData(candles);

        // Compute 24h stats if candles exist
        if (candles.length > 0) {
          let high = -Infinity;
          let low = Infinity;
          for (const c of candles) {
            if (c.high > high) high = c.high;
            if (c.low < low) low = c.low;
          }
          if (el.stat24hHigh) el.stat24hHigh.textContent = formatPrice(high);
          if (el.stat24hLow) el.stat24hLow.textContent = formatPrice(low);

          const lastCandle = candles[candles.length - 1];
          if (el.activePriceDisplay && state.lastPrice === null) {
            el.activePriceDisplay.textContent = formatPrice(lastCandle.close);
            state.lastPrice = lastCandle.close;
          }
        }
      }
    } catch (err) {
      if (state.chart) {
        state.chart.setData([]);
      }
    } finally {
      if (el.chartLoadingOverlay) {
        el.chartLoadingOverlay.classList.add('hidden');
      }
    }
  }

  /**
   * Select a new active cryptocurrency symbol
   */
  function selectSymbol(sym) {
    const symbol = sym.trim().toUpperCase();
    if (state.activeSymbol === symbol) return;

    state.activeSymbol = symbol;
    state.lastPrice = null;

    // Update Title
    if (el.activeSymbolDisplay) {
      el.activeSymbolDisplay.textContent = `${symbol.replace('USDT', '')}/USDT`;
    }

    // Update Quick Symbol pills
    if (el.quickSymbols) {
      const pills = el.quickSymbols.querySelectorAll('.pill-btn');
      pills.forEach((p) => {
        if (p.getAttribute('data-symbol') === symbol) {
          p.classList.add('active');
          p.setAttribute('aria-selected', 'true');
        } else {
          p.classList.remove('active');
          p.setAttribute('aria-selected', 'false');
        }
      });
    }

    // Update Tier Badge
    const meta = state.allSymbols.find((s) => s.symbol === symbol);
    if (el.activeSymbolTier) {
      if (meta) {
        el.activeSymbolTier.textContent = `${meta.tier} • ${meta.category}`;
      } else {
        el.activeSymbolTier.textContent = 'TRACKED SYMBOL';
      }
    }

    // Reload Candles
    loadCandles(state.activeSymbol, state.activeTimeframe);
  }

  /**
   * Load Symbol Catalog from REST API
   */
  async function loadSymbols() {
    try {
      const resp = await fetch('/api/v1/symbols');
      if (!resp.ok) return;
      state.allSymbols = await resp.json();
    } catch (err) {
      // ignore
    }
  }

  /**
   * Populate search dropdown menu based on input query
   */
  function filterSymbolDropdown(query) {
    if (!el.symbolDropdown) return;
    el.symbolDropdown.replaceChildren();

    const q = query.trim().toUpperCase();
    if (!q) {
      el.symbolDropdown.classList.add('hidden');
      return;
    }

    const matches = state.allSymbols
      .filter((s) => s.symbol.includes(q) || s.base_asset.includes(q))
      .slice(0, 10);

    if (matches.length === 0) {
      el.symbolDropdown.classList.add('hidden');
      return;
    }

    matches.forEach((item) => {
      const div = document.createElement('div');
      div.className = 'symbol-dropdown-item';

      const symSpan = document.createElement('span');
      symSpan.className = 'item-sym';
      symSpan.textContent = item.symbol;

      const tierSpan = document.createElement('span');
      tierSpan.className = 'item-tier';
      tierSpan.textContent = `${item.tier} • ${item.category}`;

      div.appendChild(symSpan);
      div.appendChild(tierSpan);

      div.addEventListener('click', () => {
        selectSymbol(item.symbol);
        if (el.symbolSearchInput) el.symbolSearchInput.value = '';
        el.symbolDropdown.classList.add('hidden');
      });

      el.symbolDropdown.appendChild(div);
    });

    el.symbolDropdown.classList.remove('hidden');
  }

  /**
   * Fetch Telemetry and update Panel 1 Top Bar
   */
  async function fetchTelemetry() {
    try {
      const resp = await fetch('/api/v1/telemetry');
      if (!resp.ok) return;
      const data = await resp.json();

      if (data.clickhouse) {
        if (el.metricChRows && data.clickhouse.bronze_trades_count !== undefined) {
          el.metricChRows.textContent = formatCompactNumber(data.clickhouse.bronze_trades_count);
        }
        if (el.metricChRatio && data.clickhouse.compression_ratio !== undefined) {
          el.metricChRatio.textContent = `ZSTD ${data.clickhouse.compression_ratio}x`;
        }
      }

      if (data.redis) {
        if (el.metricRedisMem && data.redis.used_memory_human) {
          el.metricRedisMem.textContent = data.redis.used_memory_human;
        }
        if (el.metricRedisKeys && data.redis.total_keys !== undefined) {
          el.metricRedisKeys.textContent = `${data.redis.total_keys} keys`;
        }
      }

      if (data.ws_hub && el.metricWsClients) {
        el.metricWsClients.textContent = String(data.ws_hub.active_connections || 1);
      }
    } catch (err) {
      // ignore
    }
  }

  /**
   * Fetch Live Trades from ClickHouse Bronze and update Panel 4 Table
   */
  async function fetchLiveTrades() {
    if (!el.tradesTableBody) return;
    try {
      const resp = await fetch('/api/v1/trades/live?limit=30');
      if (!resp.ok) return;
      const trades = await resp.json();

      el.tradesTableBody.replaceChildren();

      if (!Array.isArray(trades) || trades.length === 0) {
        const tr = document.createElement('tr');
        const td = document.createElement('td');
        td.colSpan = 8;
        td.className = 'text-center text-muted';
        td.textContent = 'Waiting for trades to stream into ClickHouse Bronze layer...';
        tr.appendChild(td);
        el.tradesTableBody.appendChild(tr);
        return;
      }

      trades.forEach((t) => {
        const tr = document.createElement('tr');

        // Time
        const tdTime = document.createElement('td');
        tdTime.className = 'num-mono';
        tdTime.textContent = formatTime(t.event_time);
        tr.appendChild(tdTime);

        // Symbol
        const tdSym = document.createElement('td');
        tdSym.className = 'num-mono font-bold';
        tdSym.textContent = t.symbol || '—';
        tr.appendChild(tdSym);

        // Trade ID
        const tdId = document.createElement('td');
        tdId.className = 'num-mono text-muted';
        tdId.textContent = String(t.trade_id || '—');
        tr.appendChild(tdId);

        // Price
        const tdPx = document.createElement('td');
        tdPx.className = 'num-mono font-bold';
        tdPx.textContent = formatPrice(t.price);
        tr.appendChild(tdPx);

        // Quantity
        const tdQty = document.createElement('td');
        tdQty.className = 'num-mono';
        tdQty.textContent = Number(t.qty || 0).toFixed(4);
        tr.appendChild(tdQty);

        // Notional USD
        const tdNotional = document.createElement('td');
        tdNotional.className = 'num-mono text-accent';
        const notional = (t.price || 0) * (t.qty || 0);
        tdNotional.textContent = `$${formatCompactNumber(notional)}`;
        tr.appendChild(tdNotional);

        // Side
        const tdSide = document.createElement('td');
        const sideBadge = document.createElement('span');
        const sideStr = String(t.side || 'BUY').toUpperCase();
        sideBadge.className = `side-badge ${sideStr.toLowerCase()}`;
        sideBadge.textContent = sideStr;
        tdSide.appendChild(sideBadge);
        tr.appendChild(tdSide);

        // Type
        const tdType = document.createElement('td');
        if (t.is_simulated) {
          const simBadge = document.createElement('span');
          simBadge.className = 'sim-tag';
          simBadge.textContent = 'SIMULATED';
          tdType.appendChild(simBadge);
        } else {
          tdType.className = 'text-muted';
          tdType.textContent = 'BINANCE REAL';
        }
        tr.appendChild(tdType);

        el.tradesTableBody.appendChild(tr);
      });
    } catch (err) {
      // ignore
    }
  }

  /**
   * Initialize All Event Listeners and Components
   */
  function initApp() {
    // 1. Initialize TradingView Chart
    state.chart = new SurveillanceChart('chart-container');
    const chartOk = state.chart.init();
    if (!chartOk) {
      // Retry after slight delay if script loading
      setTimeout(() => {
        state.chart.init();
        loadCandles(state.activeSymbol, state.activeTimeframe);
      }, 500);
    }

    // 2. Setup Quick Symbol Buttons
    if (el.quickSymbols) {
      el.quickSymbols.addEventListener('click', (e) => {
        const btn = e.target.closest('.pill-btn');
        if (btn) {
          const sym = btn.getAttribute('data-symbol');
          if (sym) selectSymbol(sym);
        }
      });
    }

    // 3. Setup Symbol Search Input & Dropdown
    if (el.symbolSearchInput) {
      el.symbolSearchInput.addEventListener('input', (e) => {
        filterSymbolDropdown(e.target.value);
      });
      document.addEventListener('click', (e) => {
        if (!e.target.closest('.symbol-search-wrapper') && el.symbolDropdown) {
          el.symbolDropdown.classList.add('hidden');
        }
      });
    }

    // 4. Setup Timeframe Buttons
    const tfButtons = document.querySelectorAll('.tf-btn');
    tfButtons.forEach((btn) => {
      btn.addEventListener('click', () => {
        tfButtons.forEach((b) => {
          b.classList.remove('active');
          b.setAttribute('aria-selected', 'false');
        });
        btn.classList.add('active');
        btn.setAttribute('aria-selected', 'true');

        const tf = btn.getAttribute('data-tf') || '1m';
        state.activeTimeframe = tf;
        loadCandles(state.activeSymbol, state.activeTimeframe);
      });
    });

    // 5. Setup Alerts Filter Buttons
    const filterButtons = document.querySelectorAll('.filter-btn');
    filterButtons.forEach((btn) => {
      btn.addEventListener('click', () => {
        filterButtons.forEach((b) => {
          b.classList.remove('active');
          b.setAttribute('aria-selected', 'false');
        });
        btn.classList.add('active');
        btn.setAttribute('aria-selected', 'true');

        state.activeFilter = btn.getAttribute('data-filter') || 'ALL';
        refreshAlertsList();
      });
    });

    // 6. Setup Drawer Tabs
    const drawerTabs = document.querySelectorAll('.drawer-tab');
    drawerTabs.forEach((tab) => {
      tab.addEventListener('click', () => {
        drawerTabs.forEach((t) => {
          t.classList.remove('active');
          t.setAttribute('aria-selected', 'false');
        });
        tab.classList.add('active');
        tab.setAttribute('aria-selected', 'true');

        const targetPaneId = tab.getAttribute('data-tab');
        const panes = document.querySelectorAll('.drawer-pane');
        panes.forEach((p) => {
          if (p.id === targetPaneId) {
            p.classList.remove('hidden');
            p.classList.add('active');
          } else {
            p.classList.add('hidden');
            p.classList.remove('active');
          }
        });
      });
    });

    // 7. Refresh Button
    if (el.btnRefreshDrawer) {
      el.btnRefreshDrawer.addEventListener('click', () => {
        fetchLiveTrades();
        fetchTelemetry();
      });
    }

    // 8. Start WebSocket & Initial Fetches
    connectWebSocket();
    loadSymbols();
    loadCandles(state.activeSymbol, state.activeTimeframe);
    fetchTelemetry();
    fetchLiveTrades();

    // 9. Periodic Polling
    setInterval(fetchTelemetry, 3000);
    setInterval(fetchLiveTrades, 2000);
  }

  // DOM Content Loaded Handler
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initApp);
  } else {
    initApp();
  }
})();
