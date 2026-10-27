/**
 * TradingView Lightweight Charts Wrapper for Crypto Market Surveillance (ADR D1)
 * Renders real-time 4Hz candlestick & volume series with PySpark Anomaly Markers.
 */

/* global LightweightCharts */

class SurveillanceChart {
  constructor(containerId) {
    this.container = document.getElementById(containerId);
    this.chart = null;
    this.candleSeries = null;
    this.volumeSeries = null;
    this.markers = [];
    this.currentBar = null;
    this.resizeObserver = null;
    this.timeframeSeconds = 60; // default 1m = 60s
  }

  /**
   * Initialize Lightweight Chart inside DOM container with dark surveillance theme.
   */
  init() {
    if (!this.container) {
      return false;
    }

    if (typeof LightweightCharts === 'undefined') {
      return false;
    }

    // Chart visual theme options
    const chartOptions = {
      layout: {
        background: { type: 'solid', color: '#090e1a' },
        textColor: '#94a3b8',
        fontSize: 11,
        fontFamily: "'JetBrains Mono', monospace",
      },
      grid: {
        vertLines: { color: 'rgba(255, 255, 255, 0.04)' },
        horzLines: { color: 'rgba(255, 255, 255, 0.04)' },
      },
      crosshair: {
        mode: 1, // Magnet mode
        vertLine: {
          color: 'rgba(0, 242, 254, 0.4)',
          width: 1,
          style: 2,
        },
        horzLine: {
          color: 'rgba(0, 242, 254, 0.4)',
          width: 1,
          style: 2,
        },
      },
      timeScale: {
        borderColor: 'rgba(255, 255, 255, 0.08)',
        timeVisible: true,
        secondsVisible: false,
      },
      rightPriceScale: {
        borderColor: 'rgba(255, 255, 255, 0.08)',
        autoScale: true,
      },
    };

    this.chart = LightweightCharts.createChart(this.container, chartOptions);

    // Candlestick Series
    this.candleSeries = this.chart.addCandlestickSeries({
      upColor: '#10b981',
      downColor: '#ef4444',
      borderVisible: false,
      wickUpColor: '#10b981',
      wickDownColor: '#ef4444',
    });

    // Volume Histogram Series (Bottom 25% height)
    this.volumeSeries = this.chart.addHistogramSeries({
      color: '#26a69a',
      priceFormat: { type: 'volume' },
      priceScaleId: '', // overlay
      scaleMargins: {
        top: 0.8,
        bottom: 0,
      },
    });

    // Responsive Auto-Resize
    this.resizeObserver = new ResizeObserver((entries) => {
      if (!entries || entries.length === 0 || !this.chart) {
        return;
      }
      const { width, height } = entries[0].contentRect;
      this.chart.applyOptions({ width, height });
    });
    this.resizeObserver.observe(this.container);

    return true;
  }

  /**
   * Set timeframe seconds for bucket alignment (1m=60, 5m=300, etc.)
   */
  setTimeframe(tfStr) {
    const map = {
      '1m': 60,
      '5m': 300,
      '15m': 900,
      '1h': 3600,
      '1d': 86400,
    };
    this.timeframeSeconds = map[tfStr] || 60;
  }

  /**
   * Populate chart with initial candle bars array from REST API.
   * Format: [{ time: number, open: number, high: number, low: number, close: number, volume: number }]
   */
  setData(candles) {
    if (!this.candleSeries || !Array.isArray(candles)) {
      return;
    }

    this.markers = [];
    if (this.candleSeries.setMarkers) {
      this.candleSeries.setMarkers([]);
    }

    if (candles.length === 0) {
      this.currentBar = null;
      return;
    }

    // Ensure sorted strictly by ascending timestamp
    const sorted = [...candles].sort((a, b) => a.time - b.time);

    // Filter duplicate timestamps to prevent Lightweight Charts crash
    const dedupCandles = [];
    const dedupVolumes = [];
    let lastTime = 0;

    for (const c of sorted) {
      if (c.time > lastTime) {
        dedupCandles.push({
          time: c.time,
          open: c.open,
          high: c.high,
          low: c.low,
          close: c.close,
        });

        const isUp = c.close >= c.open;
        dedupVolumes.push({
          time: c.time,
          value: c.volume || 0,
          color: isUp ? 'rgba(16, 185, 129, 0.35)' : 'rgba(239, 68, 68, 0.35)',
        });

        lastTime = c.time;
      }
    }

    this.candleSeries.setData(dedupCandles);
    if (this.volumeSeries) {
      this.volumeSeries.setData(dedupVolumes);
    }

    this.currentBar = dedupCandles.length > 0 ? { ...dedupCandles[dedupCandles.length - 1] } : null;

    if (this.chart) {
      this.chart.timeScale().fitContent();
    }
  }

  /**
   * Update active candle bar in real time from incoming 4Hz price tick.
   */
  updatePriceTick(price, timestampMs) {
    if (!this.candleSeries || !price) {
      return;
    }

    const tickSeconds = Math.floor((timestampMs || Date.now()) / 1000);
    const bucketTime = Math.floor(tickSeconds / this.timeframeSeconds) * this.timeframeSeconds;

    if (!this.currentBar || this.currentBar.time < bucketTime) {
      // Start new candle bucket
      const newBar = {
        time: bucketTime,
        open: price,
        high: price,
        low: price,
        close: price,
      };
      this.currentBar = newBar;
      this.candleSeries.update(newBar);
      if (this.volumeSeries) {
        this.volumeSeries.update({
          time: bucketTime,
          value: 0,
          color: 'rgba(16, 185, 129, 0.35)',
        });
      }
    } else {
      // Update existing candle bar
      this.currentBar.high = Math.max(this.currentBar.high, price);
      this.currentBar.low = Math.min(this.currentBar.low, price);
      this.currentBar.close = price;
      this.candleSeries.update(this.currentBar);
    }
  }

  /**
   * Attach visual Anomaly Marker directly onto the corresponding candle bar.
   */
  addAnomalyMarker(alert) {
    if (!this.candleSeries || !alert) {
      return;
    }

    const eventTimeMs = alert.event_time ? new Date(alert.event_time).getTime() : Date.now();
    const eventSec = Math.floor(eventTimeMs / 1000);
    const bucketTime = Math.floor(eventSec / this.timeframeSeconds) * this.timeframeSeconds;

    const alertType = String(alert.alert_type || alert.type || 'ANOMALY').toUpperCase();
    let shape = 'circle';
    let color = '#38bdf8';
    let position = 'aboveBar';
    let text = 'ALERT';

    if (alertType.includes('WHALE')) {
      shape = 'arrowDown';
      color = '#38bdf8';
      position = 'aboveBar';
      const usd = alert.notional_usd ? `$${(alert.notional_usd / 1e6).toFixed(1)}M` : '$1M+';
      text = `🐋 Whale ${usd}`;
    } else if (alertType.includes('LIQ')) {
      shape = 'arrowUp';
      color = '#ef4444';
      position = 'belowBar';
      const usd = alert.notional_usd ? `$${(alert.notional_usd / 1e3).toFixed(0)}k` : '$500k+';
      text = `💀 Liq ${usd}`;
    } else if (alertType.includes('VOL') || alertType.includes('ZSCORE')) {
      shape = 'circle';
      color = '#fbbf24';
      position = 'aboveBar';
      const z = alert.zscore ? `${Number(alert.zscore).toFixed(1)}σ` : '4.0σ';
      text = `⚡ Z-Score ${z}`;
    }

    const marker = {
      time: bucketTime,
      position,
      color,
      shape,
      text,
    };

    this.markers.push(marker);
    // Sort markers by time
    this.markers.sort((a, b) => a.time - b.time);

    // Keep max 50 markers
    if (this.markers.length > 50) {
      this.markers.shift();
    }

    if (typeof this.candleSeries.setMarkers === 'function') {
      this.candleSeries.setMarkers(this.markers);
    }
  }

  /**
   * Clean up observers and chart instance.
   */
  destroy() {
    if (this.resizeObserver && this.container) {
      this.resizeObserver.unobserve(this.container);
      this.resizeObserver.disconnect();
    }
    if (this.chart) {
      this.chart.remove();
      this.chart = null;
    }
    this.candleSeries = null;
    this.volumeSeries = null;
    this.currentBar = null;
    this.markers = [];
  }
}

window.SurveillanceChart = SurveillanceChart;
