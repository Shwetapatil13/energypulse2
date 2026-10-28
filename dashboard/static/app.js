/**
 * EnergyPulse Frontend Application Logic
 * DASSS Big Data Case Study - PICT Pune
 * 
 * Manages WebSocket real-time communication, metric updates, Chart.js graphs,
 * and page navigation across the dark-themed single-page application.
 */

document.addEventListener("DOMContentLoaded", () => {
    // -------------------------------------------------------------
    // 1. Application State Variables
    // -------------------------------------------------------------
    const state = {
        ws: null,                   // WebSocket client instance
        reconnectAttempts: 0,       // Counter for auto-reconnect backoff
        maxChartPoints: 40,         // Sliding window size for real-time chart
        liveLabels: [],             // Time labels on chart x-axis
        liveDataPoints: [],         // Active power values on chart y-axis
        thresholdPoints: [],        // Red reference line at 5.0 kW
        alerts: [],                 // List of high-power alerts
        tariff: 7.50,               // Current tariff in ₹ per kWh
        threshold: 5.0,             // Current alert threshold in kW
        historicalData: null,       // Cached historical analytics payload
        classificationData: null    // Cached load classification payload
    };

    // -------------------------------------------------------------
    // 2. DOM Elements Cache
    // -------------------------------------------------------------
    const el = {
        navItems: document.querySelectorAll(".nav-item"),
        pageSections: document.querySelectorAll(".page-section"),
        tabButtons: document.querySelectorAll(".tab-btn"),
        tabContents: document.querySelectorAll(".tab-content"),
        wsStatusContainer: document.getElementById("ws-status-container"),
        wsStatusText: document.getElementById("ws-status-text"),
        liveClock: document.getElementById("live-clock"),
        sidebarAlertBadge: document.getElementById("sidebar-alert-badge"),

        // Metric Card Values
        valGap: document.getElementById("val-gap"),
        valGrp: document.getElementById("val-grp"),
        valVolt: document.getElementById("val-volt"),
        valCost: document.getElementById("val-cost"),
        valKwhMin: document.getElementById("val-kwh-min"),
        valTimestamp: document.getElementById("val-timestamp"),
        valIntensityFooter: document.getElementById("val-intensity-footer"),
        badgeLoadClass: document.getElementById("badge-load-class"),
        valAnomaly: document.getElementById("val-anomaly"),
        anomalyCard: document.getElementById("anomaly-card"),
        tariffLabel: document.getElementById("tariff-label"),
        valSm1: document.getElementById("val-sm1"),
        valSm2: document.getElementById("val-sm2"),
        valSm3: document.getElementById("val-sm3"),

        // Alerts & Tables
        alertsTbody: document.getElementById("alerts-tbody"),
        clearAlertsBtn: document.getElementById("clear-alerts-btn"),
        recommendationsContainer: document.getElementById("recommendations-container"),
        heatmapGrid: document.getElementById("heatmap-grid"),
        settingsForm: document.getElementById("settings-form"),
        inputTariff: document.getElementById("input-tariff"),
        inputThreshold: document.getElementById("input-threshold"),
        exportCsvBtn: document.getElementById("export-csv-btn")
    };

    // -------------------------------------------------------------
    // 3. Live System Clock
    // -------------------------------------------------------------
    function updateClock() {
        const now = new Date();
        el.liveClock.textContent = now.toLocaleTimeString();
    }
    setInterval(updateClock, 1000);
    updateClock();

    // -------------------------------------------------------------
    // 4. Sidebar Page Switching (Single-Page App Navigation)
    // -------------------------------------------------------------
    function switchPage(pageId) {
        // Update active navigation link in sidebar
        el.navItems.forEach(item => {
            if (item.getAttribute("data-page") === pageId) {
                item.classList.add("active");
            } else {
                item.classList.remove("active");
            }
        });

        // Show selected section, hide other sections
        el.pageSections.forEach(section => {
            if (section.id === `page-${pageId}`) {
                section.classList.add("active");
            } else {
                section.classList.remove("active");
            }
        });

        // Lazily fetch data or render charts when user navigates to a tab
        if (pageId === "historical" && state.historicalData) {
            renderHistoricalCharts();
        } else if (pageId === "classification" && state.classificationData) {
            renderClassificationChart();
        } else if (pageId === "forecast") {
            fetchForecastData();
        }
    }

    el.navItems.forEach(item => {
        item.addEventListener("click", (e) => {
            e.preventDefault();
            const pageId = item.getAttribute("data-page");
            switchPage(pageId);
        });
    });

    // -------------------------------------------------------------
    // 5. Historical Sub-Tabs (Hourly, Daily, Heatmap)
    // -------------------------------------------------------------
    el.tabButtons.forEach(btn => {
        btn.addEventListener("click", () => {
            const tabId = btn.getAttribute("data-tab");
            el.tabButtons.forEach(b => b.classList.remove("active"));
            el.tabContents.forEach(c => c.classList.remove("active"));
            btn.classList.add("active");
            document.getElementById(`tab-${tabId}`).classList.add("active");
        });
    });

    // -------------------------------------------------------------
    // 6. Chart.js Setup: Real-Time Active Power Chart
    // -------------------------------------------------------------
    Chart.defaults.color = "#9ca3af";
    Chart.defaults.font.family = "'Outfit', sans-serif";

    const liveCtx = document.getElementById("liveChart").getContext("2d");
    const liveGradient = liveCtx.createLinearGradient(0, 0, 0, 300);
    liveGradient.addColorStop(0, "rgba(6, 182, 212, 0.45)");
    liveGradient.addColorStop(1, "rgba(6, 182, 212, 0.0)");

    const liveChart = new Chart(liveCtx, {
        type: "line",
        data: {
            labels: state.liveLabels,
            datasets: [
                {
                    label: "Active Power (kW)",
                    data: state.liveDataPoints,
                    borderColor: "#06b6d4",
                    borderWidth: 2.5,
                    backgroundColor: liveGradient,
                    fill: true,
                    tension: 0.35,
                    pointRadius: 2,
                    pointHoverRadius: 6
                },
                {
                    label: "Alert Threshold (5.0 kW)",
                    data: state.thresholdPoints,
                    borderColor: "#ef4444",
                    borderWidth: 1.5,
                    borderDash: [5, 5],
                    fill: false,
                    pointRadius: 0
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 250 },
            scales: {
                x: {
                    grid: { color: "rgba(255, 255, 255, 0.05)" },
                    ticks: { maxTicksLimit: 10 }
                },
                y: {
                    min: 0,
                    suggestedMax: 6.5,
                    grid: { color: "rgba(255, 255, 255, 0.05)" },
                    title: { display: true, text: "Active Power (kW)" }
                }
            },
            plugins: {
                legend: { display: false }
            }
        }
    });

    // -------------------------------------------------------------
    // 7. WebSocket Client with Auto-Reconnect
    // -------------------------------------------------------------
    function connectWebSocket() {
        const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
        const wsUrl = `${protocol}//${window.location.host}/ws`;

        el.wsStatusText.textContent = "Connecting...";
        const dot = el.wsStatusContainer.querySelector(".status-dot");
        dot.style.backgroundColor = "#f59e0b";
        dot.style.boxShadow = "0 0 10px #f59e0b";

        state.ws = new WebSocket(wsUrl);

        state.ws.onopen = () => {
            console.log("[WS] Connected to EnergyPulse server.");
            el.wsStatusText.textContent = "WS Connected";
            dot.style.backgroundColor = "#10b981";
            dot.style.boxShadow = "0 0 10px #10b981";
            state.reconnectAttempts = 0;
        };

        state.ws.onmessage = (event) => {
            try {
                const msg = JSON.parse(event.data);
                // Handle live telemetry packet
                if (msg.type === "telemetry" && msg.data) {
                    handleTelemetry(msg.data);
                } else if (msg.type === "init") {
                    // Handle initial state packet upon connecting
                    if (msg.tariff) state.tariff = msg.tariff;
                    if (msg.threshold) state.threshold = msg.threshold;
                    if (msg.readings) {
                        msg.readings.forEach(r => handleTelemetry(r, false));
                        liveChart.update();
                    }
                    if (msg.alerts) {
                        msg.alerts.forEach(a => recordAlert(a));
                    }
                }
            } catch (err) {
                console.error("[WS] Message parsing error:", err);
            }
        };

        state.ws.onclose = () => {
            // Reconnect automatically if connection drops
            el.wsStatusText.textContent = "Disconnected";
            dot.style.backgroundColor = "#ef4444";
            dot.style.boxShadow = "0 0 10px #ef4444";
            state.reconnectAttempts++;
            const timeout = Math.min(5000, 1000 * state.reconnectAttempts);
            setTimeout(connectWebSocket, timeout);
        };

        state.ws.onerror = (err) => {
            console.warn("[WS] Error:", err);
            state.ws.close();
        };
    }

    // -------------------------------------------------------------
    // 8. Telemetry Metric Update Handler
    // -------------------------------------------------------------
    function handleTelemetry(reading, updateChartNow = true) {
        const gap = Number(reading.Global_active_power || 0);
        const grp = Number(reading.Global_reactive_power || 0);
        const volt = Number(reading.Voltage || 0);
        const intensity = Number(reading.Global_intensity || 0);
        const energyKwhMin = Number(reading.energy_kwh_min || (gap / 60));
        const costMin = Number(reading.cost_inr_min || (energyKwhMin * state.tariff));
        const timestamp = reading.timestamp || reading.Time || "--";
        const isAlert = gap > state.threshold;

        // Update card values in DOM
        el.valGap.textContent = gap.toFixed(3);
        el.valGrp.textContent = grp.toFixed(3);
        el.valVolt.textContent = volt.toFixed(1);
        el.valCost.textContent = `₹${costMin.toFixed(2)}`;
        el.valKwhMin.textContent = energyKwhMin.toFixed(4);
        el.valTimestamp.textContent = `Time: ${timestamp.replace("T", " ")}`;
        el.valIntensityFooter.textContent = `Current: ${intensity.toFixed(1)} A`;

        el.valSm1.textContent = `${Number(reading.Sub_metering_1 || 0).toFixed(1)} Wh`;
        el.valSm2.textContent = `${Number(reading.Sub_metering_2 || 0).toFixed(1)} Wh`;
        el.valSm3.textContent = `${Number(reading.Sub_metering_3 || 0).toFixed(1)} Wh`;

        // Update load classification badge
        const loadClass = reading.load_class || (gap < 1 ? "Low (Standby)" : gap <= 3 ? "Normal" : gap <= 5 ? "High" : "Critical");
        el.badgeLoadClass.textContent = loadClass;

        // Anomaly highlight if power > 5.0 kW
        if (isAlert) {
            el.valAnomaly.textContent = "YES (ALERT)";
            el.valAnomaly.style.color = "#ef4444";
            el.anomalyCard.style.borderColor = "rgba(239, 68, 68, 0.7)";
            el.anomalyCard.style.boxShadow = "0 0 15px rgba(239, 68, 68, 0.3)";
            recordAlert(reading);
        } else {
            el.valAnomaly.textContent = "No";
            el.valAnomaly.style.color = "#10b981";
            el.anomalyCard.style.borderColor = "var(--border-color)";
            el.anomalyCard.style.boxShadow = "none";
        }

        // Push new point into chart sliding window
        const timeLabel = timestamp.includes("T") ? timestamp.split("T")[1].substring(0, 8) : timestamp;
        state.liveLabels.push(timeLabel);
        state.liveDataPoints.push(gap);
        state.thresholdPoints.push(state.threshold);

        // Keep at most maxChartPoints on screen
        if (state.liveLabels.length > state.maxChartPoints) {
            state.liveLabels.shift();
            state.liveDataPoints.shift();
            state.thresholdPoints.shift();
        }

        if (updateChartNow) {
            liveChart.update();
        }
    }

    // -------------------------------------------------------------
    // 9. Alert List Manager
    // -------------------------------------------------------------
    function recordAlert(alertItem) {
        // Avoid duplicate alerts for the same timestamp
        if (state.alerts.length > 0 && state.alerts[0].timestamp === alertItem.timestamp) {
            return;
        }

        state.alerts.unshift(alertItem);
        if (state.alerts.length > 50) state.alerts.pop();

        el.sidebarAlertBadge.textContent = state.alerts.length;

        // Prepend new alert row into the Alerts table
        if (el.alertsTbody) {
            if (state.alerts.length === 1) el.alertsTbody.innerHTML = "";

            const row = document.createElement("tr");
            row.className = "alert-row";
            const gap = Number(alertItem.Global_active_power).toFixed(3);
            const volt = Number(alertItem.Voltage).toFixed(1);
            const gi = Number(alertItem.Global_intensity).toFixed(1);
            const ts = alertItem.timestamp ? alertItem.timestamp.replace("T", " ") : alertItem.Date + " " + alertItem.Time;

            row.innerHTML = `
                <td><strong>${ts}</strong></td>
                <td><strong style="color: #ef4444;">${gap} kW</strong></td>
                <td>${volt} V</td>
                <td>${gi} A</td>
                <td><span class="badge badge-alert">HIGH LOAD TRIGGER</span></td>
                <td>Power &gt; ${state.threshold} kW (Rule)</td>
            `;

            el.alertsTbody.insertBefore(row, el.alertsTbody.firstChild);
        }
    }

    if (el.clearAlertsBtn) {
        el.clearAlertsBtn.addEventListener("click", () => {
            state.alerts = [];
            el.sidebarAlertBadge.textContent = "0";
            el.alertsTbody.innerHTML = `<tr><td colspan="6" class="text-center">Listening for alert events...</td></tr>`;
        });
    }

    // -------------------------------------------------------------
    // 10. Historical Analytics Charts & 24x7 Heatmap
    // -------------------------------------------------------------
    let hourlyChartInstance = null;
    let dailyChartInstance = null;

    async function fetchHistoricalData() {
        try {
            const res = await fetch("/api/historical");
            state.historicalData = await res.json();
            renderHistoricalCharts();
        } catch (err) {
            console.error("Failed to load historical analytics:", err);
        }
    }

    function renderHistoricalCharts() {
        if (!state.historicalData) return;

        // 1. Hourly Bar Chart (24 Hours)
        const hourlyCtx = document.getElementById("hourlyChart");
        if (hourlyCtx) {
            if (hourlyChartInstance) hourlyChartInstance.destroy();
            const labels = state.historicalData.hourly.map(h => `${h.hour}:00`);
            const data = state.historicalData.hourly.map(h => h.avg_kw);

            hourlyChartInstance = new Chart(hourlyCtx, {
                type: "bar",
                data: {
                    labels: labels,
                    datasets: [{
                        label: "Average Active Power (kW)",
                        data: data,
                        backgroundColor: "rgba(6, 182, 212, 0.65)",
                        borderColor: "#06b6d4",
                        borderWidth: 1,
                        borderRadius: 4
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        x: { grid: { color: "rgba(255, 255, 255, 0.05)" } },
                        y: { min: 0, grid: { color: "rgba(255, 255, 255, 0.05)" }, title: { display: true, text: "Mean kW" } }
                    }
                }
            });
        }

        // 2. Day of Week Line Chart (7 Days)
        const dailyCtx = document.getElementById("dailyChart");
        if (dailyCtx) {
            if (dailyChartInstance) dailyChartInstance.destroy();
            const labels = state.historicalData.daily.map(d => d.day);
            const data = state.historicalData.daily.map(d => d.avg_kw);

            dailyChartInstance = new Chart(dailyCtx, {
                type: "line",
                data: {
                    labels: labels,
                    datasets: [{
                        label: "Day-of-Week Average Load (kW)",
                        data: data,
                        borderColor: "#8b5cf6",
                        backgroundColor: "rgba(139, 92, 246, 0.2)",
                        fill: true,
                        tension: 0.3,
                        pointRadius: 5
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        x: { grid: { color: "rgba(255, 255, 255, 0.05)" } },
                        y: { min: 0, grid: { color: "rgba(255, 255, 255, 0.05)" } }
                    }
                }
            });
        }

        // 3. 24h x 7d Matrix Heatmap
        if (el.heatmapGrid && state.historicalData.heatmap) {
            el.heatmapGrid.innerHTML = "";
            const emptyCorner = document.createElement("div");
            emptyCorner.className = "heatmap-label";
            emptyCorner.textContent = "Day / Hr";
            el.heatmapGrid.appendChild(emptyCorner);

            for (let h = 0; h < 24; h++) {
                const hourHeader = document.createElement("div");
                hourHeader.className = "heatmap-label";
                hourHeader.style.justifyContent = "center";
                hourHeader.textContent = `${h}`;
                el.heatmapGrid.appendChild(hourHeader);
            }

            const days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
            days.forEach((day) => {
                const dayLabel = document.createElement("div");
                dayLabel.className = "heatmap-label";
                dayLabel.textContent = day.substring(0, 3);
                el.heatmapGrid.appendChild(dayLabel);

                const dayCells = state.historicalData.heatmap.filter(c => c.day === day);
                dayCells.sort((a, b) => a.hour - b.hour);

                dayCells.forEach(cell => {
                    const div = document.createElement("div");
                    div.className = "heatmap-cell";
                    div.textContent = cell.avg_kw.toFixed(1);
                    div.title = `${day} at ${cell.hour}:00 - Avg ${cell.avg_kw} kW`;

                    // Dynamic color gradient based on power consumption
                    const p = Math.min(1.0, Math.max(0.0, (cell.avg_kw - 0.3) / 1.5));
                    if (p < 0.25) {
                        div.style.backgroundColor = "#1e293b";
                    } else if (p < 0.5) {
                        div.style.backgroundColor = "#0369a1";
                    } else if (p < 0.75) {
                        div.style.backgroundColor = "#06b6d4";
                        div.style.color = "#0b0f19";
                    } else {
                        div.style.backgroundColor = "#ef4444";
                    }
                    el.heatmapGrid.appendChild(div);
                });
            });
        }
    }

    // -------------------------------------------------------------
    // 11. Load Level Classification Doughnut Chart
    // -------------------------------------------------------------
    let classificationChartInstance = null;
    async function fetchClassificationData() {
        try {
            const res = await fetch("/api/classification");
            state.classificationData = await res.json();
            renderClassificationChart();
        } catch (err) {
            console.error("Classification fetch failed:", err);
        }
    }

    function renderClassificationChart() {
        const ctx = document.getElementById("classificationChart");
        if (!ctx || !state.classificationData) return;

        if (classificationChartInstance) classificationChartInstance.destroy();

        classificationChartInstance = new Chart(ctx, {
            type: "doughnut",
            data: {
                labels: state.classificationData.bands.map(b => b.band),
                datasets: [{
                    data: state.classificationData.bands.map(b => b.percentage),
                    backgroundColor: ["#10b981", "#06b6d4", "#f59e0b", "#ef4444"],
                    borderWidth: 0
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { position: "right" }
                }
            }
        });
    }

    // -------------------------------------------------------------
    // 12. Near-Term Moving Average Forecast Chart
    // -------------------------------------------------------------
    let forecastChartInstance = null;
    async function fetchForecastData() {
        try {
            const res = await fetch("/api/forecast");
            const data = await res.json();
            const ctx = document.getElementById("forecastChart");
            if (!ctx) return;

            if (forecastChartInstance) forecastChartInstance.destroy();

            const labels = data.forecast_points.map(p => `+${p.minute_ahead}m (${p.time})`);
            const projected = data.forecast_points.map(p => p.projected_kw);
            const upper = data.forecast_points.map(p => p.upper_bound);
            const lower = data.forecast_points.map(p => p.lower_bound);

            forecastChartInstance = new Chart(ctx, {
                type: "line",
                data: {
                    labels: labels,
                    datasets: [
                        {
                            label: "Forecasted Active Power (kW)",
                            data: projected,
                            borderColor: "#38bdf8",
                            backgroundColor: "rgba(56, 189, 248, 0.2)",
                            fill: false,
                            tension: 0.3,
                            pointRadius: 4
                        },
                        {
                            label: "Upper Margin (95% CI)",
                            data: upper,
                            borderColor: "rgba(245, 158, 11, 0.4)",
                            borderDash: [4, 4],
                            fill: false,
                            pointRadius: 0
                        },
                        {
                            label: "Lower Margin (95% CI)",
                            data: lower,
                            borderColor: "rgba(245, 158, 11, 0.4)",
                            borderDash: [4, 4],
                            fill: false,
                            pointRadius: 0
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        x: { grid: { color: "rgba(255, 255, 255, 0.05)" } },
                        y: { min: 0, grid: { color: "rgba(255, 255, 255, 0.05)" }, title: { display: true, text: "Demand (kW)" } }
                    }
                }
            });
        } catch (err) {
            console.error("Forecast fetch failed:", err);
        }
    }

    // -------------------------------------------------------------
    // 13. Context-Aware Energy Saving Recommendations
    // -------------------------------------------------------------
    async function fetchRecommendations() {
        try {
            const res = await fetch("/api/recommendations");
            const data = await res.json();
            if (el.recommendationsContainer && data.tips) {
                el.recommendationsContainer.innerHTML = data.tips.map(tip => `
                    <div class="rec-card">
                        <div>
                            <div class="rec-title">${tip.title}</div>
                            <div class="rec-condition">Condition: ${tip.condition}</div>
                            <div class="rec-action">${tip.action}</div>
                        </div>
                    </div>
                `).join("");
            }
        } catch (err) {
            console.error("Recommendations fetch error:", err);
        }
    }

    // -------------------------------------------------------------
    // 14. Settings Form (Tariff & Threshold Configuration)
    // -------------------------------------------------------------
    if (el.settingsForm) {
        el.settingsForm.addEventListener("submit", async (e) => {
            e.preventDefault();
            const newTariff = parseFloat(el.inputTariff.value);
            const newThreshold = parseFloat(el.inputThreshold.value);

            try {
                await fetch("/api/settings", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ tariff: newTariff, threshold: newThreshold })
                });
                state.tariff = newTariff;
                state.threshold = newThreshold;
                el.tariffLabel.textContent = `Tariff: ₹${newTariff.toFixed(2)} / kWh`;
                alert("Settings saved successfully!");
            } catch (err) {
                alert("Error saving settings.");
            }
        });
    }

    // -------------------------------------------------------------
    // 15. CSV Export Handler
    // -------------------------------------------------------------
    if (el.exportCsvBtn) {
        el.exportCsvBtn.addEventListener("click", () => {
            let csv = "Timestamp,Active_Power_kW,Voltage_V,Current_A,Load_Class,Alert\n";
            state.alerts.forEach(a => {
                csv += `"${a.timestamp || ''}",${a.Global_active_power},${a.Voltage},${a.Global_intensity},"${a.load_class}","${a.anomaly}"\n`;
            });
            const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
            const url = URL.createObjectURL(blob);
            const link = document.createElement("a");
            link.setAttribute("href", url);
            link.setAttribute("download", `energypulse_audit_report_${Date.now()}.csv`);
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
        });
    }

    // -------------------------------------------------------------
    // 16. App Bootstrapping
    // -------------------------------------------------------------
    connectWebSocket();
    fetchHistoricalData();
    fetchClassificationData();
    fetchRecommendations();
});
