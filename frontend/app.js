let forecastChart;
let inventorySortDir = {};

document.addEventListener('DOMContentLoaded', () => {
    const token = localStorage.getItem('accessToken');
    if (!token) {
        window.location.href = '/';
        return;
    }

    initializeDashboard(token);
    setupTabListeners();
    setupInventoryTableListener(token); // Attach listener once
});

async function initializeDashboard(token) {
    try {
        await Promise.all([
            fetchUserInfo(token),
            fetchSummaryData(token),
            fetchAlerts(token),
            fetchInsights(token), 
            populateProductSelector(token)
        ]);
    } catch (error) {
        console.error('Dashboard initialization error:', error);
        if (error.message === 'UNAUTHORIZED') {
            localStorage.removeItem('accessToken');
            window.location.href = '/';
        }
    }

    document.getElementById('logout-button').addEventListener('click', handleLogout);
}

function setupTabListeners() {
    const tabs = document.querySelectorAll('.tab-link');
    const tabContents = document.querySelectorAll('.tab-content');

    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            tabs.forEach(t => t.classList.remove('active'));
            tab.classList.add('active');

            tabContents.forEach(content => {
                content.classList.remove('active');
            });

            document.getElementById(tab.dataset.tab).classList.add('active');

            if (tab.dataset.tab === 'forecast-analytics-tab' && forecastChart) {
                setTimeout(() => forecastChart.resize(), 50);
            }
        });
    });
}

// CORRECTED LOGIC: Now switches to the correct tab for forecasts
function setupInventoryTableListener(token) {
    const tableBody = document.querySelector('#inventory-table tbody');
    
    tableBody.addEventListener('click', (event) => {
        const row = event.target.closest('tr');
        if (!row || !row.dataset.productId) return;

        const productId = row.dataset.productId;
        
        // Find the Forecasts & Analytics tab button and click it
        const forecastTabButton = document.querySelector('.tab-link[data-tab="forecast-analytics-tab"]');
        if (forecastTabButton) {
            forecastTabButton.click();
        }

        // Update the forecast dropdown and fetch the new data
        const selector = document.getElementById('product-selector');
        selector.value = productId;
        fetchForecast(productId, token);
    });
}

function handleLogout() {
    localStorage.removeItem('accessToken');
    window.location.href = '/';
}

async function fetchInsights(token) {
    const listContainer = document.getElementById('insights-list');
    try {
        const response = await fetch('/api/insights/frequently-bought-together', {
            headers: { 'Authorization': `Bearer ${token}` }
        });
        if (!response.ok) throw new Error('Failed to fetch insights');
        const rules = await response.json();

        if (rules.length === 0) {
            listContainer.innerHTML = '<p style="padding: 1rem; text-align: center;">Not enough data to find purchase patterns yet.</p>';
            return;
        }

        let html = '<ul>';
        rules.forEach(rule => {
            html += `
                <li>
                    <span>When a customer buys <b>${rule.antecedents}</b>...</span>
                    <span style="font-size: 0.9em; color: var(--text-light);">
                        ...they also buy <b>${rule.consequents}</b> ${(rule.confidence * 100).toFixed(0)}% of the time.
                    </span>
                </li>
            `;
        });
        html += '</ul>';
        listContainer.innerHTML = html;

    } catch (error) {
        console.error('Error fetching insights:', error);
        listContainer.innerHTML = '<p class="error-row">Could not load insights.</p>';
    }
}

async function fetchAlerts(token) {
    const listContainer = document.getElementById('alerts-list');
    try {
        const response = await fetch('/api/alerts', {
            headers: { 'Authorization': `Bearer ${token}` }
        });
        if (response.status === 401) throw new Error('UNAUTHORIZED');
        if (!response.ok) throw new Error('Failed to fetch alerts');

        const alerts = await response.json();
        
        if (alerts.length === 0) {
            listContainer.innerHTML = '<p style="padding: 1rem; text-align: center;">No active alerts. Good job!</p>';
            return;
        }

        let html = '<ul>';
        alerts.forEach(alert => {
            html += `
                <li>
                    <span class="alert-product">${alert.product_id}</span>
                    <span class="alert-message">${alert.alert_message}</span>
                </li>
            `;
        });
        html += '</ul>';
        listContainer.innerHTML = html;

    } catch (error) {
        console.error('Error fetching alerts:', error);
        listContainer.innerHTML = '<p class="error-row">Could not load alerts.</p>';
        throw error;
    }
}

async function fetchUserInfo(token) {
    try {
        const response = await fetch('/api/users/me', {
            headers: { 'Authorization': `Bearer ${token}` }
        });
        
        if (response.status === 401) throw new Error('UNAUTHORIZED');
        if (!response.ok) throw new Error('Failed to fetch user info');
        
        const userData = await response.json();
        document.getElementById('welcome-message').textContent = `Welcome, ${userData.full_name}`;
    } catch (error) {
        console.error('Error fetching user info:', error);
        throw error;
    }
}

async function fetchSummaryData(token) {
    try {
        const response = await fetch('/api/summary', {
            headers: { 'Authorization': `Bearer ${token}` }
        });
        
        if (response.status === 401) throw new Error('UNAUTHORIZED');
        if (!response.ok) throw new Error('Failed to fetch summary');
        
        const data = await response.json();

        if (data.total_products === 0 && data.total_units_in_stock === 0) {
            displayEmptyState(token);
        } else {
            hideEmptyState();
            animateCounter('kpi-total-products', data.total_products);
            animateCounter('kpi-total-units', data.total_units_in_stock);
            animateCounter('kpi-sales-24h', data.sales_last_24h);
            animateCounter('kpi-low-stock', data.low_stock_items);
        }
    } catch (error)
 {
        console.error('Error fetching summary data:', error);
        if (error.message !== 'UNAUTHORIZED') {
            displayEmptyState(token);
        }
        throw error;
    }
}

function displayEmptyState(token) {
    document.getElementById('empty-state-container').style.display = 'block';
    document.querySelector('.kpi-grid').style.display = 'none';
    document.querySelector('.container-wrapper').style.display = 'none';

    const genButton = document.getElementById('generate-data-button');
    const statusText = document.getElementById('generation-status');
    
    if (genButton.dataset.listenerAttached) return;
    genButton.dataset.listenerAttached = 'true';

    genButton.addEventListener('click', async () => {
        genButton.disabled = true;
        genButton.querySelector('.button-text').textContent = 'Generating...';
        statusText.textContent = 'Please wait, this can take up to 30 seconds. The page will reload automatically.';

        try {
            const response = await fetch('/api/users/me/generate-demo-data', {
                method: 'POST',
                headers: { 'Authorization': `Bearer ${token}` }
            });

            if (response.ok) {
                setTimeout(() => {
                    window.location.reload();
                }, 25000); 
            } else {
                 statusText.textContent = 'An error occurred. Please try refreshing the page.';
                 genButton.disabled = false;
                 genButton.querySelector('.button-text').textContent = 'Generate Demo Data';
            }
        } catch (error) {
            console.error('Error generating data:', error);
            statusText.textContent = 'A network error occurred. Please try again.';
            genButton.disabled = false;
            genButton.querySelector('.button-text').textContent = 'Generate Demo Data';
        }
    });
}

function hideEmptyState() {
    document.getElementById('empty-state-container').style.display = 'none';
    document.querySelector('.kpi-grid').style.display = 'grid';
    document.querySelector('.container-wrapper').style.display = 'block';
}

function animateCounter(elementId, targetValue) {
    const element = document.getElementById(elementId);
    const duration = 1000;
    const startTime = performance.now();

    function update(currentTime) {
        const elapsed = currentTime - startTime;
        const progress = Math.min(elapsed / duration, 1);
        const current = Math.floor(progress * targetValue);
        element.textContent = current.toLocaleString();
        
        if (progress < 1) {
            requestAnimationFrame(update);
        } else {
            element.textContent = targetValue.toLocaleString();
        }
    }
    
    requestAnimationFrame(update);
}

async function populateProductSelector(token) {
    try {
        const response = await fetch('/api/inventory', {
            headers: { 'Authorization': `Bearer ${token}` }
        });

        if (response.status === 401) throw new Error('UNAUTHORIZED');
        if (!response.ok) throw new Error('Failed to fetch inventory');

        const inventoryData = await response.json();
        
        const selector = document.getElementById('product-selector');
        selector.innerHTML = '<option value="">Select a product...</option>';
        
        inventoryData.forEach(product => {
            const option = document.createElement('option');
            option.value = product.product_id;
            option.textContent = `${product.product_id} (Stock: ${product.stock_level})`;
            selector.appendChild(option);
        });

        selector.addEventListener('change', (event) => {
            if (event.target.value) {
                fetchForecast(event.target.value, token);
            }
        });

        if (inventoryData.length > 0) {
            displayInventory(inventoryData);
            selector.value = inventoryData[0].product_id;
            fetchForecast(inventoryData[0].product_id, token);
        }
    } catch (error) {
        console.error('Error populating product selector:', error);
        throw error;
    }
}

function displayInventory(data) {
    const tableBody = document.querySelector('#inventory-table tbody');
    tableBody.innerHTML = '';
    
    data.forEach((product, index) => {
        const row = document.createElement('tr');
        row.style.animationDelay = `${index * 0.05}s`;
        row.classList.add('fade-in');
        row.dataset.productId = product.product_id;
        
        const stockClass = product.stock_level < 50 ? 'low-stock' : 
                          product.stock_level < 100 ? 'medium-stock' : 'high-stock';
        
        row.innerHTML = `
            <td><span class="product-id">${product.product_id}</span></td>
            <td><span class="stock-badge ${stockClass}">${product.stock_level}</span></td>
        `;
        
        tableBody.appendChild(row);
    });
}

async function fetchForecast(productId, token) {
    const chartContainer = document.getElementById('forecast-chart-container');
    const canvas = document.getElementById('forecast-chart');
    
    if (!canvas || !chartContainer) return;

    chartContainer.classList.add('loading');

    try {
        const response = await fetch(`/api/forecast/${productId}`, {
            headers: { 'Authorization': `Bearer ${token}` }
        });
        
        if (response.status === 401) throw new Error('UNAUTHORIZED');
        if (!response.ok) throw new Error(`Failed to fetch forecast: ${response.status}`);
        
        const forecastData = await response.json();

        if (forecastChart) {
            forecastChart.destroy();
        }

        const labels = forecastData.map(item => {
            const date = new Date(item.forecast_date);
            return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
        });
        const predictedUnits = forecastData.map(item => item.predicted_units);

        const ctx = canvas.getContext('2d');
        forecastChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: `Predicted Sales for ${productId}`,
                    data: predictedUnits,
                    borderColor: '#3498db',
                    backgroundColor: 'rgba(52, 152, 219, 0.1)',
                    fill: true,
                    tension: 0.4,
                    pointRadius: 4,
                    pointHoverRadius: 6,
                    pointBackgroundColor: '#3498db',
                    pointBorderColor: '#fff',
                    pointBorderWidth: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: true, position: 'top', labels: { font: { size: 14, family: "'Roboto', sans-serif" }, padding: 15 } },
                    tooltip: { backgroundColor: 'rgba(0, 0, 0, 0.8)', padding: 12, titleFont: { size: 14 }, bodyFont: { size: 13 }, cornerRadius: 4 }
                },
                scales: {
                    y: { beginAtZero: true, grid: { color: 'rgba(0, 0, 0, 0.05)' }, ticks: { font: { size: 12 } } },
                    x: { grid: { display: false }, ticks: { font: { size: 12 } } }
                }
            }
        });

        chartContainer.classList.remove('loading');
    } catch (error) {
        console.error(`Error fetching forecast for ${productId}:`, error);
        chartContainer.classList.remove('loading');
        
        if (forecastChart) {
            forecastChart.destroy();
            forecastChart = null;
        }
        
        const ctx = canvas.getContext('2d');
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        ctx.font = '14px Roboto';
        ctx.fillStyle = '#e74c3c';
        ctx.textAlign = 'center';
        ctx.fillText(`Failed to load forecast for ${productId}`, canvas.width / 2, canvas.height / 2);
    }
}

function sortTable(columnIndex) {
    const table = document.getElementById("inventory-table");
    const tbody = table.tBodies[0];
    const rows = Array.from(tbody.rows);
    
    if (rows.length === 0) return;
    
    const isNumeric = columnIndex === 1;
    const direction = inventorySortDir[columnIndex] === 'asc' ? 'desc' : 'asc';
    
    inventorySortDir = {};
    inventorySortDir[columnIndex] = direction;

    rows.sort((a, b) => {
        const aText = a.cells[columnIndex].textContent.trim();
        const bText = b.cells[columnIndex].textContent.trim();
        
        const valA = isNumeric ? parseInt(aText) : aText.toLowerCase();
        const valB = isNumeric ? parseInt(bText) : bText.toLowerCase();
        
        if (valA < valB) return direction === 'asc' ? -1 : 1;
        if (valA > valB) return direction === 'asc' ? 1 : -1;
        return 0;
    });

    tbody.innerHTML = '';
    rows.forEach(row => tbody.appendChild(row));
    
    updateSortIndicators(table, columnIndex, direction);
}

function updateSortIndicators(table, activeColumn, direction) {
    const headers = table.querySelectorAll('thead th');
    headers.forEach((header, index) => {
        header.classList.remove('sort-asc', 'sort-desc');
        if (index === activeColumn) {
            header.classList.add(`sort-${direction}`);
        }
    });
}