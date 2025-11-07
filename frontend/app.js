let forecastChart;
let inventorySortDir = {};

document.addEventListener('DOMContentLoaded', () => {
    const token = localStorage.getItem('accessToken');
    if (!token) {
        window.location.href = '/';
        return;
    }

    initializeDashboard(token);
});

async function initializeDashboard(token) {
    try {
        await Promise.all([
            fetchUserInfo(token),
            fetchSummaryData(token),
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

function handleLogout() {
    localStorage.removeItem('accessToken');
    window.location.href = '/';
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

        animateCounter('kpi-total-products', data.total_products);
        animateCounter('kpi-total-units', data.total_units_in_stock);
        animateCounter('kpi-sales-24h', data.sales_last_24h);
        animateCounter('kpi-low-stock', data.low_stock_items);
    } catch (error) {
        console.error('Error fetching summary data:', error);
        throw error;
    }
}

function animateCounter(elementId, targetValue) {
    const element = document.getElementById(elementId);
    const duration = 1000;
    const start = 0;
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
                    label: 'Predicted Sales',
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
                maintainAspectRatio: true,
                plugins: {
                    legend: {
                        display: true,
                        position: 'top',
                        labels: {
                            font: { size: 14, family: "'Roboto', sans-serif" },
                            padding: 15
                        }
                    },
                    tooltip: {
                        backgroundColor: 'rgba(0, 0, 0, 0.8)',
                        padding: 12,
                        titleFont: { size: 14 },
                        bodyFont: { size: 13 },
                        cornerRadius: 4
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        grid: {
                            color: 'rgba(0, 0, 0, 0.05)'
                        },
                        ticks: {
                            font: { size: 12 }
                        }
                    },
                    x: {
                        grid: {
                            display: false
                        },
                        ticks: {
                            font: { size: 12 }
                        }
                    }
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
        ctx.fillText('Failed to load forecast data', canvas.width / 2, canvas.height / 2);
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