// ============================================================
//  BizPulse app.js  –  Complete Feature Implementation
// ============================================================

// ── Global State ────────────────────────────────────────────
let forecastChart = null;
let inventorySortDir = {};
let allProducts = [];          // full product list for client-side ops
let filteredProducts = [];     // after search / filter applied
let currentPage = 1;
const PAGE_SIZE = 10;
let allSuppliers = [];         // cached supplier list
let poItems = [];              // line items being built for a PO
let editingProductId = null;   // null = Add, string = Edit

// ── Boot ────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
    const token = localStorage.getItem('accessToken');
    if (!token) { window.location.href = '/'; return; }

    initializeDashboard(token);
    setupTabListeners(token);
    setupInventoryTableListener(token);
    setupModalListeners(token);
    setupInventoryControls();
    setupStockAdjustPreview();
    setupBulkImportPreview();
    setupAutoRefresh(token)
});

// ── Dashboard Init ───────────────────────────────────────────
async function initializeDashboard(token) {
    try {
        await Promise.all([
            fetchUserInfo(token),
            fetchSummaryData(token),
            fetchAlerts(token),
            fetchInsights(token),
            populateProductSelector(token),
            loadProductMasterTable(token), 
            loadSuppliers(token)
        ]);
    } catch (err) {
        console.error('Dashboard init error:', err);
        if (err.message === 'UNAUTHORIZED') {
            localStorage.removeItem('accessToken');
            window.location.href = '/';
        }
    }
    document.getElementById('logout-button').addEventListener('click', handleLogout);
}

// ── Tab Switching ────────────────────────────────────────────
function setupTabListeners(token) {
    const tabs = document.querySelectorAll('.tab-link');
    const contents = document.querySelectorAll('.tab-content');

    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            tabs.forEach(t => t.classList.remove('active'));
            contents.forEach(c => c.classList.remove('active'));
            tab.classList.add('active');
            document.getElementById(tab.dataset.tab).classList.add('active');

            if (tab.dataset.tab === 'forecast-analytics-tab' && forecastChart) {
                setTimeout(() => forecastChart.resize(), 50);
            }
            if (tab.dataset.tab === 'inventory-ops-tab') {
                loadProductMasterTable(token);
                loadStockMovements(token);
            }
            if (tab.dataset.tab === 'supply-chain-tab') {
                loadPurchaseOrders(token);
                loadSuppliers(token);
            }
        });
    });
}

// ── Dashboard-tab click → forecast ──────────────────────────
function setupInventoryTableListener(token) {
    const tbody = document.querySelector('#inventory-table tbody');
    if (!tbody) return;
    tbody.addEventListener('click', e => {
        const row = e.target.closest('tr');
        if (!row || !row.dataset.productId) return;
        document.querySelector('.tab-link[data-tab="forecast-analytics-tab"]')?.click();
        const sel = document.getElementById('product-selector');
        if (sel) { sel.value = row.dataset.productId; fetchForecast(row.dataset.productId, token); }
    });
}

function setupAutoRefresh(token) {
    setInterval(async () => {
        // 1. Always update KPIs silently
        fetchSummaryData(token);

        // 2. Update dashboard inventory list if we are on the dashboard tab
        if (document.getElementById('dashboard-tab').classList.contains('active')) {
            try {
                const res = await apiFetch('/api/inventory/enriched', token);
                const inv = await res.json();
                displayInventory(inv);
            } catch (e) { /* Ignore network blips */ }
        }

        // 3. Update stock movements if we are on the inventory tab
        if (document.getElementById('inventory-ops-tab').classList.contains('active')) {
            loadStockMovements(token, true); // true = silent reload
        }
    }, 3000); // Refreshes every 3 seconds
}

// ── Modal open / close ───────────────────────────────────────
window.openModal = function (id) {
    const m = document.getElementById(id);
    if (m) m.classList.add('active');
};
window.closeModal = function (id) {
    const m = document.getElementById(id);
    if (m) m.classList.remove('active');
};

function setupModalListeners(token) {
    // close on overlay click
    document.querySelectorAll('.modal-overlay').forEach(overlay => {
        overlay.addEventListener('click', e => {
            if (e.target === overlay) overlay.classList.remove('active');
        });
    });

    // ── Add/Edit Product ──
    document.getElementById('product-form')?.addEventListener('submit', e => e.preventDefault());
    document.querySelector('#product-modal .btn-primary')
        ?.addEventListener('click', () => saveProduct(token));

    // ── Stock Adjustment ──
    document.querySelector('#stock-adjust-modal .btn-primary')
        ?.addEventListener('click', () => confirmStockAdjustment(token));

    // ── Bulk Import ──
    document.querySelector('#bulk-import-modal .btn-primary')
        ?.addEventListener('click', () => processBulkImport(token));

    // ── Create PO ──
    document.querySelector('#create-po-modal .btn-primary')
        ?.addEventListener('click', () => submitPurchaseOrder(token));

    // ── Add Supplier ──
    document.querySelector('#add-supplier-modal .btn-primary')
        ?.addEventListener('click', () => saveSupplier(token));

    // ── Export button ──
    document.querySelector('#product-master-table')?.closest('.card')
        ?.querySelector('.btn-sm[onclick="exportInventory()"]')
        ?.removeAttribute('onclick');
    // handled via inline onclick below – kept as window fn
}

// ── Logout ───────────────────────────────────────────────────
function handleLogout() {
    localStorage.removeItem('accessToken');
    window.location.href = '/';
}

// ════════════════════════════════════════════════════════════
//  SECTION 1: EXISTING DASHBOARD FEATURES (unchanged logic,
//             kept intact so nothing breaks)
// ════════════════════════════════════════════════════════════

async function fetchUserInfo(token) {
    const res = await apiFetch('/api/users/me', token);
    const data = await res.json();
    const el = document.getElementById('welcome-message');
    if (el) el.textContent = `Welcome, ${data.full_name}`;
}

async function fetchSummaryData(token) {
    try {
        const res = await apiFetch('/api/summary', token);
        const data = await res.json();
        animateCounter('kpi-total-products', data.total_products);
        animateCounter('kpi-total-units', data.total_units_in_stock);
        animateCounter('kpi-sales-24h', data.sales_last_24h);
        animateCounter('kpi-low-stock', data.low_stock_items);
    } catch (error) {
        console.error('Error fetching summary data:', error);
        throw error;
    }
}

// ── PATCH 1: fetchAlerts — adds dismiss button on each alert ─
 
async function fetchAlerts(token) {
    const listContainer = document.getElementById('alerts-list');
    if (!listContainer) return;
    try {
        const res = await apiFetch('/api/alerts', token);
        const alerts = await res.json();
        if (alerts.length === 0) {
            listContainer.innerHTML = '<p style="padding:1rem;text-align:center;">No active alerts. All good!</p>';
            return;
        }
        listContainer.innerHTML = '<ul>' + alerts.map(a => `
            <li style="display:flex;justify-content:space-between;align-items:flex-start;gap:1rem;">
              <div>
                <span class="alert-product">${a.product_id}</span>
                <span class="alert-message">${a.alert_message}</span>
              </div>
              <button
                class="btn-sm"
                style="flex-shrink:0;font-size:11px;padding:3px 8px;"
                onclick="dismissAlert(${a.id}, '${token}')"
              >Dismiss</button>
            </li>`).join('') + '</ul>';
    } catch (err) {
        listContainer.innerHTML = '<p class="error-row">Could not load alerts.</p>';
        throw err;
    }
}
 
// Add this new function anywhere in app.js (e.g. after fetchAlerts)
window.dismissAlert = async function (alertId, token) {
    try {
        const res = await fetch(`/api/alerts/${alertId}/dismiss`, {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${token}` }
        });
        if (!res.ok) throw new Error('Dismiss failed');
        // Re-render the alerts list
        fetchAlerts(token);
        showToast('Alert dismissed.', 'success');
    } catch (err) {
        showToast('Could not dismiss alert.', 'error');
    }
};


async function fetchInsights(token) {
    const listContainer = document.getElementById('insights-list');
    if (!listContainer) return;
    
    try {
        // Fetch both insights and products simultaneously to map SKUs to Names
        const [rulesRes, productsRes] = await Promise.all([
            apiFetch('/api/insights/frequently-bought-together', token),
            apiFetch('/api/products', token)
        ]);
        
        const rules = await rulesRes.json();
        const products = await productsRes.json();
        
        // Build a lookup dictionary: SKU -> Product Name
        const nameMap = {};
        products.forEach(p => {
            nameMap[p.sku] = p.product_name;
        });
        
        // Helper to translate SKUs to Names (handles comma-separated lists too)
        const getProductName = (skuString) => {
            if (!skuString) return skuString;
            return skuString.toString().split(',').map(s => {
                const cleanSku = s.trim();
                return nameMap[cleanSku] || cleanSku; // fallback to SKU if name isn't found
            }).join(', ');
        };

        if (rules.length === 0) {
            listContainer.innerHTML = '<p style="padding:1rem;text-align:center;">Not enough data yet.</p>';
            return;
        }
        
        listContainer.innerHTML = '<ul>' + rules.map(r => `
            <li>
              <span>When a customer buys <b>${escHtml(getProductName(r.antecedents))}</b>…</span>
              <span style="font-size:.9em;color:var(--text-light)">
                …they also buy <b>${escHtml(getProductName(r.consequents))}</b> ${(r.confidence * 100).toFixed(0)}% of the time.
              </span>
            </li>`).join('') + '</ul>';
            
    } catch (err) {
        console.error('Insights error:', err);
        listContainer.innerHTML = '<p style="padding:1rem;color:var(--danger-color)">Could not load insights.</p>';
    }
}

// ── PATCH 2: populateProductSelector — uses enriched endpoint ─


async function populateProductSelector(token) {
    try {
        // Use the enriched endpoint so we get product_name alongside stock_level
        const res = await apiFetch('/api/inventory/enriched', token);
        const inv = await res.json();
 
        const sel = document.getElementById('product-selector');
        if (!sel) return;
        sel.innerHTML = '<option value="">Select a product…</option>';
        inv.forEach(p => {
            const o = document.createElement('option');
            o.value = p.product_id;
            // Show name if available, otherwise fall back to product_id
            const label = p.product_name !== p.product_id
                ? `${p.product_name} (${p.product_id})`
                : p.product_id;
            o.textContent = `${label} — Stock: ${p.stock_level}`;
            sel.appendChild(o);
        });
        sel.addEventListener('change', e => {
            if (e.target.value) fetchForecast(e.target.value, token);
        });
 
        if (inv.length > 0) {
            displayInventory(inv);
            sel.value = inv[0].product_id;
            fetchForecast(inv[0].product_id, token);
        }
    } catch (err) {
        console.error('Error populating product selector:', err);
        throw err;
    }
}

let lastInventoryState = "";
function displayInventory(data) {
    const tbody = document.querySelector('#inventory-table tbody');
    if (!tbody) return;
    
    // Prevent the table rows from flashing if no stock levels changed
    const currentState = JSON.stringify(data);
    if (currentState === lastInventoryState && !tbody.querySelector('.loading-row')) return;
    lastInventoryState = currentState;

    tbody.innerHTML = data.map((p, i) => {
        const cls = p.stock_level < 50 ? 'low-stock' : p.stock_level < 100 ? 'medium-stock' : 'high-stock';
        return `<tr data-product-id="${p.product_id}" class="fade-in" style="animation-delay:${i * 0.05}s">
          <td><span class="product-id">${p.product_id}</span></td>
          <td><span class="stock-badge ${cls}">${p.stock_level}</span></td>
        </tr>`;
    }).join('');
}

async function fetchForecast(productId, token) {
    const container = document.getElementById('forecast-chart-container');
    const canvas = document.getElementById('forecast-chart');
    if (!canvas || !container) return;
    container.classList.add('loading');
    try {
        const res = await apiFetch(`/api/forecast/${productId}`, token);
        const data = await res.json();
        if (forecastChart) { forecastChart.destroy(); forecastChart = null; }
        const labels = data.map(d => new Date(d.forecast_date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }));
        const values = data.map(d => d.predicted_units);
        forecastChart = new Chart(canvas.getContext('2d'), {
            type: 'line',
            data: {
                labels,
                datasets: [{
                    label: `Predicted Sales – ${productId}`,
                    data: values,
                    borderColor: '#3498db',
                    backgroundColor: 'rgba(52,152,219,.1)',
                    fill: true, tension: 0.4,
                    pointRadius: 4, pointHoverRadius: 6,
                    pointBackgroundColor: '#3498db', pointBorderColor: '#fff', pointBorderWidth: 2
                }]
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                plugins: { legend: { display: true, position: 'top' }, tooltip: { backgroundColor: 'rgba(0,0,0,.8)', padding: 12, cornerRadius: 4 } },
                scales: { y: { beginAtZero: true }, x: { grid: { display: false } } }
            }
        });
    } catch (err) {
        console.error(err);
    } finally {
        container.classList.remove('loading');
    }
}

function sortTable(col) {
    const table = document.getElementById('inventory-table');
    if (!table) return;
    const tbody = table.tBodies[0];
    const rows = Array.from(tbody.rows);
    const dir = inventorySortDir[col] === 'asc' ? 'desc' : 'asc';
    inventorySortDir = { [col]: dir };
    rows.sort((a, b) => {
        const av = col === 1 ? parseInt(a.cells[col].textContent) : a.cells[col].textContent.trim().toLowerCase();
        const bv = col === 1 ? parseInt(b.cells[col].textContent) : b.cells[col].textContent.trim().toLowerCase();
        return (av < bv ? -1 : av > bv ? 1 : 0) * (dir === 'asc' ? 1 : -1);
    });
    tbody.innerHTML = '';
    rows.forEach(r => tbody.appendChild(r));
    table.querySelectorAll('thead th').forEach((th, i) => {
        th.classList.remove('sort-asc', 'sort-desc');
        if (i === col) th.classList.add(`sort-${dir}`);
    });
}

function displayEmptyState(token) {
    document.getElementById('empty-state-container').style.display = 'block';
    document.querySelector('.kpi-grid').style.display = 'none';
    document.querySelector('.action-grid').style.display = 'none';
    document.querySelector('.container-wrapper').style.display = 'none';

    const btn = document.getElementById('generate-data-button');
    const status = document.getElementById('generation-status');
    if (!btn || btn.dataset.listenerAttached) return;
    btn.dataset.listenerAttached = 'true';
    btn.addEventListener('click', async () => {
        btn.disabled = true;
        btn.querySelector('.button-text').textContent = 'Generating…';
        if (status) status.textContent = 'Please wait up to 30 seconds. Page will reload automatically.';
        try {
            const res = await fetch('/api/users/me/generate-demo-data', { method: 'POST', headers: { 'Authorization': `Bearer ${token}` } });
            if (res.ok) { setTimeout(() => window.location.reload(), 25000); }
            else { if (status) status.textContent = 'Error – please refresh the page.'; btn.disabled = false; btn.querySelector('.button-text').textContent = 'Generate Demo Data'; }
        } catch { if (status) status.textContent = 'Network error – please try again.'; btn.disabled = false; btn.querySelector('.button-text').textContent = 'Generate Demo Data'; }
    });
}

function hideEmptyState() {
    document.getElementById('empty-state-container').style.display = 'none';
    document.querySelector('.kpi-grid').style.display = 'grid';
    document.querySelector('.action-grid').style.display = 'grid';
    document.querySelector('.container-wrapper').style.display = 'block';
}

// ── Smooth UI Updates ────────────────────────────────────────
function animateCounter(id, target) {
    const el = document.getElementById(id);
    if (!el) return;
    
    // Parse the current value currently displayed on screen
    const currentVal = parseInt(el.textContent.replace(/,/g, '')) || 0;
    
    // Prevent the numbers from constantly spinning if they haven't changed!
    if (currentVal === target) return; 

    const start = performance.now();
    const duration = 1000; // 1 second animation duration
    const diff = target - currentVal; // Calculate the difference
    
    const tick = now => {
        // p goes from 0 to 1 over the duration
        const p = Math.min((now - start) / duration, 1);
        
        // Smoothly animate from the current value to the new value
        el.textContent = Math.floor(currentVal + (diff * p)).toLocaleString();
        
        if (p < 1) {
            requestAnimationFrame(tick);
        } else {
            el.textContent = target.toLocaleString(); // Ensure it ends exactly on target
        }
    };
    
    requestAnimationFrame(tick);
}


// ════════════════════════════════════════════════════════════
//  SECTION 2: INVENTORY MANAGEMENT TAB
// ════════════════════════════════════════════════════════════

async function loadProductMasterTable(token) {
    const tbody = document.getElementById('product-master-body');
    if (!tbody) return;
    tbody.innerHTML = '<tr><td colspan="11" class="loading-row">Loading…</td></tr>';
    try {
        const res = await apiFetch('/api/products', token);
        allProducts = await res.json();
        filteredProducts = [...allProducts];
        // Also update the stock-adjust dropdown
        populateProductDropdowns();
        renderProductTable();
    } catch (err) {
        tbody.innerHTML = `<tr><td colspan="11" class="loading-row" style="color:var(--danger-color)">Failed to load products.</td></tr>`;
    }
}

function renderProductTable() {
    const tbody = document.getElementById('product-master-body');
    const pageInfo = document.getElementById('page-info');
    const prevBtn = document.getElementById('prev-page');
    const nextBtn = document.getElementById('next-page');
    if (!tbody) return;

    const totalPages = Math.max(1, Math.ceil(filteredProducts.length / PAGE_SIZE));
    currentPage = Math.min(currentPage, totalPages);
    const start = (currentPage - 1) * PAGE_SIZE;
    const pageItems = filteredProducts.slice(start, start + PAGE_SIZE);

    if (pageItems.length === 0) {
        tbody.innerHTML = '<tr><td colspan="11" class="loading-row">No products found.</td></tr>';
    } else {
        tbody.innerHTML = pageItems.map(p => {
            const value = (p.cost_price * p.current_stock).toFixed(2);
            let statusBadge;
            if (p.current_stock === 0) statusBadge = '<span class="status-badge status-warning">Out of Stock</span>';
            else if (p.current_stock < p.min_stock_level) statusBadge = '<span class="status-badge status-pending">Low Stock</span>';
            else statusBadge = '<span class="status-badge status-success">Active</span>';

            return `<tr>
              <td><code>${p.sku}</code></td>
              <td>${escHtml(p.product_name)}</td>
              <td>${escHtml(p.category || '—')}</td>
              <td>${escHtml(p.warehouse || '—')}</td>
              <td>$${Number(p.cost_price).toFixed(2)}</td>
              <td>$${Number(p.sell_price).toFixed(2)}</td>
              <td>${p.current_stock}</td>
              <td>${p.min_stock_level}</td>
              <td>$${value}</td>
              <td>${statusBadge}</td>
              <td>
                <button class="action-icon" title="Edit" onclick="openEditProduct('${p.sku}', token_ref())">✏️</button>
                <button class="action-icon delete" title="Delete" onclick="deleteProduct('${p.sku}', token_ref())">🗑️</button>
              </td>
            </tr>`;
        }).join('');
    }

    if (pageInfo) pageInfo.textContent = `Page ${currentPage} of ${totalPages}`;
    if (prevBtn) prevBtn.disabled = currentPage <= 1;
    if (nextBtn) nextBtn.disabled = currentPage >= totalPages;
}

// Pagination buttons
document.getElementById('prev-page')?.addEventListener('click', () => { currentPage--; renderProductTable(); });
document.getElementById('next-page')?.addEventListener('click', () => { currentPage++; renderProductTable(); });

function setupInventoryControls() {
    ['search-sku', 'filter-category', 'filter-warehouse', 'filter-status'].forEach(id => {
        document.getElementById(id)?.addEventListener('input', applyFilters);
        document.getElementById(id)?.addEventListener('change', applyFilters);
    });
}

function applyFilters() {
    const search = document.getElementById('search-sku')?.value.toLowerCase() || '';
    const cat = document.getElementById('filter-category')?.value || 'all';
    const wh = document.getElementById('filter-warehouse')?.value || 'all';
    const st = document.getElementById('filter-status')?.value || 'all';

    filteredProducts = allProducts.filter(p => {
        const matchSearch = !search || p.sku.toLowerCase().includes(search) || p.product_name.toLowerCase().includes(search);
        const matchCat = cat === 'all' || (p.category || '').toLowerCase() === cat;
        const matchWh = wh === 'all' || (p.warehouse || '').toLowerCase() === wh;
        let matchSt = true;
        if (st === 'active') matchSt = p.current_stock >= p.min_stock_level && p.current_stock > 0;
        if (st === 'low') matchSt = p.current_stock < p.min_stock_level && p.current_stock > 0;
        if (st === 'out') matchSt = p.current_stock === 0;
        return matchSearch && matchCat && matchWh && matchSt;
    });
    currentPage = 1;
    renderProductTable();
}

// ── Add / Edit Product Modal ─────────────────────────────────
window.openAddProduct = function () {
    editingProductId = null;
    document.getElementById('product-modal-title').textContent = 'Add New Product';
    document.getElementById('product-form').reset();
    document.getElementById('product-id').value = '';
    document.getElementById('product-sku').disabled = false;
    openModal('product-modal');
};

window.openEditProduct = function (sku, token) {
    const p = allProducts.find(x => x.sku === sku);
    if (!p) return;
    editingProductId = sku;
    document.getElementById('product-modal-title').textContent = 'Edit Product';
    document.getElementById('product-id').value = p.sku;
    document.getElementById('product-sku').value = p.sku;
    document.getElementById('product-sku').disabled = true;  // SKU is the key – don't allow editing
    document.getElementById('product-name').value = p.product_name;
    document.getElementById('product-category').value = p.category || '';
    document.getElementById('product-warehouse').value = p.warehouse || '';
    document.getElementById('product-cost').value = p.cost_price;
    document.getElementById('product-price').value = p.sell_price;
    document.getElementById('product-stock').value = p.current_stock;
    document.getElementById('product-min-stock').value = p.min_stock_level;
    document.getElementById('product-description').value = p.description || '';
    openModal('product-modal');
};

async function saveProduct(token) {
    const sku = document.getElementById('product-sku').value.trim();
    const name = document.getElementById('product-name').value.trim();
    const cost = parseFloat(document.getElementById('product-cost').value);
    const sell = parseFloat(document.getElementById('product-price').value);
    const stock = parseInt(document.getElementById('product-stock').value);
    const minStock = parseInt(document.getElementById('product-min-stock').value);

    if (!sku || !name || isNaN(cost) || isNaN(sell) || isNaN(stock) || isNaN(minStock)) {
        showToast('Please fill in all required fields.', 'error'); return;
    }

    const payload = {
        sku,
        product_name: name,
        category: document.getElementById('product-category').value || null,
        warehouse: document.getElementById('product-warehouse').value || null,
        cost_price: cost,
        sell_price: sell,
        current_stock: stock,
        min_stock_level: minStock,
        description: document.getElementById('product-description').value || null,
    };

    const isEdit = editingProductId !== null;
    const url = isEdit ? `/api/products/${encodeURIComponent(sku)}` : '/api/products';
    const method = isEdit ? 'PUT' : 'POST';

    try {
        const res = await fetch(url, {
            method,
            headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
            body: JSON.stringify(payload)
        });
        if (!res.ok) { const e = await res.json(); throw new Error(e.detail || 'Save failed'); }
        showToast(isEdit ? 'Product updated.' : 'Product added!', 'success');
        closeModal('product-modal');
        loadProductMasterTable(token);
        // also refresh KPIs
        fetchSummaryData(token);
        populateProductSelector(token);
    } catch (err) { showToast(err.message, 'error'); }
}

window.deleteProduct = async function (sku, token) {
    if (!confirm(`Delete product ${sku}? This cannot be undone.`)) return;
    try {
        const res = await fetch(`/api/products/${encodeURIComponent(sku)}`, {
            method: 'DELETE',
            headers: { 'Authorization': `Bearer ${token}` }
        });
        if (!res.ok) { const e = await res.json(); throw new Error(e.detail || 'Delete failed'); }
        showToast('Product deleted.', 'success');
        loadProductMasterTable(token);
        fetchSummaryData(token);
    } catch (err) { showToast(err.message, 'error'); }
};

// ── Stock Movements ──────────────────────────────────────────
async function loadStockMovements(token, silent = false) {
    const tbody = document.getElementById('stock-movements-body');
    if (!tbody) return;
    
    if (!silent) tbody.innerHTML = '<tr><td colspan="7" class="loading-row">Loading…</td></tr>';
    
    try {
        const res = await apiFetch('/api/stock-movements', token);
        const moves = await res.json();
        if (moves.length === 0) {
            tbody.innerHTML = '<tr><td colspan="7" class="loading-row">No movements recorded yet.</td></tr>';
            return;
        }
        
        tbody.innerHTML = moves.map(m => {
            // Fix timezone by explicitly treating the database time as UTC
            let dateStr = m.created_at;
            if (dateStr && !dateStr.endsWith('Z')) dateStr += 'Z';
            
            return `
          <tr>
            <td>${new Date(dateStr).toLocaleString()}</td>
            <td><code>${m.sku}</code></td>
            <td>${escHtml(m.product_name)}</td>
            <td><span class="status-badge ${m.movement_type === 'in' || m.movement_type === 'return' ? 'status-success' : 'status-warning'}">${m.movement_type.toUpperCase()}</span></td>
            <td>${m.quantity > 0 ? '+' : ''}${m.quantity}</td>
            <td>${escHtml(m.performed_by || '—')}</td>
            <td>${escHtml(m.note || '—')}</td>
          </tr>`
        }).join('');
    } catch {
        if (!silent) tbody.innerHTML = '<tr><td colspan="7" class="loading-row" style="color:var(--danger-color)">Failed to load movements.</td></tr>';
    }
}

// ── Stock Adjustment Modal ───────────────────────────────────
function populateProductDropdowns() {
    const adjustSel = document.getElementById('adjust-product-sku');
    const poSel = document.getElementById('po-product');
    const supplierSel = document.getElementById('product-supplier');

    if (adjustSel) {
        adjustSel.innerHTML = '<option value="">Choose a product…</option>' +
            allProducts.map(p => `<option value="${p.sku}" data-stock="${p.current_stock}">
              ${p.sku} – ${escHtml(p.product_name)} (Stock: ${p.current_stock})
            </option>`).join('');
    }
    if (poSel) {
        poSel.innerHTML = '<option value="">Select Product</option>' +
            allProducts.map(p => `<option value="${p.sku}" data-price="${p.cost_price}">
              ${p.sku} – ${escHtml(p.product_name)}
            </option>`).join('');
    }
    if (supplierSel && allSuppliers.length) {
        supplierSel.innerHTML = '<option value="">No Supplier</option>' +
            allSuppliers.map(s => `<option value="${s.id}">${escHtml(s.supplier_name)}</option>`).join('');
    }
}

function setupStockAdjustPreview() {
    const productSel = document.getElementById('adjust-product-sku');
    const typeSel = document.getElementById('adjust-type');
    const qtyInput = document.getElementById('adjust-quantity');
    const currentEl = document.getElementById('adjust-current-stock');
    const newEl = document.getElementById('adjust-new-stock');

    const update = () => {
        const opt = productSel?.selectedOptions[0];
        const current = parseInt(opt?.dataset.stock) || 0;
        if (currentEl) currentEl.value = current;
        const qty = parseInt(qtyInput?.value) || 0;
        const type = typeSel?.value;
        const delta = (type === 'in' || type === 'return') ? qty : -qty;
        const newVal = current + delta;
        if (newEl) {
            newEl.value = newVal;
            newEl.style.color = newVal < 0 ? 'var(--danger-color)' : 'inherit';
        }
    };

    productSel?.addEventListener('change', update);
    typeSel?.addEventListener('change', update);
    qtyInput?.addEventListener('input', update);
}

async function confirmStockAdjustment(token) {
    const sku = document.getElementById('adjust-product-sku').value;
    const type = document.getElementById('adjust-type').value;
    const qty = parseInt(document.getElementById('adjust-quantity').value);
    const note = document.getElementById('adjust-note').value.trim();
    const ref = document.getElementById('adjust-reference').value.trim();
    const newStock = parseInt(document.getElementById('adjust-new-stock').value);

    if (!sku || !type || !qty || !note) { showToast('Please fill all required fields.', 'error'); return; }
    if (newStock < 0) { showToast('Adjustment would result in negative stock.', 'error'); return; }

    try {
        const res = await fetch('/api/stock-adjustments', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
            body: JSON.stringify({ sku, movement_type: type, quantity: qty, note, reference: ref })
        });
        if (!res.ok) { const e = await res.json(); throw new Error(e.detail || 'Adjustment failed'); }
        showToast('Stock adjusted successfully.', 'success');
        closeModal('stock-adjust-modal');
        document.getElementById('adjust-form')?.reset();
        loadProductMasterTable(token);
        loadStockMovements(token);
        fetchSummaryData(token);
    } catch (err) { showToast(err.message, 'error'); }
}

// ── Bulk Import ──────────────────────────────────────────────
function setupBulkImportPreview() {
    document.getElementById('bulk-file')?.addEventListener('change', e => {
        const file = e.target.files[0];
        if (!file) return;
        const reader = new FileReader();
        reader.onload = ev => {
            const lines = ev.target.result.split('\n').filter(Boolean);
            if (lines.length < 2) return;
            const headers = lines[0].split(',').map(h => h.trim());
            const rows = lines.slice(1, 6); // preview first 5
            document.getElementById('bulk-preview-header').innerHTML = '<tr>' + headers.map(h => `<th>${escHtml(h)}</th>`).join('') + '</tr>';
            document.getElementById('bulk-preview-body').innerHTML = rows.map(r => '<tr>' + r.split(',').map(c => `<td>${escHtml(c.trim())}</td>`).join('') + '</tr>').join('');
            document.getElementById('bulk-import-preview').style.display = 'block';
        };
        reader.readAsText(file);
    });
}

async function processBulkImport(token) {
    const file = document.getElementById('bulk-file')?.files[0];
    if (!file) { showToast('Please select a CSV file.', 'error'); return; }
    const skipDups = document.getElementById('bulk-skip-duplicates')?.checked || false;

    const reader = new FileReader();
    reader.onload = async ev => {
        const lines = ev.target.result.split('\n').filter(Boolean);
        if (lines.length < 2) { showToast('CSV appears empty.', 'error'); return; }
        const rows = lines.slice(1).map(l => {
            const [sku, product_name, category, warehouse, cost_price, sell_price, current_stock, min_stock_level] = l.split(',').map(s => s.trim());
            return { sku, product_name, category, warehouse, cost_price: parseFloat(cost_price), sell_price: parseFloat(sell_price), current_stock: parseInt(current_stock), min_stock_level: parseInt(min_stock_level) };
        }).filter(r => r.sku && r.product_name);

        try {
            const res = await fetch('/api/products/bulk', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
                body: JSON.stringify({ products: rows, skip_duplicates: skipDups })
            });
            if (!res.ok) { const e = await res.json(); throw new Error(e.detail || 'Import failed'); }
            const result = await res.json();
            showToast(`Imported ${result.imported} products (${result.skipped} skipped).`, 'success');
            closeModal('bulk-import-modal');
            document.getElementById('bulk-import-form')?.reset();
            document.getElementById('bulk-import-preview').style.display = 'none';
            loadProductMasterTable(token);
            fetchSummaryData(token);
        } catch (err) { showToast(err.message, 'error'); }
    };
    reader.readAsText(file);
}

// ── Export CSV ───────────────────────────────────────────────
window.exportInventory = function () {
    if (filteredProducts.length === 0) { showToast('No data to export.', 'error'); return; }
    const headers = ['SKU', 'Product Name', 'Category', 'Warehouse', 'Cost Price', 'Sell Price', 'Stock', 'Min Stock', 'Total Value', 'Status'];
    const rows = filteredProducts.map(p => {
        const status = p.current_stock === 0 ? 'Out of Stock' : p.current_stock < p.min_stock_level ? 'Low Stock' : 'Active';
        return [p.sku, p.product_name, p.category || '', p.warehouse || '', p.cost_price, p.sell_price, p.current_stock, p.min_stock_level, (p.cost_price * p.current_stock).toFixed(2), status];
    });
    const csv = [headers, ...rows].map(r => r.map(c => `"${String(c).replace(/"/g, '""')}"`).join(',')).join('\n');
    downloadFile(csv, 'bizpulse_inventory.csv', 'text/csv');
};

// ════════════════════════════════════════════════════════════
//  SECTION 3: SUPPLY CHAIN TAB
// ════════════════════════════════════════════════════════════

async function loadPurchaseOrders(token) {
    const tbody = document.getElementById('po-table-body');
    if (!tbody) return;
    tbody.innerHTML = '<tr><td colspan="7" class="loading-row">Loading…</td></tr>';
    try {
        const res = await apiFetch('/api/purchase-orders', token);
        const pos = await res.json();
        if (pos.length === 0) { tbody.innerHTML = '<tr><td colspan="7" class="loading-row">No purchase orders yet.</td></tr>'; return; }
        tbody.innerHTML = pos.map(po => `
          <tr>
            <td><code>PO-${String(po.id).padStart(4, '0')}</code></td>
            <td>${escHtml(po.supplier_name)}</td>
            <td>${po.item_count}</td>
            <td>$${Number(po.total_amount).toFixed(2)}</td>
            <td><span class="status-badge ${po.status === 'received' ? 'status-success' : po.status === 'pending' ? 'status-pending' : 'status-warning'}">${po.status}</span></td>
            <td>${new Date(po.created_at).toLocaleDateString()}</td>
            <td>
              ${po.status === 'pending' ? `<button class="btn-sm" onclick="receivePO(${po.id}, token_ref())">Receive</button>` : '—'}
            </td>
          </tr>`).join('');
    } catch {
        tbody.innerHTML = '<tr><td colspan="7" class="loading-row" style="color:var(--danger-color)">Failed to load.</td></tr>';
    }
}

async function loadSuppliers(token) {
    const container = document.getElementById('supplier-list');
    if (!container) return;
    container.innerHTML = '<p class="loading-row">Loading…</p>';
    try {
        const res = await apiFetch('/api/suppliers', token);
        allSuppliers = await res.json();
        populateProductDropdowns(); // refresh PO supplier dropdown too

        if (allSuppliers.length === 0) { container.innerHTML = '<p style="padding:1rem;text-align:center;">No suppliers added yet.</p>'; return; }
        container.innerHTML = '<ul>' + allSuppliers.map(s => `
          <li style="padding:.75rem 1.5rem;border-bottom:1px solid var(--border-color);display:flex;justify-content:space-between;align-items:center;">
            <div>
              <strong>${escHtml(s.supplier_name)}</strong>
              <br><small style="color:var(--text-light)">${escHtml(s.contact_email || '')} ${s.lead_time_days ? `· Lead: ${s.lead_time_days}d` : ''}</small>
            </div>
            <button class="action-icon delete" onclick="deleteSupplier(${s.id}, token_ref())">🗑️</button>
          </li>`).join('') + '</ul>';

        // populate PO supplier select
        const poSup = document.getElementById('po-supplier');
        if (poSup) {
            poSup.innerHTML = '<option value="">Select Supplier</option>' +
                allSuppliers.map(s => `<option value="${s.id}">${escHtml(s.supplier_name)}</option>`).join('');
        }
    } catch {
        container.innerHTML = '<p style="padding:1rem;color:var(--danger-color)">Failed to load suppliers.</p>';
    }
}

async function saveSupplier(token) {
    const name = document.getElementById('supplier-name')?.value.trim();
    if (!name) { showToast('Supplier name is required.', 'error'); return; }
    const payload = {
        supplier_name: name,
        contact_person: document.getElementById('supplier-contact')?.value.trim() || null,
        contact_email: document.getElementById('supplier-email')?.value.trim() || null,
        phone: document.getElementById('supplier-phone')?.value.trim() || null,
        lead_time_days: parseInt(document.getElementById('supplier-lead-time')?.value) || null,
        address: document.getElementById('supplier-address')?.value.trim() || null,
    };
    try {
        const res = await fetch('/api/suppliers', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
            body: JSON.stringify(payload)
        });
        if (!res.ok) { const e = await res.json(); throw new Error(e.detail || 'Save failed'); }
        showToast('Supplier added!', 'success');
        closeModal('add-supplier-modal');
        document.getElementById('supplier-form')?.reset();
        loadSuppliers(token);
    } catch (err) { showToast(err.message, 'error'); }
}

window.deleteSupplier = async function (id, token) {
    if (!confirm('Delete this supplier?')) return;
    try {
        const res = await fetch(`/api/suppliers/${id}`, { method: 'DELETE', headers: { 'Authorization': `Bearer ${token}` } });
        if (!res.ok) throw new Error('Delete failed');
        showToast('Supplier removed.', 'success');
        loadSuppliers(token);
    } catch (err) { showToast(err.message, 'error'); }
};

// ── Purchase Order Builder ───────────────────────────────────
window.addPOItem = function () {
    const sel = document.getElementById('po-product');
    const qtyInput = document.getElementById('po-quantity');
    const sku = sel?.value;
    const qty = parseInt(qtyInput?.value);
    if (!sku || !qty || qty < 1) { showToast('Select a product and enter a valid quantity.', 'error'); return; }

    const opt = sel.selectedOptions[0];
    const price = parseFloat(opt.dataset.price) || 0;
    const label = opt.textContent.trim();

    const existing = poItems.find(i => i.sku === sku);
    if (existing) { existing.qty += qty; }
    else { poItems.push({ sku, label, qty, unit_price: price }); }

    qtyInput.value = '';
    sel.value = '';
    renderPOItems();
};

function renderPOItems() {
    const tbody = document.getElementById('po-items-body');
    const totalEl = document.getElementById('po-total');
    if (!tbody) return;
    if (poItems.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;color:#999">No items added yet</td></tr>';
        if (totalEl) totalEl.textContent = '$0.00';
        return;
    }
    let total = 0;
    tbody.innerHTML = poItems.map((item, i) => {
        const lineTotal = item.qty * item.unit_price;
        total += lineTotal;
        return `<tr>
          <td><code>${item.sku}</code></td>
          <td>${escHtml(item.label)}</td>
          <td>${item.qty}</td>
          <td>$${item.unit_price.toFixed(2)}</td>
          <td>$${lineTotal.toFixed(2)}</td>
          <td><button class="action-icon delete" onclick="removePOItem(${i})">✕</button></td>
        </tr>`;
    }).join('');
    if (totalEl) totalEl.textContent = `$${total.toFixed(2)}`;
}

window.removePOItem = function (i) { poItems.splice(i, 1); renderPOItems(); };

async function submitPurchaseOrder(token) {
    const supplierId = document.getElementById('po-supplier')?.value;
    const deliveryDate = document.getElementById('po-delivery-date')?.value;
    if (!supplierId) { showToast('Please select a supplier.', 'error'); return; }
    if (poItems.length === 0) { showToast('Add at least one item.', 'error'); return; }

    const payload = { supplier_id: parseInt(supplierId), expected_delivery: deliveryDate || null, items: poItems };
    try {
        const res = await fetch('/api/purchase-orders', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
            body: JSON.stringify(payload)
        });
        if (!res.ok) { const e = await res.json(); throw new Error(e.detail || 'PO failed'); }
        showToast('Purchase order created!', 'success');
        closeModal('create-po-modal');
        poItems = [];
        renderPOItems();
        document.getElementById('po-form')?.reset();
        loadPurchaseOrders(token);
    } catch (err) { showToast(err.message, 'error'); }
}

window.receivePO = async function (id, token) {
    if (!confirm('Mark this PO as received? This will update stock levels.')) return;
    try {
        const res = await fetch(`/api/purchase-orders/${id}/receive`, {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${token}` }
        });
        if (!res.ok) { const e = await res.json(); throw new Error(e.detail || 'Failed'); }
        showToast('PO received – stock updated!', 'success');
        loadPurchaseOrders(token);
        loadProductMasterTable(token);
        fetchSummaryData(token);
    } catch (err) { showToast(err.message, 'error'); }
};

// ════════════════════════════════════════════════════════════
//  SECTION 4: REPORTS TAB
// ════════════════════════════════════════════════════════════

// Wire up report buttons once DOM ready
document.addEventListener('DOMContentLoaded', () => {
    // We target by position since the buttons have no IDs – safer to add event delegation
    document.querySelectorAll('.report-card .btn-primary').forEach((btn, i) => {
        btn.addEventListener('click', () => generateReport(i));
    });
});

// ── PATCH 3: generateReport — add index 4 for PDF ────────────

async function generateReport(index) {
    const token = localStorage.getItem('accessToken');
    const reports = [
        { url: '/api/reports/valuation',        filename: 'inventory_valuation.csv',   mime: 'text/csv' },
        { url: '/api/reports/movements',         filename: 'stock_movements.csv',        mime: 'text/csv' },
        { url: '/api/reports/low-stock',         filename: 'low_stock_report.csv',       mime: 'text/csv' },
        { url: '/api/reports/sales-performance', filename: 'sales_performance.csv',      mime: 'text/csv' },
        { url: '/api/reports/summary-pdf',       filename: 'bizpulse_summary.pdf',       mime: 'application/pdf' },
    ];
    const { url, filename, mime } = reports[index];
    try {
        const res = await apiFetch(url, token);
        const blob = await res.blob();
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = filename;
        a.click();
        URL.revokeObjectURL(a.href);
        showToast('Report downloaded!', 'success');
    } catch {
        showToast('Failed to generate report.', 'error');
    }
}

// ════════════════════════════════════════════════════════════
//  UTILITIES
// ════════════════════════════════════════════════════════════

// Unified fetch wrapper — throws on 401
async function apiFetch(url, token, opts = {}) {
    const res = await fetch(url, {
        ...opts,
        headers: { 'Authorization': `Bearer ${token}`, ...(opts.headers || {}) }
    });
    if (res.status === 401) {
        localStorage.removeItem('accessToken');
        window.location.href = '/';
        throw new Error('UNAUTHORIZED');
    }
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res;
}

// Token accessor used in inline onclick attrs that need the token
window.token_ref = function () { return localStorage.getItem('accessToken'); };

function escHtml(str) {
    if (!str) return '';
    return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function downloadFile(content, filename, mime) {
    const a = document.createElement('a');
    a.href = URL.createObjectURL(new Blob([content], { type: mime }));
    a.download = filename;
    a.click();
    URL.revokeObjectURL(a.href);
}

// Toast notification
function showToast(msg, type = 'success') {
    let container = document.getElementById('toast-container');
    if (!container) {
        container = document.createElement('div');
        container.id = 'toast-container';
        container.style.cssText = 'position:fixed;bottom:1.5rem;right:1.5rem;z-index:9999;display:flex;flex-direction:column;gap:.5rem;';
        document.body.appendChild(container);
    }
    const toast = document.createElement('div');
    toast.style.cssText = `
        padding:.75rem 1.25rem;border-radius:8px;font-size:.875rem;font-weight:500;
        box-shadow:0 4px 12px rgba(0,0,0,.15);animation:slideInRight .25s ease;
        background:${type === 'success' ? '#d1fae5' : '#fee2e2'};
        color:${type === 'success' ? '#065f46' : '#991b1b'};
        border:1px solid ${type === 'success' ? '#a7f3d0' : '#fecaca'};
    `;
    toast.textContent = msg;
    container.appendChild(toast);
    setTimeout(() => { toast.style.opacity = '0'; toast.style.transition = 'opacity .3s'; setTimeout(() => toast.remove(), 300); }, 3500);
}

window.downloadBulkTemplate = function () {
    const headers = 'SKU,Product Name,Category,Warehouse,Cost Price,Sell Price,Initial Stock,Min Stock';
    const example = 'TECH-001,Wireless Mouse,electronics,main,8.50,19.99,100,20';
    const csv = `${headers}\n${example}\n`;
    const a = document.createElement('a');
    a.href = URL.createObjectURL(new Blob([csv], { type: 'text/csv' }));
    a.download = 'bizpulse_import_template.csv';
    a.click();
    URL.revokeObjectURL(a.href);
};