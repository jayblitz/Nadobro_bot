const tg = window.Telegram.WebApp;
tg.ready();
tg.expand();

const state = {
    currentTab: 'dashboard',
    user: null,
    prices: {},
    positions: [],
    balance: null,
    wallet: null,
    selectedProduct: 'BTC',
    direction: 'long',
    leverage: 1,
    priceInterval: null,
    positionInterval: null,
    tradeSubmitting: false,
    productList: null,
};

function applyTheme() {
    if (tg.themeParams) {
        const root = document.documentElement;
        if (tg.themeParams.bg_color) root.style.setProperty('--tg-theme-bg-color', tg.themeParams.bg_color);
        if (tg.themeParams.text_color) root.style.setProperty('--tg-theme-text-color', tg.themeParams.text_color);
        if (tg.themeParams.hint_color) root.style.setProperty('--tg-theme-hint-color', tg.themeParams.hint_color);
        if (tg.themeParams.button_color) root.style.setProperty('--tg-theme-button-color', tg.themeParams.button_color);
        if (tg.themeParams.button_text_color) root.style.setProperty('--tg-theme-button-text-color', tg.themeParams.button_text_color);
        if (tg.themeParams.secondary_bg_color) root.style.setProperty('--tg-theme-secondary-bg-color', tg.themeParams.secondary_bg_color);
    }
}

async function apiCall(url, options = {}) {
    const headers = { 'X-Telegram-Init-Data': tg.initData || '' };
    if (options.body && typeof options.body === 'object') {
        headers['Content-Type'] = 'application/json';
        options.body = JSON.stringify(options.body);
    }
    const resp = await fetch(url, { ...options, headers: { ...headers, ...options.headers } });
    const data = await resp.json();
    if (!resp.ok) {
        throw new Error(data.error || 'Request failed');
    }
    return data;
}

function showLoading() {
    document.getElementById('loadingOverlay').classList.add('visible');
}

function hideLoading() {
    document.getElementById('loadingOverlay').classList.remove('visible');
}

let toastTimeout = null;
function showToast(message, type = 'info') {
    const el = document.getElementById('toast');
    el.textContent = message;
    el.className = 'toast ' + type;
    requestAnimationFrame(() => { el.classList.add('visible'); });
    if (toastTimeout) clearTimeout(toastTimeout);
    toastTimeout = setTimeout(() => { el.classList.remove('visible'); }, 3000);
}

function formatUSD(val) {
    if (val === null || val === undefined || isNaN(val)) return '--';
    return '$' + Number(val).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function formatPrice(val, product) {
    if (val === null || val === undefined || isNaN(val)) return '--';
    const n = Number(val);
    if (['BTC'].includes(product)) return n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    if (['ETH', 'BNB'].includes(product)) return n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    if (n >= 1) return n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 4 });
    return n.toLocaleString('en-US', { minimumFractionDigits: 4, maximumFractionDigits: 6 });
}

function formatSize(val) {
    if (val === null || val === undefined) return '--';
    const n = Number(val);
    if (Math.abs(n) >= 1) return n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 4 });
    return n.toLocaleString('en-US', { minimumFractionDigits: 4, maximumFractionDigits: 6 });
}

function getSizeStep(product) {
    if (['BTC', 'ETH'].includes(product)) return 0.001;
    return 0.1;
}

function switchTab(tab) {
    state.currentTab = tab;
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
    document.getElementById('page-' + tab).classList.add('active');
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelector('.tab[data-tab="' + tab + '"]').classList.add('active');

    if (tab === 'dashboard') loadDashboard();
    if (tab === 'trade') loadTrade();
    if (tab === 'positions') loadPositions();
    if (tab === 'wallet') loadWallet();
}

async function initApp() {
    applyTheme();
    showLoading();
    try {
        try {
            const prodData = await fetch('/api/products').then(r => r.json());
            if (prodData.products) {
                state.productList = prodData.products.map(p => p.name);
            }
        } catch (e) { /* fallback to hardcoded list */ }

        const userData = await apiCall('/api/user');
        state.user = userData;
        updateNetworkBadge(userData.network);

        if (userData.is_new && userData.mnemonic) {
            showMnemonicModal(userData.mnemonic);
        }

        await loadDashboard();
    } catch (e) {
        showToast('Failed to initialize: ' + e.message, 'error');
    } finally {
        hideLoading();
    }

    if (state.priceInterval) clearInterval(state.priceInterval);
    state.priceInterval = setInterval(refreshPrices, 15000);

    if (state.positionInterval) clearInterval(state.positionInterval);
    state.positionInterval = setInterval(() => {
        if (state.currentTab === 'positions' || state.currentTab === 'dashboard') {
            refreshPositions();
        }
    }, 30000);
}

function updateNetworkBadge(network) {
    const badge = document.getElementById('networkBadge');
    badge.textContent = network;
    badge.className = 'network-badge' + (network === 'mainnet' ? ' mainnet' : '');
}

async function loadDashboard() {
    try {
        const [balData, priceData] = await Promise.all([
            apiCall('/api/balance'),
            apiCall('/api/prices'),
        ]);

        state.balance = balData;
        state.prices = priceData.prices || {};

        const balStr = balData.exists ? formatUSD(balData.usdt_balance) : '$0.00';
        document.getElementById('dashBalance').textContent = balStr;
        document.getElementById('headerBalance').textContent = balStr;

        renderPriceGrid();
        await refreshPositions();
    } catch (e) {
        showToast('Error loading data: ' + e.message, 'error');
    }
}

async function refreshPrices() {
    try {
        const data = await apiCall('/api/prices');
        state.prices = data.prices || {};
        if (state.currentTab === 'dashboard') renderPriceGrid();
        if (state.currentTab === 'trade') updateTradePrice();
    } catch (e) { /* silent */ }
}

async function refreshPositions() {
    try {
        const data = await apiCall('/api/positions');
        state.positions = data.positions || [];
        if (state.currentTab === 'positions') renderPositionsList();
        if (state.currentTab === 'dashboard') renderDashPositions();
    } catch (e) { /* silent */ }
}

function getProductList() {
    return state.productList || ['BTC', 'ETH', 'SOL', 'XRP', 'BNB', 'LINK', 'DOGE', 'AVAX'];
}

function renderPriceGrid() {
    const grid = document.getElementById('priceGrid');
    const products = getProductList();
    let html = '';
    products.forEach(p => {
        const info = state.prices[p];
        const mid = info ? formatPrice(info.mid, p) : '--';
        const spread = info ? (Number(info.ask) - Number(info.bid)).toFixed(4) : '--';
        html += '<div class="price-card" onclick="goToTrade(\'' + p + '\')">' +
            '<div class="price-card-symbol">' + p + '-PERP</div>' +
            '<div class="price-card-price">' + mid + '</div>' +
            '<div class="price-card-spread">Spread: ' + spread + '</div>' +
            '</div>';
    });
    grid.innerHTML = html;
}

function renderDashPositions() {
    const container = document.getElementById('dashPositions');
    if (!state.positions.length) {
        container.innerHTML = '<div class="empty-state">No open positions</div>';
        return;
    }
    let html = '';
    state.positions.slice(0, 3).forEach(pos => {
        const sideClass = pos.side === 'long' ? 'long' : 'short';
        const productName = pos.product_name || ('ID:' + pos.product_id);
        html += '<div class="position-card">' +
            '<div class="position-header">' +
            '<span class="position-product">' + productName + '</span>' +
            '<span class="position-side ' + sideClass + '">' + pos.side + '</span>' +
            '</div>' +
            '<div class="position-details">' +
            '<div><div class="position-detail-label">Size</div><div class="position-detail-value">' + formatSize(pos.amount) + '</div></div>' +
            '<div><div class="position-detail-label">Entry</div><div class="position-detail-value">' + formatPrice(pos.price, productName) + '</div></div>' +
            '</div></div>';
    });
    if (state.positions.length > 3) {
        html += '<div class="empty-state">' + (state.positions.length - 3) + ' more position(s)</div>';
    }
    container.innerHTML = html;
}

function goToTrade(product) {
    state.selectedProduct = product;
    switchTab('trade');
}

function quickTrade(direction) {
    state.direction = direction;
    switchTab('trade');
}

function loadTrade() {
    document.querySelectorAll('.product-btn').forEach(btn => {
        btn.classList.toggle('selected', btn.dataset.product === state.selectedProduct);
    });

    const step = getSizeStep(state.selectedProduct);
    const input = document.getElementById('sizeInput');
    input.step = step;
    if (Number(input.value) === 0 || isNaN(Number(input.value))) {
        input.value = step;
    }

    setDirection(state.direction);
    updateTradePrice();
    updateMargin();
}

document.querySelectorAll('.product-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        state.selectedProduct = btn.dataset.product;
        document.querySelectorAll('.product-btn').forEach(b => b.classList.remove('selected'));
        btn.classList.add('selected');
        const step = getSizeStep(state.selectedProduct);
        document.getElementById('sizeInput').step = step;
        updateTradePrice();
        updateMargin();
    });
});

function setDirection(dir) {
    state.direction = dir;
    const btnLong = document.getElementById('btnLong');
    const btnShort = document.getElementById('btnShort');
    const submitBtn = document.getElementById('submitTrade');
    btnLong.classList.toggle('active', dir === 'long');
    btnShort.classList.toggle('active', dir === 'short');
    submitBtn.className = 'btn btn-trade ' + (dir === 'long' ? 'long-active' : 'short-active');
    submitBtn.textContent = dir === 'long' ? 'Open Long' : 'Open Short';
}

function adjustSize(direction) {
    const input = document.getElementById('sizeInput');
    const step = getSizeStep(state.selectedProduct);
    let val = parseFloat(input.value) || 0;
    val += step * direction;
    if (val < 0) val = 0;
    val = Math.round(val * 1000000) / 1000000;
    input.value = val;
    updateMargin();
}

function updateLeverage() {
    const slider = document.getElementById('leverageSlider');
    state.leverage = parseInt(slider.value);
    document.getElementById('leverageValue').textContent = state.leverage;
    updateMargin();
}

function updateTradePrice() {
    const info = state.prices[state.selectedProduct];
    const el = document.getElementById('tradePrice');
    if (info) {
        el.textContent = formatPrice(info.mid, state.selectedProduct);
    } else {
        el.textContent = '--';
    }
    updateMargin();
}

function updateMargin() {
    const size = parseFloat(document.getElementById('sizeInput').value) || 0;
    const info = state.prices[state.selectedProduct];
    const price = info ? Number(info.mid) : 0;
    const leverage = state.leverage || 1;
    const margin = (size * price) / leverage;
    document.getElementById('estMargin').textContent = formatUSD(margin);
}

document.getElementById('sizeInput').addEventListener('input', updateMargin);

async function submitTrade() {
    if (state.tradeSubmitting) return;
    state.tradeSubmitting = true;
    const btn = document.getElementById('submitTrade');
    btn.disabled = true;

    const size = parseFloat(document.getElementById('sizeInput').value);
    if (!size || size <= 0) {
        showToast('Enter a valid size', 'error');
        btn.disabled = false;
        state.tradeSubmitting = false;
        return;
    }

    showLoading();
    try {
        const result = await apiCall('/api/trade', {
            method: 'POST',
            body: {
                product: state.selectedProduct,
                size: size,
                action: state.direction,
                leverage: state.leverage,
                order_type: 'market',
            },
        });
        showToast('Trade executed successfully', 'success');
        refreshPrices();
        refreshPositions();
        loadBalance();
    } catch (e) {
        showToast(e.message, 'error');
    } finally {
        hideLoading();
        btn.disabled = false;
        setTimeout(() => { state.tradeSubmitting = false; }, 1000);
    }
}

async function loadBalance() {
    try {
        const data = await apiCall('/api/balance');
        state.balance = data;
        const balStr = data.exists ? formatUSD(data.usdt_balance) : '$0.00';
        document.getElementById('headerBalance').textContent = balStr;
        if (state.currentTab === 'dashboard') {
            document.getElementById('dashBalance').textContent = balStr;
        }
    } catch (e) { /* silent */ }
}

async function loadPositions() {
    try {
        const data = await apiCall('/api/positions');
        state.positions = data.positions || [];
        renderPositionsList();
    } catch (e) {
        showToast('Error loading positions: ' + e.message, 'error');
    }
}

function renderPositionsList() {
    const container = document.getElementById('positionsList');
    const closeAllBtn = document.getElementById('closeAllBtn');
    if (!state.positions.length) {
        container.innerHTML = '<div class="empty-state">No open positions</div>';
        closeAllBtn.style.display = 'none';
        return;
    }
    closeAllBtn.style.display = 'block';

    let html = '';
    state.positions.forEach(pos => {
        const sideClass = pos.side === 'long' ? 'long' : 'short';
        const productName = pos.product_name || ('ID:' + pos.product_id);
        const baseName = productName.replace('-PERP', '');
        const priceInfo = state.prices[baseName];
        const currentPrice = priceInfo ? formatPrice(priceInfo.mid, baseName) : '--';
        let pnlHtml = '<span class="position-pnl">--</span>';
        if (priceInfo && pos.price) {
            const cur = Number(priceInfo.mid);
            const entry = Number(pos.price);
            const amt = Math.abs(Number(pos.amount));
            let pnl = pos.side === 'long' ? (cur - entry) * amt : (entry - cur) * amt;
            const pnlClass = pnl >= 0 ? 'positive' : 'negative';
            const pnlSign = pnl >= 0 ? '+' : '';
            pnlHtml = '<span class="position-pnl ' + pnlClass + '">' + pnlSign + formatUSD(pnl) + '</span>';
        }

        html += '<div class="position-card">' +
            '<div class="position-header">' +
            '<span class="position-product">' + productName + '</span>' +
            '<span class="position-side ' + sideClass + '">' + pos.side + '</span>' +
            '</div>' +
            '<div class="position-details">' +
            '<div><div class="position-detail-label">Size</div><div class="position-detail-value">' + formatSize(pos.amount) + '</div></div>' +
            '<div><div class="position-detail-label">Entry</div><div class="position-detail-value">' + formatPrice(pos.price, baseName) + '</div></div>' +
            '<div><div class="position-detail-label">Current</div><div class="position-detail-value">' + currentPrice + '</div></div>' +
            '<div><div class="position-detail-label">PnL</div><div class="position-detail-value">' + pnlHtml + '</div></div>' +
            '</div>' +
            '<div class="position-footer">' +
            '<button class="btn btn-close" onclick="closePosition(\'' + baseName + '\')">Close</button>' +
            '</div></div>';
    });
    container.innerHTML = html;
}

async function closePosition(product) {
    showLoading();
    try {
        await apiCall('/api/close', { method: 'POST', body: { product: product } });
        showToast('Position closed', 'success');
        await loadPositions();
        loadBalance();
    } catch (e) {
        showToast(e.message, 'error');
    } finally {
        hideLoading();
    }
}

async function closeAllPositions() {
    showLoading();
    try {
        await apiCall('/api/close', { method: 'POST', body: { close_all: true } });
        showToast('All positions closed', 'success');
        await loadPositions();
        loadBalance();
    } catch (e) {
        showToast(e.message, 'error');
    } finally {
        hideLoading();
    }
}

async function loadWallet() {
    try {
        const data = await apiCall('/api/wallet');
        state.wallet = data;
        document.getElementById('walletAddress').textContent = data.active_address || '--';

        const network = data.network || 'testnet';
        document.getElementById('btnTestnet').classList.toggle('active', network === 'testnet');
        document.getElementById('btnMainnet').classList.toggle('active', network === 'mainnet');
        document.getElementById('faucetSection').style.display = network === 'testnet' ? 'block' : 'none';
    } catch (e) {
        showToast('Error loading wallet: ' + e.message, 'error');
    }
}

async function switchNetwork(network) {
    showLoading();
    try {
        const result = await apiCall('/api/network', { method: 'POST', body: { network: network } });
        if (result.success) {
            state.user.network = network;
            updateNetworkBadge(network);
            showToast('Switched to ' + network, 'success');
            loadWallet();
            loadBalance();
            refreshPrices();
        }
    } catch (e) {
        showToast(e.message, 'error');
    } finally {
        hideLoading();
    }
}

async function copyAddress() {
    const addr = document.getElementById('walletAddress').textContent;
    if (!addr || addr === '--') return;
    try {
        await navigator.clipboard.writeText(addr);
        showToast('Address copied', 'success');
    } catch (e) {
        const ta = document.createElement('textarea');
        ta.value = addr;
        ta.style.position = 'fixed';
        ta.style.opacity = '0';
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
        showToast('Address copied', 'success');
    }
}

function showMnemonicModal(mnemonic) {
    const words = mnemonic.split(' ');
    const container = document.getElementById('mnemonicWords');
    let html = '';
    words.forEach((word, i) => {
        html += '<div class="mnemonic-word"><span class="word-num">' + (i + 1) + '</span>' + word + '</div>';
    });
    container.innerHTML = html;
    document.getElementById('mnemonicModal').classList.add('visible');
}

function closeMnemonicModal() {
    document.getElementById('mnemonicModal').classList.remove('visible');
}

document.addEventListener('DOMContentLoaded', initApp);
