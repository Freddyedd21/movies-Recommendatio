// ============================================
// CONFIGURACIÓN
// ============================================

const API_BASE_URL = 'http://34.66.170.230:8000';

// Estado global
let allUsersData = [];
let currentJsonViewerOpen = false;

// ============================================
// FUNCIONES DE UTILIDAD
// ============================================

function showLoading(containerId) {
    const container = document.getElementById(containerId);
    if (container) {
        container.innerHTML = `
            <div class="loading">
                <div class="loading-spinner"></div>
                <p>Cargando datos...</p>
            </div>
        `;
    }
}

function hideLoading(containerId) {
    // El contenido se reemplazará al cargar los datos
}

function showError(containerId, message) {
    const container = document.getElementById(containerId);
    if (container) {
        container.innerHTML = `
            <div class="error">
                ❌ Error: ${message}
            </div>
        `;
    }
}

async function fetchAPI(endpoint) {
    const response = await fetch(`${API_BASE_URL}${endpoint}`);
    if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }
    return response.json();
}

// ============================================
// VERIFICACIÓN DE API
// ============================================

async function checkAPIStatus() {
    const statusElement = document.getElementById('apiStatus');
    const statusDot = statusElement?.querySelector('.status-dot');
    
    try {
        const response = await fetch(`${API_BASE_URL}/health`);
        if (response.ok) {
            statusElement.innerHTML = `
                <span class="status-dot online"></span>
                API Online
            `;
        } else {
            throw new Error('API no responde');
        }
    } catch (error) {
        statusElement.innerHTML = `
            <span class="status-dot offline"></span>
            API Offline
        `;
        console.error('API status check failed:', error);
    }
}

// ============================================
// ESTADÍSTICAS
// ============================================

function updateStats(users) {
    const totalRecs = users.reduce((sum, u) => sum + (u.recommendations?.length || 0), 0);
    const uniqueClusters = [...new Set(users.map(u => u.cluster))];
    
    const statsHtml = `
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-number">${users.length}</div>
                <div class="stat-label">Usuarios</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">${totalRecs}</div>
                <div class="stat-label">Recomendaciones</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">${(totalRecs / users.length).toFixed(1)}</div>
                <div class="stat-label">Promedio/usuario</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">${Math.min(...uniqueClusters)} - ${Math.max(...uniqueClusters)}</div>
                <div class="stat-label">Clusters</div>
            </div>
        </div>
    `;
    
    const statsContainer = document.getElementById('statsContainer');
    if (statsContainer) {
        statsContainer.innerHTML = statsHtml;
    }
}

// ============================================
// MOSTRAR RESULTADO DE UN USUARIO
// ============================================

function displayUserResult(user) {
    const recommendationsHtml = user.recommendations.map((rec, index) => `
        <div class="rec-card" onclick="showMovieInfo(${rec.movie_id}, '${escapeHtml(rec.movie_title)}')">
            <div class="rec-rank">#${index + 1}</div>
            <div class="rec-title">${escapeHtml(rec.movie_title)}</div>
            <span class="rec-score">⭐ ${rec.score}</span>
            <div class="rec-id">ID: ${rec.movie_id}</div>
        </div>
    `).join('');
    
    const html = `
        <div class="user-info">
            <div>
                <span class="badge badge-primary">👤 Usuario ID: ${user.user_id}</span>
                <span class="badge badge-success">🏷️ Cluster: ${user.cluster}</span>
            </div>
            <span class="badge badge-info">📊 ${user.recommendations.length} recomendaciones</span>
        </div>
        <h3>🎯 Top Recomendaciones</h3>
        <div class="recommendations-grid">
            ${recommendationsHtml}
        </div>
        <button class="json-toggle" onclick="toggleJsonViewer('${escapeHtml(JSON.stringify(user))}')">
            📄 Ver JSON
        </button>
        <div id="jsonViewer" class="json-viewer" style="display: none;"></div>
    `;
    
    const resultContent = document.getElementById('resultContent');
    if (resultContent) {
        resultContent.innerHTML = html;
    }
    
    // Mostrar la tarjeta de resultados
    const resultCard = document.getElementById('resultCard');
    if (resultCard) {
        resultCard.style.display = 'block';
    }
    
    // Ocultar la tarjeta de todos los usuarios
    const allUsersCard = document.getElementById('allUsersCard');
    if (allUsersCard) {
        allUsersCard.style.display = 'none';
    }
}

// ============================================
// MOSTRAR TODOS LOS USUARIOS
// ============================================

function displayAllUsers(users) {
    const usersHtml = users.map(user => `
        <div class="user-item" onclick="searchUserById(${user.user_id})">
            <div class="user-header">
                <div>
                    <span class="badge badge-primary">👤 ID: ${user.user_id}</span>
                    <span class="badge badge-success">🏷️ Cluster: ${user.cluster}</span>
                </div>
                <span>🎬 ${user.recommendations.length} recs</span>
            </div>
            <div class="user-preview">
                ${user.recommendations.slice(0, 3).map(rec => `
                    <span class="preview-movie">${escapeHtml(rec.movie_title.substring(0, 30))}${rec.movie_title.length > 30 ? '...' : ''}</span>
                `).join('')}
                ${user.recommendations.length > 3 ? `<span class="preview-movie">+${user.recommendations.length - 3} más</span>` : ''}
            </div>
        </div>
    `).join('');
    
    const html = `
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-number">${users.length}</div>
                <div class="stat-label">Usuarios totales</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">${users.reduce((sum, u) => sum + u.recommendations.length, 0)}</div>
                <div class="stat-label">Recomendaciones totales</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">${(users.reduce((sum, u) => sum + u.recommendations.length, 0) / users.length).toFixed(1)}</div>
                <div class="stat-label">Promedio de recs</div>
            </div>
        </div>
        <div class="users-list">
            ${usersHtml}
        </div>
        <button class="json-toggle" onclick="toggleAllUsersJsonViewer()">
            📄 Ver JSON completo
        </button>
        <div id="allUsersJsonViewer" class="json-viewer" style="display: none;"></div>
    `;
    
    const allUsersContent = document.getElementById('allUsersContent');
    if (allUsersContent) {
        allUsersContent.innerHTML = html;
    }
    
    // Mostrar la tarjeta de todos los usuarios
    const allUsersCard = document.getElementById('allUsersCard');
    if (allUsersCard) {
        allUsersCard.style.display = 'block';
    }
    
    // Ocultar la tarjeta de resultados individuales
    const resultCard = document.getElementById('resultCard');
    if (resultCard) {
        resultCard.style.display = 'none';
    }
}

// ============================================
// FUNCIONES DE CONSULTA API
// ============================================

async function searchUser() {
    const userId = document.getElementById('userId')?.value;
    if (!userId) {
        alert('Por favor ingresa un ID de usuario');
        return;
    }
    
    showLoading('resultContent');
    
    try {
        const data = await fetchAPI(`/recommendations/${userId}`);
        displayUserResult(data);
        updateStats([data]);
    } catch (error) {
        showError('resultContent', `Usuario ${userId} no encontrado. ${error.message}`);
    }
}

async function searchUserById(userId) {
    document.getElementById('userId').value = userId;
    await searchUser();
}

async function randomUser() {
    // IDs disponibles según tu JSON
    const userIds = [1, 2, 3, 4, 5];
    const randomId = userIds[Math.floor(Math.random() * userIds.length)];
    document.getElementById('userId').value = randomId;
    await searchUser();
}

async function loadAllUsers() {
    showLoading('allUsersContent');
    
    try {
        const data = await fetchAPI('/recommendations');
        allUsersData = data.users || data;
        displayAllUsers(allUsersData);
        updateStats(allUsersData);
    } catch (error) {
        showError('allUsersContent', `Error al cargar usuarios: ${error.message}`);
    }
}

// ============================================
// FUNCIONES UI
// ============================================

function escapeHtml(str) {
    return str.replace(/[&<>]/g, function(m) {
        if (m === '&') return '&amp;';
        if (m === '<') return '&lt;';
        if (m === '>') return '&gt;';
        return m;
    });
}

function showMovieInfo(movieId, title) {
    alert(`🎬 ${title}\n📽️ ID: ${movieId}\n✨ Recomendada por similitud con otros usuarios de tu cluster.`);
}

function toggleJsonViewer(jsonStr) {
    const viewer = document.getElementById('jsonViewer');
    if (viewer.style.display === 'none' || viewer.style.display === '') {
        viewer.style.display = 'block';
        viewer.innerHTML = `<pre>${JSON.stringify(JSON.parse(jsonStr), null, 2)}</pre>`;
    } else {
        viewer.style.display = 'none';
    }
}

function toggleAllUsersJsonViewer() {
    const viewer = document.getElementById('allUsersJsonViewer');
    if (viewer.style.display === 'none' || viewer.style.display === '') {
        viewer.style.display = 'block';
        viewer.innerHTML = `<pre>${JSON.stringify(allUsersData, null, 2)}</pre>`;
    } else {
        viewer.style.display = 'none';
    }
}

function showInfo() {
    const modal = document.getElementById('infoModal');
    if (modal) {
        modal.style.display = 'block';
    }
}

function showDocs() {
    const modal = document.getElementById('docsModal');
    if (modal) {
        modal.style.display = 'block';
    }
}

function closeModal() {
    const modal = document.getElementById('infoModal');
    if (modal) {
        modal.style.display = 'none';
    }
}

function closeDocsModal() {
    const modal = document.getElementById('docsModal');
    if (modal) {
        modal.style.display = 'none';
    }
}

// ============================================
// CIERRE DE MODAL CON CLICK FUERA
// ============================================

window.onclick = function(event) {
    const infoModal = document.getElementById('infoModal');
    const docsModal = document.getElementById('docsModal');
    
    if (event.target === infoModal) {
        infoModal.style.display = 'none';
    }
    if (event.target === docsModal) {
        docsModal.style.display = 'none';
    }
};

// ============================================
// INICIALIZACIÓN
// ============================================

document.addEventListener('DOMContentLoaded', () => {
    checkAPIStatus();
    loadAllUsers();
    
    // Permitir buscar con Enter
    const userIdInput = document.getElementById('userId');
    if (userIdInput) {
        userIdInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                searchUser();
            }
        });
    }
});