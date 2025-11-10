document.addEventListener('DOMContentLoaded', () => {
    const token = localStorage.getItem('accessToken');
    if (!token) {
        window.location.href = '/';
        return;
    }

    const connectionForm = document.getElementById('connection-form');
    const messageArea = document.getElementById('form-message');

    loadConnections(token);

    connectionForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        const button = e.target.querySelector('button');
        const buttonText = button.querySelector('.button-text');

        const connectionData = {
            connection_name: document.getElementById('connection_name').value,
            db_type: document.getElementById('db_type').value,
            db_host: document.getElementById('db_host').value,
            db_port: parseInt(document.getElementById('db_port').value),
            db_user: document.getElementById('db_user').value,
            db_password: document.getElementById('db_password').value,
            db_name: document.getElementById('db_name').value,
        };

        button.disabled = true;
        buttonText.textContent = 'Testing...';
        messageArea.classList.remove('show');

        try {
            const response = await fetch('/api/connections', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${token}`
                },
                body: JSON.stringify(connectionData)
            });

            const result = await response.json();

            if (response.ok) {
                showMessage(result.message, 'success');
                connectionForm.reset();
                loadConnections(token);
            } else {
                showMessage(result.detail || 'An unknown error occurred.', 'error');
            }
        } catch (error) {
            showMessage('A network error occurred. Please try again.', 'error');
        } finally {
            button.disabled = false;
            buttonText.textContent = 'Test & Save Connection';
        }
    });
});

async function loadConnections(token) {
    const listContainer = document.getElementById('connections-list');
    listContainer.innerHTML = '<p>Loading your connections...</p>';

    try {
        const response = await fetch('/api/connections', {
            headers: { 'Authorization': `Bearer ${token}` }
        });
        if (!response.ok) throw new Error('Failed to load connections');
        
        const connections = await response.json();

        if (connections.length === 0) {
            listContainer.innerHTML = '<p>You have not added any data connections yet.</p>';
            return;
        }

        let html = '<ul>';
        connections.forEach(conn => {
            html += `
                <li style="margin-bottom: 1rem; padding-bottom: 1rem; border-bottom: 1px solid #f0f0f0;">
                    <strong>${conn.connection_name}</strong> (${conn.db_type})<br>
                    <small>${conn.db_user}@${conn.db_host}:${conn.db_port}/${conn.db_name}</small><br>
                    <small>Status: ${conn.is_valid ? '<span style="color: green;">Verified</span>' : '<span style="color: red;">Failed</span>'}</small>
                </li>
            `;
        });
        html += '</ul>';
        listContainer.innerHTML = html;

    } catch (error) {
        listContainer.innerHTML = '<p style="color: red;">Could not load connections.</p>';
    }
}

function showMessage(message, type = 'error') {
    const messageArea = document.getElementById('form-message');
    messageArea.textContent = message;
    messageArea.style.background = type === 'success' ? '#d1fae5' : '#fee';
    messageArea.style.color = type === 'success' ? '#065f46' : '#c53030';
    messageArea.style.borderColor = type === 'success' ? '#a7f3d0' : '#fcc';
    messageArea.classList.add('show');
}