document.addEventListener('DOMContentLoaded', () => {
    const searchInput = document.getElementById('search-input');
    const searchButton = document.getElementById('search-button');
    const cardsContainer = document.querySelector('.cards-container');
    const detailsContainer = document.querySelector('.details-container');

    searchButton.addEventListener('click', performSearch);
    searchInput.addEventListener('keyup', (event) => {
        if (event.key === 'Enter') {
            performSearch();
        }
    });

    async function performSearch() {
        const query = searchInput.value;
        if (query.length < 3) {
            alert('Please enter at least 3 characters to search.');
            return;
        }

        const response = await fetch(`/api/search?q=${query}`);
        const results = await response.json();
        
        displayResults(results);
    }

    function displayResults(results) {
        cardsContainer.innerHTML = '';
        detailsContainer.innerHTML = '';

        if (results.length === 0) {
            cardsContainer.innerHTML = '<p>No results found.</p>';
            return;
        }

        results.forEach(result => {
            const card = document.createElement('div');
            card.classList.add('card');
            card.dataset.accountNumber = result.account_number;
            card.innerHTML = `
                <h3>${result.account_name}</h3>
                <p>${result.account_number}</p>
                <p>${result.account_address}</p>
            `;
            card.addEventListener('click', () => {
                document.querySelectorAll('.card').forEach(c => c.classList.remove('active'));
                card.classList.add('active');
                displayDetails(result.account_number);
            });
            cardsContainer.appendChild(card);
        });
    }

    async function displayDetails(accountNumber) {
        const response = await fetch(`/api/customer/${accountNumber}`);
        const data = await response.json();

        if (data.error) {
            detailsContainer.innerHTML = `<p class="error">${data.error}</p>`;
            return;
        }

        const customer = data.customer;
        detailsContainer.innerHTML = `
            <div class="customer-info-card">
                <h2>${customer.account_name}</h2>
                <div class="info-grid">
                    <p><strong>Account Number:</strong> ${customer.account_number}</p>
                    <p><strong>Address:</strong> ${customer.account_address}</p>
                    <p><strong>Business Unit:</strong> ${customer.business_unit}</p>
                    <p><strong>Account Officer:</strong> ${customer.account_officer}</p>
                    <p><strong>Phone:</strong> ${customer.phone_number || '--'}</p>
                </div>
            </div>
            
            <div class="tables-grid">
                <div class="table-section">
                    <h3>Payment History</h3>
                    <div class="table-container">
                        <table>
                            <thead>
                                <tr>
                                    <th>Date</th>
                                    <th>Amount Paid</th>
                                    <th>Transaction ID</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${data.collections.length > 0 ? data.collections.map(c => `
                                    <tr>
                                        <td>${new Date(c.date_of_payment).toLocaleDateString()}</td>
                                        <td>₦${Number(c.amount_paid).toLocaleString()}</td>
                                        <td>${c.transaction_id}</td>
                                    </tr>
                                `).join('') : '<tr><td colspan="3">No collections found</td></tr>'}
                            </tbody>
                        </table>
                    </div>
                </div>

                <div class="table-section">
                    <h3>Disconnection History</h3>
                    <div class="table-container">
                        <table>
                            <thead>
                                <tr>
                                    <th>Date</th>
                                    <th>Reason</th>
                                    <th>Status</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${data.disconnections.length > 0 ? data.disconnections.map(d => `
                                    <tr>
                                        <td>${new Date(d.date_of_disconnection).toLocaleDateString()}</td>
                                        <td>${d.reason || '--'}</td>
                                        <td>${d.status || '--'}</td>
                                    </tr>
                                `).join('') : '<tr><td colspan="3">No disconnections found</td></tr>'}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        `;
    }
});
