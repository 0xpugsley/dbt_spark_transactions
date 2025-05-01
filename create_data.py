# Script to generate sample transactions data
import pandas as pd
import random

data = {
    'txn_id': range(1000),
    'txn_date': [pd.Timestamp('2024-01-01') + pd.Timedelta(days=random.randint(0, 365)) for _ in range(1000)],
    'amount': [random.uniform(10, 5000) for _ in range(1000)],
    'description': random.choices(['bought banana at store', 'payment for services', 'grocery shopping apple'], k=1000),
    'code': random.choices(['TXN001', 'TXN002', 'PAY001'], k=1000),
    'country_code': random.choices(['FR', 'DE', 'ES', 'IT'], k=1000),
    'counterparty_number': [f'ACC{random.randint(1000,9999)}' for _ in range(1000)],
    'counterparty_name': random.choices(['Market Ltd', 'Tech Corp', 'Food Store Inc'], k=1000)
}
df = pd.DataFrame(data)
df.to_csv('seeds/raw_transactions.csv', index=False)