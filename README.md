# therocktrading-blockchain-data
collect data to be submitted to the therocktrading bankruptcy procedure

# Create a virtual environment
```bash
python3.11 -m venv .venv
```

# Install requirements
The projects needs a [blockchair API key](https://blockchair.com/api), to query their database.

```bash
.venv/bin/python -m pip install -e .[dev]
```

# Run 

```bash
.venv/bin/python therocktrading_blockchain_data/analyze_transactions.py \
    --transactions ~/Documents/therocktrading/matteo/transactions_23734_11130830.csv \
    --blockchair_api_key $(vault kv get -mount=secret -field api_key blockchair) \
    --output_file transactions.csv
```
