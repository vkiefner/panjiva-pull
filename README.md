# Panjiva pull

Small ETL project for the Panjiva API: explore the API, then pull US import/
export shipments into Parquet. Built to respect Panjiva's documented limits
(20 req/s per token) and to use the sanctioned async/bulk path for volume.

## Structure

```
panjiva-pull/
├── code/
│   ├── client.py    # OAuth token + rate limiting + retries (shared)
│   ├── explore.py   # run FIRST: auth, schema, pagination, rate, async check
│   └── pull.py      # async bulk pull -> clean -> partitioned Parquet
├── data/            # gitignored; point PANJIVA_DATA_DIR at your Z drive
│   └── raw/         # pull_<timestamp>/<source>/year=/month=/data.parquet
├── .env.example     # copy to .env and fill in
├── .gitignore
└── requirements.txt
```

## Setup

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt python-dateutil
cp .env.example .env        # then edit .env
```

Get `client_id` / `client_secret` from **panjiva.com → myPanjiva → 🔧 API
Settings**. If you don't see "API Settings" there, your account isn't entitled
to the API yet — ask your Panjiva rep to enable it (same for async/bulk).

Before the first run, open the [API Reference](https://panjiva.com/api-guide/spec)
and confirm the **base URL** and the **endpoint paths** in `.env`, plus the exact
**data-source names** and **filter field names** used in `explore.py` / `pull.py`.
They're all in one place so you only fix them once.

## Run order

```bash
cd code
python client.py     # smoke test: can I get a token?
python explore.py    # maps schema, pagination, rate behavior, async availability
python pull.py       # the actual pull (resumable)
```

`explore.py` tells you two things that decide how `pull.py` behaves:
the real field names (so `clean_record` produces a stable WRDS-style schema),
and whether async/bulk is enabled (the efficient path for volume).

## Scope note

Keep `SOURCES` / `DATE_FROM` / `DATE_TO` in `pull.py` within what your trial
permits. The async bulk cap is 1,000,000 records per job; the code chunks by
month to stay well under it and to make the run resumable. If you need more
history or volume than the trial allows, that's a quota request to your rep —
not something to engineer around.
```
