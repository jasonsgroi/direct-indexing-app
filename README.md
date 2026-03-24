# Blended-index Portfolio Builder

This small Python utility builds a non-taxable (e.g. tax-advantaged account) portfolio benchmarked to a blended index:

- 40% S&P 500
- 40% Nasdaq (sourced from Nasdaq-100 constituents)
- 20% Russell 3000

Rules enforced:
- No stock overlap across slices (a ticker appears only once)
- No more than 8% in any one company
- Limit to 100 total holdings
- Nasdaq slice is selected from the Nasdaq-100 CSV and other slices by market-cap

Usage:

1. Provide three CSV files with headers: `ticker,name,market_cap`.
2. Run either the CLI tool or the web app:

- CLI (generates `portfolio.csv` directly):

```bash
python portfolio.py --sp500 data/sp500.csv --nasdaq100 data/nasdaq100.csv --russell data/russell3000.csv --out portfolio.csv
```

- Web UI (opens in your browser):

```bash
# Start the server
python -m uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Then open:

**http://localhost:8000/**

The script writes `portfolio.csv` with columns `ticker,name,market_cap,source,weight_pct`.

Notes:
- The repository includes sample/mock CSVs in `data/` for demonstration. Replace them with up-to-date index constituent files if you want a real portfolio.
- This tool avoids distributing copyrighted index constituent lists — supply your own licensed data when needed.

API Usage:

- The FastAPI app exposes a `POST /build-portfolio` endpoint that accepts three multipart file fields: `sp500`, `nasdaq100`, and `russell`.
- Example using `curl` (uploads three CSVs and downloads the resulting CSV):

```bash
curl -F "sp500=@data/sp500.csv" -F "nasdaq100=@data/nasdaq100.csv" -F "russell=@data/russell3000.csv" \
	-o portfolio.csv \
	http://localhost:8000/build-portfolio
```

Or use the web upload form at the app root (`/`) served by the static `index.html`.

Background jobs and cleanup:

- You can submit long-running jobs to run asynchronously using `POST /jobs/build` with the same multipart fields `sp500`, `nasdaq100`, and `russell`. The endpoint returns a JSON `job_id`.
- Poll job status with `GET /jobs/{job_id}`. When the job is `done`, download the CSV at `GET /jobs/{job_id}/download`.
- Temporary input and output files are automatically cleaned up after a download; job entries are removed from the in-memory store on cleanup.

Example:

```bash
# submit background job
curl -X POST -F "sp500=@data/sp500.csv" -F "nasdaq100=@data/nasdaq100.csv" -F "russell=@data/russell3000.csv" http://localhost:8000/jobs/build
# poll status
curl http://localhost:8000/jobs/<job_id>
# download when done
curl -O http://localhost:8000/jobs/<job_id>/download
```

Authentication & Rate limiting:

- If you set `PORTFOLIO_API_KEY` in the environment, the API will require that clients send the header `X-API-Key` with that value.
- A basic rate limit applies per-client (by API key or IP): `PORTFOLIO_RATE_LIMIT` requests per minute (default 10). The limit is enforced in-memory.

CSV validation:

- Uploaded CSVs are validated for required headers (`ticker`, `market_cap`) and that `market_cap` parses as a number for the first rows. Files larger than 10 MB are rejected.
