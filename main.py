from fastapi import FastAPI, UploadFile, File, BackgroundTasks, Header, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
import tempfile
import asyncio
from concurrent.futures import ThreadPoolExecutor
import uuid
import os
import time
import csv
import io
from typing import Optional

from portfolio import build_portfolio

app = FastAPI()

# Serve static assets (the UI)
static_dir = os.path.join(os.path.dirname(__file__), "static")
os.makedirs(static_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# executor for CPU / I/O work
executor = ThreadPoolExecutor(max_workers=3)

# In-memory job store: job_id -> {status: pending|running|done|error, out: path, files: [paths], error: text, created: ts}
jobs = {}

# simple auth and rate-limit settings
API_KEY = os.environ.get("PORTFOLIO_API_KEY")
RATE_LIMIT_WINDOW = 60  # seconds
RATE_LIMIT_MAX = int(os.environ.get("PORTFOLIO_RATE_LIMIT", "10"))  # requests per window per client
rate_store = {}  # client_id -> list[timestamps]


def _client_id(request: Request, x_api_key: Optional[str]):
    if x_api_key:
        return f"key:{x_api_key}"
    ip = None
    try:
        ip = request.client.host # type: ignore
    except Exception:
        ip = "unknown"
    return f"ip:{ip}"


def check_api_key(x_api_key: Optional[str]):
    if API_KEY:
        if not x_api_key or x_api_key != API_KEY:
            raise HTTPException(status_code=401, detail="Invalid or missing API key")


def check_rate_limit(client_id: str):
    now = time.time()
    arr = rate_store.get(client_id, [])
    # drop old
    arr = [t for t in arr if t > now - RATE_LIMIT_WINDOW]
    if len(arr) >= RATE_LIMIT_MAX:
        raise HTTPException(status_code=429, detail="rate limit exceeded")
    arr.append(now)
    rate_store[client_id] = arr


def validate_csv_file(path: str, max_rows_check: int = 50, max_size_bytes: int = 10 * 1024 * 1024):
    """Basic CSV validation: headers and market_cap numeric. Returns (ok, message)."""
    try:
        if os.path.getsize(path) > max_size_bytes:
            return False, f"file too large (>{max_size_bytes} bytes)"
        with open(path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            headers = [h.lower() for h in reader.fieldnames or []]
            required = {"ticker", "market_cap"}
            if not required.issubset(set(headers)):
                return False, f"missing required headers: {required - set(headers)}"
            count = 0
            for r in reader:
                if count >= max_rows_check:
                    break
                count += 1
                mc = r.get("market_cap") or r.get("Market_Cap") or r.get("marketCap")
                if mc is None:
                    return False, "market_cap missing in a row"
                try:
                    float(str(mc).replace(',', ''))
                except Exception:
                    return False, f"invalid market_cap '{mc}' in row {count}"
    except Exception as e:
        return False, str(e)
    return True, "ok"


@app.get("/")
def read_root():
    # Serve the frontend UI
    return FileResponse(os.path.join(static_dir, "index.html"))


@app.post("/build-portfolio")
async def build_portfolio_endpoint(
    request: Request,
    sp500: UploadFile = File(...),
    nasdaq100: UploadFile = File(...),
    russell: UploadFile = File(...),
    x_api_key: Optional[str] = Header(None),
):
    """Synchronous blocking endpoint: runs build in threadpool and returns CSV response."""
    # auth + rate limit
    check_api_key(x_api_key)
    client = _client_id(request, x_api_key)
    check_rate_limit(client)

    t1 = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    t2 = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    t3 = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    out = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    try:
        t1.write(await sp500.read()); t1.flush(); t1.close()
        t2.write(await nasdaq100.read()); t2.flush(); t2.close()
        t3.write(await russell.read()); t3.flush(); t3.close()

        # validate uploads
        ok, msg = validate_csv_file(t1.name)
        if not ok:
            # cleanup
            for p in (t1.name, t2.name, t3.name, out.name):
                try:
                    if os.path.exists(p):
                        os.remove(p)
                except Exception:
                    pass
            raise HTTPException(status_code=400, detail=f"sp500.csv invalid: {msg}")
        ok, msg = validate_csv_file(t2.name)
        if not ok:
            for p in (t1.name, t2.name, t3.name, out.name):
                try:
                    if os.path.exists(p):
                        os.remove(p)
                except Exception:
                    pass
            raise HTTPException(status_code=400, detail=f"nasdaq100.csv invalid: {msg}")
        ok, msg = validate_csv_file(t3.name)
        if not ok:
            for p in (t1.name, t2.name, t3.name, out.name):
                try:
                    if os.path.exists(p):
                        os.remove(p)
                except Exception:
                    pass
            raise HTTPException(status_code=400, detail=f"russell.csv invalid: {msg}")

        loop = asyncio.get_running_loop()
        # run blocking work in threadpool
        await loop.run_in_executor(
            executor, build_portfolio, t1.name, t2.name, t3.name, out.name
        )

        # schedule cleanup of temp files after response via BackgroundTasks by client download (not here)
        return FileResponse(out.name, filename="portfolio.csv", media_type="text/csv")
    finally:
        # keep inputs for potential inspection; they will be deleted by cleanup endpoints or background jobs
        pass


def _run_job(job_id, t1name, t2name, t3name, outname, file_paths):
    jobs[job_id]["status"] = "running"
    try:
        build_portfolio(t1name, t2name, t3name, outname)
        jobs[job_id]["status"] = "done"
        jobs[job_id]["out"] = outname
    except Exception as e:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(e)
    finally:
        # keep file_paths listed for later cleanup
        jobs[job_id]["files"] = file_paths
        jobs[job_id]["finished"] = time.time()


@app.post("/jobs/build")
async def submit_job(
    request: Request,
    background_tasks: BackgroundTasks,
    sp500: UploadFile = File(...),
    nasdaq100: UploadFile = File(...),
    russell: UploadFile = File(...),
    x_api_key: Optional[str] = Header(None),
):
    """Submit a background job that returns a job id. Check status at /jobs/{job_id}."""
    # auth + rate-limit
    check_api_key(x_api_key)
    client = _client_id(request, x_api_key)
    check_rate_limit(client)

    t1 = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    t2 = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    t3 = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    out = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    # write uploads
    t1.write(await sp500.read()); t1.flush(); t1.close()
    t2.write(await nasdaq100.read()); t2.flush(); t2.close()
    t3.write(await russell.read()); t3.flush(); t3.close()

    # validate
    ok, msg = validate_csv_file(t1.name)
    if not ok:
        _cleanup_paths([t1.name, t2.name, t3.name, out.name])
        raise HTTPException(status_code=400, detail=f"sp500.csv invalid: {msg}")
    ok, msg = validate_csv_file(t2.name)
    if not ok:
        _cleanup_paths([t1.name, t2.name, t3.name, out.name])
        raise HTTPException(status_code=400, detail=f"nasdaq100.csv invalid: {msg}")
    ok, msg = validate_csv_file(t3.name)
    if not ok:
        _cleanup_paths([t1.name, t2.name, t3.name, out.name])
        raise HTTPException(status_code=400, detail=f"russell.csv invalid: {msg}")

    job_id = str(uuid.uuid4())
    jobs[job_id] = {"status": "pending", "created": time.time(), "out": None, "files": [t1.name, t2.name, t3.name], "error": None}
    # submit to executor
    executor.submit(_run_job, job_id, t1.name, t2.name, t3.name, out.name, [t1.name, t2.name, t3.name, out.name])
    return {"job_id": job_id, "status": "pending"}


@app.get("/jobs/{job_id}")
def job_status(job_id: str):
    job = jobs.get(job_id)
    if not job:
        return JSONResponse({"error": "job not found"}, status_code=404)
    info = {k: v for k, v in job.items() if k != "files"}
    return info


def _cleanup_paths(paths: list, job_id: str = None): # type: ignore
    for p in paths or []:
        try:
            if os.path.exists(p):
                os.remove(p)
        except Exception:
            pass
    if job_id and job_id in jobs:
        try:
            del jobs[job_id]
        except Exception:
            pass


@app.get("/jobs/{job_id}/download")
def job_download(job_id: str, background_tasks: BackgroundTasks):
    job = jobs.get(job_id)
    if not job:
        return JSONResponse({"error": "job not found"}, status_code=404)
    if job.get("status") != "done":
        return JSONResponse({"error": "job not finished", "status": job.get("status")}, status_code=409)
    out = job.get("out")
    if not out or not os.path.exists(out):
        return JSONResponse({"error": "output not available"}, status_code=404)
    # schedule cleanup of files after response has been sent
    background_tasks.add_task(_cleanup_paths, job.get("files", []) , job_id)
    return FileResponse(out, filename="portfolio.csv", media_type="text/csv")