import os
from fastapi import FastAPI, BackgroundTasks, Query
from fastapi.responses import JSONResponse
import uvicorn

# Imports your exact existing scanner without modifying it
import swing_scanner

app = FastAPI()
is_scanning = False

def run_scan_job(mode: str):
    global is_scanning
    is_scanning = True
    if mode and mode.upper() in {"MORNING", "BTST", "EOD"}:
        os.environ["SCAN_MODE"] = mode.upper()
    elif "SCAN_MODE" in os.environ:
        del os.environ["SCAN_MODE"]
    try:
        swing_scanner.main()
    finally:
        is_scanning = False

@app.get("/")
def home():
    return {"status": "TradeFlow Bot is online", "is_scanning": is_scanning}

@app.get("/trigger-scan")
def trigger_scan(background_tasks: BackgroundTasks, mode: str = Query("auto")):
    global is_scanning
    if is_scanning:
        return JSONResponse(status_code=429, content={"message": "Scan already running in background"})
    background_tasks.add_task(run_scan_job, mode)
    return {"status": "scan_started", "mode": mode}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
