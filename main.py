# ============================================================
# main.py
# StreamPulse - FastAPI Web Application
# Simulates users watching content on StreamPulse platform
# Author: Khushboo Patel
# ============================================================

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from connection import send_to_event_hub
from data import generate_stream_event
import logging

# ── Logging Setup ─────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── FastAPI App Setup ─────────────────────────────────────────
app = FastAPI(
    title="StreamPulse",
    description="Real-time video streaming analytics platform",
    version="1.0.0"
)

# ── Templates Setup ───────────────────────────────────────────
templates = Jinja2Templates(directory="templates")


# ── Home Page ─────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def streampulse_home(request: Request):
    """
    StreamPulse home page.
    Shows available content for users to watch.
    """
    content_list = [
        {"title": "Sacred Games",   "genre": "Thriller", "type": "Series"},
        {"title": "Mirzapur",       "genre": "Crime",    "type": "Series"},
        {"title": "Panchayat",      "genre": "Comedy",   "type": "Series"},
        {"title": "RRR",            "genre": "Action",   "type": "Movie"},
        {"title": "KGF Chapter 2",  "genre": "Action",   "type": "Movie"},
        {"title": "Scam 1992",      "genre": "Drama",    "type": "Series"},
        {"title": "The Family Man", "genre": "Action",   "type": "Series"},
        {"title": "Delhi Crime",    "genre": "Crime",    "type": "Series"},
        {"title": "Rocket Boys",    "genre": "Drama",    "type": "Series"},
        {"title": "Drishyam 2",     "genre": "Thriller", "type": "Movie"},
    ]

    return templates.TemplateResponse(
        "home.html",
        {
            "request": request,
            "content_list": content_list,
            "platform_name": "StreamPulse"
        }
    )


# ── Watch Endpoint ────────────────────────────────────────────
@app.get("/watch", response_class=HTMLResponse)
async def watch_content(request: Request):
    """
    Watch endpoint - simulates user pressing play.
    Generates a watch event and sends to Event Hubs.
    This is the core of the streaming pipeline.
    """
    # Step 1 - Generate watch event
    event = generate_stream_event()
    logger.info(f"Generated event for: {event['content_title']}")

    # Step 2 - Send to Azure Event Hubs
    result = send_to_event_hub(event)
    logger.info(f"Event Hub result: {result['status']}")

    # Step 3 - Show confirmation to user
    return templates.TemplateResponse(
        "watch.html",
        {
            "request":       request,
            "event":         event,
            "result":        result,
            "platform_name": "StreamPulse"
        }
    )


# ── Health Check Endpoint ─────────────────────────────────────
@app.get("/health")
async def health_check():
    """
    Simple health check endpoint.
    Used by Azure to verify app is running.
    """
    return {
        "status": "healthy",
        "platform": "StreamPulse",
        "version": "1.0.0"
    }


# ── Run App ───────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )