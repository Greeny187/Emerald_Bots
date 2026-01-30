# server.py - Support Bot API Server
"""
Standalone FastAPI server for Emerald Support Bot.
Can be run via: uvicorn bots.support.server:app --port 8001
Or integrated into the main bot.py.
Comprehensive logging enabled for function verification.
"""

import logging
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

logger = logging.getLogger("bot.support.server")
logger.info("[SERVER_INIT] Support Bot Server module loaded")

# Create FastAPI app
app = FastAPI(
    title="Emerald Support Bot API",
    description="Support ticket system with multi-tenancy",
    version="1.0.0"
)
logger.debug("[SERVER_INIT] FastAPI application instance created")

# Add CORS middleware
logger.debug("[SERVER_INIT] Configuring CORS middleware")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
logger.debug("[SERVER_INIT] ✅ CORS middleware configured")

# Import and include router
try:
    from .support_api import router as support_router
    logger.debug("[SERVER_INIT] Support API router imported successfully")
    app.include_router(support_router)
    logger.info("✅ [SERVER_INIT] Support Bot API router included")
except ImportError as e:
    logger.error(f"❌ [SERVER_INIT] Failed to import support_api router: {e}")

@app.get("/health")
async def health():
    """Health check endpoint"""
    logger.debug("[HEALTH] Health check endpoint called")
    return {"status": "ok", "service": "emerald-support"}

@app.on_event("startup")
async def startup():
    """On server startup"""
    logger.info("[STARTUP] Support Bot API server starting up...")
    logger.info("[STARTUP] ✅ Server ready to accept requests")

@app.on_event("shutdown")
async def shutdown():
    """On server shutdown"""
    logger.info("[SHUTDOWN] Support Bot API server shutting down...")

if __name__ == "__main__":
    logger.info("[MAIN] Starting uvicorn server on 0.0.0.0:8001")
    uvicorn.run(app, host="0.0.0.0", port=8001)
