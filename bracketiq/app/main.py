"""
BracketIQ FastAPI entrypoint.
Phase 1: Foundation — data pipeline and prediction API.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import teams, matchups, predictions

app = FastAPI(
    title="BracketIQ",
    description="March Madness bracket prediction platform (KenPom + Odds API)",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(teams.router)
app.include_router(matchups.router)
app.include_router(predictions.router)


@app.get("/")
def root():
    return {"app": "BracketIQ", "docs": "/docs", "api": "/api"}


@app.get("/health")
def health():
    return {"status": "ok"}
