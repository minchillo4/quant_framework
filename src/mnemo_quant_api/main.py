from fastapi import FastAPI

from mnemo_quant_api.health import router as health_router  # Import shared router

# from .routes.oi_queries import router as oi_router  # Future additions

app = FastAPI(title="Mnemo Quant API", version="0.1.0")
app.include_router(health_router, prefix="")  # /health directly
# app.include_router(oi_router, prefix="/api")  # Example future route


@app.get("/")
async def root():
    return {"message": "Mnemo Quant API is running"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
