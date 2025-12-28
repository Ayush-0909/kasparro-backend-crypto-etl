from fastapi import FastAPI

app = FastAPI(title="Kasparro Crypto ETL API")

@app.get("/")
def root():
    return {"message": "Kasparro Backend is running"}

@app.get("/health")
def health():
    return {
        "status": "ok",
        "db": "not checked at startup"
    }
