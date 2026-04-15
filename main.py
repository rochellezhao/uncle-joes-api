from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from pydantic import BaseModel
from typing import List

app = FastAPI(title="Uncle Joes API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA_PROJECT = "uncle-joes-coffee-company" 
DATASET = "uncle_joes"
FULL_PATH = f"{DATA_PROJECT}.{DATASET}"

def get_bq_client():
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()

@app.get("/")
def read_root():
    return {"status": "healthy", "message": "Uncle Joe's API is online"}

# --- MENU ENDPOINT ---
@app.get("/menu")
def get_menu(bq: bigquery.Client = Depends(get_bq_client)):
    query = f"SELECT name, price, category FROM `{FULL_PATH}.menu_items` LIMIT 20"
    try:
        query_job = bq.query(query)
        results = [dict(row) for row in query_job]
        return {"items": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"BigQuery Error: {str(e)}")
# --- LOCATIONS ENDPOINTS ---
@app.get("/locations")
def get_locations(bq: bigquery.Client = Depends(get_bq_client)):
    # Try selecting * first to see all available columns if you're unsure
    query = f"SELECT * FROM `{FULL_PATH}.locations` LIMIT 10"
    try:
        query_job = bq.query(query)
        results = [dict(row) for row in query_job]
        return {"total": len(results), "locations": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"BigQuery Error: {str(e)}")
