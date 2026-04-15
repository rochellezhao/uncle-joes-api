from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from pydantic import BaseModel
from typing import List

app = FastAPI(title="Uncle Joes API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For Uncle Joes, you'll put your Cloud Run frontend URL here
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PROJECT_ID = "uncle-joes-api"
DATASET = "uncle_joes"


def get_bq_client():
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()

@app.get("/")
def read_root():
    return {"status": "healthy"}

#here
