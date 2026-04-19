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

class LoginRequest(BaseModel):
    email: str
    password: str

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
    return {"status": "healthy", "message": "Uncle Joe's API is online. Great Success! Did it work?"}

# --- MENU ENDPOINT ---
@app.get("/menu")
def get_menu(category: str = None, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns menu items with names, prices, sizes, and calories.
    Optional filter: /menu?category=Espresso
    """
    # Selecting columns based on your image_74cb6a.png preview
    query = f"SELECT name, category, size, calories, price FROM `{FULL_PATH}.menu_items`"
    
    job_config = None
    if category:
        # Using @cat to safely handle the category string
        query += " WHERE category = @cat"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("cat", "STRING", category)
            ]
        )
    
    # Ordering by name and size to keep the list organized
    query += " ORDER BY name ASC, price ASC LIMIT 50"
    
    try:
        query_job = bq.query(query, job_config=job_config)
        results = [dict(row) for row in query_job]
        
        return {
            "category_filter": category if category else "all",
            "count": len(results),
            "items": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"BigQuery Error: {str(e)}")

# --- LOCATIONS ENDPOINTS ---
@app.get("/locations")
def get_locations(bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns a simple list of all physical store addresses.
    """
    # We only select the specific column we need to keep the response light
    query = f"""
        SELECT location_map_address 
        FROM `{FULL_PATH}.locations`
        WHERE location_map_address IS NOT NULL
        ORDER BY location_map_address ASC
    """
    try:
        query_job = bq.query(query)
        # We extract just the address string from each row
        addresses = [row["location_map_address"] for row in query_job]
        
        return {
            "total_locations": len(addresses),
            "addresses": addresses
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"BigQuery Error: {str(e)}"
        )
@app.get("/locations/{location_id}")
def get_location_detail(location_id: str, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Fetch every detail for a specific store using its UUID.
    """
    # We use @lid as a placeholder to keep the query safe and clean
    query = f"""
        SELECT * FROM `{FULL_PATH}.locations` 
        WHERE id = @lid
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("lid", "STRING", location_id)
        ]
    )
    
    try:
        query_job = bq.query(query, job_config=job_config)
        results = [dict(row) for row in query_job]
        
        # If the UUID doesn't exist, we return a clean 404 error
        if not results:
            raise HTTPException(
                status_code=404, 
                detail=f"Location with ID {location_id} not found."
            )
            
        # Return the first (and only) match
        return results[0]

    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"BigQuery Error: {str(e)}"
        )

# --- LOGIN ENDPOINTS ---
@app.post("/login")
def login_member(login_data: LoginRequest, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Pilot Login: Verifies email exists in members table and checks against 
    the shared pilot password: Coffee123!
    """
    SHARED_PILOT_PASSWORD = "Coffee123!"
    
    # 1. Password Guard
    if login_data.password != SHARED_PILOT_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid password for pilot program")

    # 2. Database Lookup
    query = f"""
        SELECT first_name, last_name, email, home_store, phone_number
        FROM `{FULL_PATH}.members` 
        WHERE email = @email
        LIMIT 1
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("email", "STRING", login_data.email)
        ]
    )
    
    try:
        query_job = bq.query(query, job_config=job_config)
        results = [dict(row) for row in query_job]
        
        if not results:
            raise HTTPException(status_code=401, detail="Email not found in Coffee Club")
            
        user = results[0]
        
        return {
            "status": "success",
            "message": f"Login successful! Welcome to the pilot, {user['first_name']}.",
            "user_profile": user
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"BigQuery Error: {str(e)}")

# --- 4. MEMBER: PROFILE & STATS (The Dashboard Engine) ---
@app.get("/members/{member_id}")
def get_member_profile(member_id: str, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT 
            m.id, m.first_name, m.last_name, m.email, m.phone_number, m.home_store,
            COUNT(o.order_id) as total_orders,
            IFNULL(SUM(FLOOR(o.order_total)), 0) as total_points
        FROM `{FULL_PATH}.members` AS m
        LEFT JOIN `{FULL_PATH}.orders` AS o ON m.id = o.member_id
        WHERE m.id = @mid
        GROUP BY 1, 2, 3, 4, 5, 6
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("mid", "STRING", member_id)])
    query_job = bq.query(query, job_config=job_config)
    results = [dict(row) for row in query_job]
    if not results: raise HTTPException(status_code=404, detail="Member not found")
    return results[0]

# --- 5. MEMBER: ORDER HISTORY ---
@app.get("/members/{member_id}/orders")
def get_order_history(member_id: str, limit: Optional[int] = None, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns the member's order history. 
    If no limit is provided, it returns up to 1000 orders (essentially 'all' for this pilot).
    """
    query = f"""
        SELECT 
            o.order_id, o.order_date, o.order_total, 
            l.city, l.state
        FROM `{FULL_PATH}.orders` AS o
        JOIN `{FULL_PATH}.locations` AS l ON o.store_id = l.id
        WHERE o.member_id = @mid
        ORDER BY o.order_date DESC
    """
    final_limit = limit if limit else 1000
    query += " LIMIT @limit"
    
    params = [
        bigquery.ScalarQueryParameter("mid", "STRING", member_id),
        bigquery.ScalarQueryParameter("limit", "INTEGER", final_limit)
    ]
    
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    
    try:
        query_job = bq.query(query, job_config=job_config)
        results = [dict(row) for row in query_job]
        
        return {
            "member_id": member_id,
            "total_returned": len(results),
            "orders": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- 6. MEMBER: ORDER DETAILS ---
@app.get("/orders/{order_id}")
def get_order_details(order_id: str, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"SELECT item_name, size, quantity, price FROM `{FULL_PATH}.order_items` WHERE order_id = @oid"
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("oid", "STRING", order_id)])
    query_job = bq.query(query, job_config=job_config)
    results = [dict(row) for row in query_job]
    if not results: raise HTTPException(status_code=404, detail="Order items not found")
    return {"order_id": order_id, "items": results}