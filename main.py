from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from pydantic import BaseModel
from typing import Optional
from typing import List
import datetime
import uuid
import math

app = FastAPI(title="Uncle Joes API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
#URL
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
class UpdateMemberRequest(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    phone_number: Optional[str] = None
    home_store: Optional[str] = None

@app.get("/")
def read_root():
    return {"status": "healthy", "message": "Uncle Joe's API is online. Great Success! Did it work?"}

# --- PUBLIC: MENU ENDPOINT ---
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
# --- PUBLIC: MENU CATEGORIES ---
@app.get("/menu/categories")
def get_menu_categories(bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns a clean list of unique categories available in the menu.
    Useful for building navigation tabs or filters on the frontend.
    """
    query = f"""
        SELECT DISTINCT category 
        FROM `{FULL_PATH}.menu_items` 
        WHERE category IS NOT NULL 
        ORDER BY category ASC
    """
    
    try:
        query_job = bq.query(query)
        # We extract the string from the row object to return a simple list
        categories = [row.category for row in query_job]
        
        return {
            "status": "success",
            "count": len(categories),
            "categories": categories
        }

    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Error fetching categories: {str(e)}"
        )
@app.get("/menu/{item_id}")
def get_menu_item(item_id: str, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Retrieves the full details of a specific menu item using its unique ID.
    Used for the "Product Detail" view on the frontend.
    """
    # We select all columns so Rochelle has access to calories, price, category, etc.
    query = f"""
        SELECT * FROM `{FULL_PATH}.menu_items` 
        WHERE id = @iid
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("iid", "STRING", item_id)
        ]
    )
    
    try:
        query_job = bq.query(query, job_config=job_config)
        results = [dict(row) for row in query_job]
        
        # If no item is found with that ID, we tell the frontend explicitly
        if not results:
            raise HTTPException(
                status_code=404, 
                detail=f"Menu item with ID {item_id} not found."
            )
            
        return {
            "status": "success",
            "item": results[0]
        }

    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"BigQuery Error: {str(e)}"
        )
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
        SELECT first_name, last_name, id, email, home_store, phone_number
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

# --- MEMBER: PROFILE & STATS (The Dashboard Engine) ---
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

# --- MEMBER: ORDER HISTORY ---
# --- MEMBER: ORDER HISTORY WITH NESTED ITEMS ---
@app.get("/members/{member_id}/orders")
def get_order_history(
    member_id: str, 
    limit: Optional[int] = None, 
    bq: bigquery.Client = Depends(get_bq_client)
):
    """
    Fetches full order history for a member.
    Calculates order_total dynamically from line items to ensure math accuracy.
    """
    # 1. We calculate 'order_total' on the fly by summing (price * quantity)
    # 2. We use ARRAY_AGG(STRUCT(...)) to nest all items into a single order row
    query = f"""
        SELECT 
            o.order_id, 
            o.order_date, 
            ROUND(SUM(i.price * i.quantity), 2) AS calculated_total, 
            l.city, 
            l.state,
            ARRAY_AGG(
                STRUCT(
                    i.item_name, 
                    i.size, 
                    i.quantity, 
                    i.price,
                    ROUND(i.price * i.quantity, 2) AS item_subtotal
                )
            ) AS items
        FROM `{FULL_PATH}.orders` AS o
        LEFT JOIN `{FULL_PATH}.locations` AS l ON o.store_id = l.id
        LEFT JOIN `{FULL_PATH}.order_items` AS i ON o.order_id = i.order_id
        WHERE o.member_id = @mid
        GROUP BY o.order_id, o.order_date, l.city, l.state
        ORDER BY o.order_date DESC
    """
    
    params = [bigquery.ScalarQueryParameter("mid", "STRING", member_id)]
    
    # Handle optional limit for the frontend
    if limit:
        query += " LIMIT @limit"
        params.append(bigquery.ScalarQueryParameter("limit", "INTEGER", limit))
    else:
        query += " LIMIT 1000"
        params.append(bigquery.ScalarQueryParameter("limit", "INTEGER", 1000))

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    
    try:
        query_job = bq.query(query, job_config=job_config)
        results = [dict(row) for row in query_job]
        
        for row in results:
            # Clean up Date Formatting
            if row.get('order_date'):
                # Format to: YYYY-MM-DD HH:MM
                row['order_date'] = row['order_date'].strftime('%Y-%m-%d %H:%M')
            
            # Safety Check: If an order exists but has no items, BigQuery 
            # returns a list with one 'None' entry. We clean that up here.
            if row['items'] == [None] or (len(row['items']) > 0 and row['items'][0].get('item_name') is None):
                row['items'] = []

        return {
            "member_id": member_id,
            "order_count": len(results),
            "orders": results
        }

    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"BigQuery Order History Error: {str(e)}"
        )
# --- MEMBER: UPDATE PROFILE ---
@app.patch("/members/{member_id}")
def update_member_profile(member_id: str, data: UpdateMemberRequest, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Updates specific fields for a member. Only provided fields will be changed.
    """
    # 1. Build the SET clause dynamically based on what was provided
    updates = []
    params = [bigquery.ScalarQueryParameter("mid", "STRING", member_id)]
    
    if data.first_name:
        updates.append("first_name = @fname")
        params.append(bigquery.ScalarQueryParameter("fname", "STRING", data.first_name))
    if data.last_name:
        updates.append("last_name = @lname")
        params.append(bigquery.ScalarQueryParameter("lname", "STRING", data.last_name))
    if data.phone_number:
        updates.append("phone_number = @phone")
        params.append(bigquery.ScalarQueryParameter("phone", "STRING", data.phone_number))
    if data.home_store:
        updates.append("home_store = @hstore")
        params.append(bigquery.ScalarQueryParameter("hstore", "STRING", data.home_store))

    if not updates:
        raise HTTPException(status_code=400, detail="No update data provided")

    # 2. Execute the UPDATE in BigQuery
    query = f"""
        UPDATE `{FULL_PATH}.members`
        SET {', '.join(updates)}
        WHERE id = @mid
    """
    
    try:
        query_job = bq.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params))
        query_job.result() # Wait for the update to finish
        
        return {"status": "success", "message": "Profile updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- MEMBER: ORDER DETAILS ---
@app.get("/orders/{order_id}")
def get_order_details(order_id: str, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"SELECT item_name, size, quantity, price FROM `{FULL_PATH}.order_items` WHERE order_id = @oid"
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("oid", "STRING", order_id)])
    query_job = bq.query(query, job_config=job_config)
    results = [dict(row) for row in query_job]
    if not results: raise HTTPException(status_code=404, detail="Order items not found")
    return {"order_id": order_id, "items": results}

# --- MEMBER: LOYALTY POINTS ---
@app.get("/members/{member_id}/points")
def get_member_points(member_id: str, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Calculates total loyalty points for a member.
    Formula: SUM(FLOOR(order_total)) - 1 point per whole dollar spent.
    """
    # We use FLOOR to drop the cents before summing, as per your teammate's logic
    query = f"""
        SELECT 
            IFNULL(SUM(FLOOR(order_total)), 0) as total_points,
            COUNT(order_id) as total_orders
        FROM `{FULL_PATH}.orders`
        WHERE member_id = @mid
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("mid", "STRING", member_id)
        ]
    )
    
    try:
        query_job = bq.query(query, job_config=job_config)
        results = [dict(row) for row in query_job]
        
        # Grab the data (or default to 0 if nothing is found)
        data = results[0] if results else {"total_points": 0, "total_orders": 0}
        
        return {
            "member_id": member_id,
            "points_summary": {
                "current_balance": int(data['total_points']),
                "lifetime_orders": data['total_orders'],
                "program_name": "Uncle Joe's Coffee Club"
            }
        }

    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Points Calculation Error: {str(e)}"
        )

# submitting orders
# --- Input Models ---
class OrderItemRequest(BaseModel):
    item_name: str
    quantity: int

class PlaceOrderRequest(BaseModel):
    member_id: str
    store_id: str
    items: List[OrderItemRequest]
    discount_amount: Optional[float] = 0.0

# --- The Place Order Endpoint ---
@app.post("/orders", status_code=201)
def place_order(data: PlaceOrderRequest, bq: bigquery.Client = Depends(get_bq_client)):
    # 1. Generate unique identifiers and metadata
    new_order_id = str(uuid.uuid4())
    order_timestamp = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    sales_tax_rate = 0.08

    # 2. Lookup item details from the menu_items table
    # We do this so the user can't fake prices.
    item_names = [item.item_name for item in data.items]
    lookup_query = f"""
        SELECT name, size, price 
        FROM `{FULL_PATH}.menu_items` 
        WHERE name IN UNNEST(@names)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("names", "STRING", item_names)]
    )
    
    try:
        lookup_results = bq.query(lookup_query, job_config=job_config).result()
        menu_map = {row.name: {"price": row.price, "size": row.size} for row in lookup_results}
        
        # 3. Background Calculations
        items_subtotal = 0.0
        order_items_to_insert = []
        
        for requested_item in data.items:
            details = menu_map.get(requested_item.item_name)
            if not details:
                raise HTTPException(status_code=404, detail=f"Item '{requested_item.item_name}' not found in menu.")
            
            line_total = details['price'] * requested_item.quantity
            items_subtotal += line_total
            
            # Prepare data for the order_items table
            order_items_to_insert.append({
                "order_id": new_order_id,
                "item_name": requested_item.item_name,
                "size": details['size'],
                "quantity": requested_item.quantity,
                "price": details['price']
            })

        # Apply Discount & Tax
        discount = data.discount_amount if data.discount_amount else 0.0
        order_subtotal = round(max(0, items_subtotal - discount), 2)
        sales_tax = round(order_subtotal * sales_tax_rate, 2)
        order_total = round(order_subtotal + sales_tax, 2)

        # 4. Update the ORDERS table
        order_insert_query = f"""
            INSERT INTO `{FULL_PATH}.orders` 
            (order_id, member_id, store_id, order_date, items_subtotal, order_discount, order_subtotal, sales_tax, order_total)
            VALUES (@oid, @mid, @sid, @odate, @i_sub, @disc, @o_sub, @tax, @total)
        """
        order_params = [
            bigquery.ScalarQueryParameter("oid", "STRING", new_order_id),
            bigquery.ScalarQueryParameter("mid", "STRING", data.member_id),
            bigquery.ScalarQueryParameter("sid", "STRING", data.store_id),
            bigquery.ScalarQueryParameter("odate", "STRING", order_timestamp),
            bigquery.ScalarQueryParameter("i_sub", "FLOAT", items_subtotal),
            bigquery.ScalarQueryParameter("disc", "FLOAT", discount),
            bigquery.ScalarQueryParameter("o_sub", "FLOAT", order_subtotal),
            bigquery.ScalarQueryParameter("tax", "FLOAT", sales_tax),
            bigquery.ScalarQueryParameter("total", "FLOAT", order_total),
        ]
        bq.query(order_insert_query, job_config=bigquery.QueryJobConfig(query_parameters=order_params)).result()

        # 5. Update the ORDER_ITEMS table
        # Using a multi-row values string for efficiency
        items_insert_query = f"INSERT INTO `{FULL_PATH}.order_items` (order_id, item_name, size, quantity, price) VALUES "
        placeholders = []
        item_params = [bigquery.ScalarQueryParameter("oid", "STRING", new_order_id)]
        
        for i, item in enumerate(order_items_to_insert):
            suffix = f"_{i}"
            placeholders.append(f"(@oid, @name{suffix}, @size{suffix}, @qty{suffix}, @price{suffix})")
            item_params.extend([
                bigquery.ScalarQueryParameter(f"name{suffix}", "STRING", item['item_name']),
                bigquery.ScalarQueryParameter(f"size{suffix}", "STRING", item['size']),
                bigquery.ScalarQueryParameter(f"qty{suffix}", "INTEGER", item['quantity']),
                bigquery.ScalarQueryParameter(f"price{suffix}", "FLOAT", item['price'])
            ])
        
        items_insert_query += ", ".join(placeholders)
        bq.query(items_insert_query, job_config=bigquery.QueryJobConfig(query_parameters=item_params)).result()

        return {
            "status": "success",
            "order_id": new_order_id,
            "summary": {
                "subtotal": items_subtotal,
                "discount": discount,
                "tax": sales_tax,
                "total": order_total
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Order Failed: {str(e)}")