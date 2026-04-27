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
# --- Updated Input Model ---
class OrderItemRequest(BaseModel):
    item_id: str  # Changed from item_name to item_id for accuracy
    quantity: int

class PlaceOrderRequest(BaseModel):
    member_id: str
    store_id: str
    items: List[OrderItemRequest]
    discount_amount: Optional[float] = 0.0

@app.post("/orders", status_code=201)
def place_order(data: PlaceOrderRequest, bq: bigquery.Client = Depends(get_bq_client)):
    new_order_id = str(uuid.uuid4())
    order_timestamp = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    sales_tax_rate = 0.08

    # 1. Lookup item details by ID
    item_ids = [item.item_id for item in data.items]
    lookup_query = f"""
        SELECT id, name, size, price 
        FROM `{FULL_PATH}.menu_items` 
        WHERE id IN UNNEST(@ids)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("ids", "STRING", item_ids)]
    )
    
    try:
        lookup_results = bq.query(lookup_query, job_config=job_config).result()
        menu_map = {row.id: {"name": row.name, "price": float(row.price), "size": row.size} for row in lookup_results}
        
        items_subtotal = 0.0
        order_items_to_insert = []
        
        for requested_item in data.items:
            # Check if the ID exists in our lookup results
            details = menu_map.get(requested_item.item_id)
            
            if not details:
                # THIS IS THE FIX: Stop the code if the ID is missing
                raise HTTPException(
                    status_code=404, 
                    detail=f"Item ID {requested_item.item_id} was not found in the menu table. Check your FULL_PATH or Project ID."
                )
            
            # If found, do the math
            price_val = details['price']
            line_total = price_val * requested_item.quantity
            items_subtotal += line_total
            
            order_items_to_insert.append({
                "menu_item_id": requested_item.item_id,
                "item_name": details['name'],
                "size": details['size'],
                "quantity": requested_item.quantity,
                "price": price_val
            })

        # 2. Percentage Discount Logic
        # Input 10.0 becomes 10%
        input_percent = float(data.discount_amount) if data.discount_amount else 0.0
        discount_dollars = round(items_subtotal * (input_percent / 100), 2)
        
        # Calculate Final Totals
        order_subtotal = round(max(0, items_subtotal - discount_dollars), 2)
        sales_tax = round(order_subtotal * sales_tax_rate, 2)
        order_total = round(order_subtotal + sales_tax, 2)

        # 3. Update ORDERS table
        order_insert_query = f"""
            INSERT INTO `{FULL_PATH}.orders` 
            (order_id, member_id, store_id, order_date, items_subtotal, order_discount, order_subtotal, sales_tax, order_total)
            VALUES (
                @oid, @mid, @sid, @odate, 
                CAST(@i_sub AS NUMERIC), CAST(@disc AS NUMERIC), 
                CAST(@o_sub AS NUMERIC), CAST(@tax AS NUMERIC), CAST(@total AS NUMERIC)
            )
        """
        order_params = [
            bigquery.ScalarQueryParameter("oid", "STRING", new_order_id),
            bigquery.ScalarQueryParameter("mid", "STRING", data.member_id),
            bigquery.ScalarQueryParameter("sid", "STRING", data.store_id),
            bigquery.ScalarQueryParameter("odate", "STRING", order_timestamp),
            bigquery.ScalarQueryParameter("i_sub", "FLOAT", items_subtotal),
            bigquery.ScalarQueryParameter("disc", "FLOAT", discount_dollars), # Fixed variable name
            bigquery.ScalarQueryParameter("o_sub", "FLOAT", order_subtotal),
            bigquery.ScalarQueryParameter("tax", "FLOAT", sales_tax),
            bigquery.ScalarQueryParameter("total", "FLOAT", order_total),
        ]
        bq.query(order_insert_query, job_config=bigquery.QueryJobConfig(query_parameters=order_params)).result()

        # --- 4. Update ORDER_ITEMS table (Mapping the ID) ---
        # Added 'menu_item_id' to the column list below
        items_insert_query = f"""
            INSERT INTO `{FULL_PATH}.order_items` 
            (id, order_id, menu_item_id, item_name, size, quantity, price) 
            VALUES 
        """
        placeholders = []
        item_params = [bigquery.ScalarQueryParameter("oid", "STRING", new_order_id)]
        
        for i, item in enumerate(order_items_to_insert):
            suffix = f"_{i}"
            # Added @mid{suffix} to the placeholders
            placeholders.append(f"(@iid{suffix}, @oid, @mid{suffix}, @name{suffix}, @size{suffix}, @qty{suffix}, CAST(@price{suffix} AS NUMERIC))")
            
            item_params.extend([
                bigquery.ScalarQueryParameter(f"iid{suffix}", "STRING", str(uuid.uuid4())),
                bigquery.ScalarQueryParameter(f"mid{suffix}", "STRING", item['menu_item_id']), # Now saving the Menu Item ID!
                bigquery.ScalarQueryParameter(f"name{suffix}", "STRING", item['item_name']),
                bigquery.ScalarQueryParameter(f"size{suffix}", "STRING", item['size']),
                bigquery.ScalarQueryParameter(f"qty{suffix}", "INTEGER", item['quantity']),
                bigquery.ScalarQueryParameter(f"price{suffix}", "FLOAT", item['price'])
            ])
        
        items_insert_query += ", ".join(placeholders)
        bq.query(items_insert_query, job_config=bigquery.QueryJobConfig(query_parameters=item_params)).result()

        # 5. Success Response
        return {
            "status": "success",
            "order_id": new_order_id,
            "summary": {
                "items_subtotal": round(items_subtotal, 2),
                "discount_percentage": f"{input_percent}%",
                "discount_saved": round(discount_dollars, 2), # Fixed variable name
                "tax": round(sales_tax, 2),
                "total": round(order_total, 2)
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Order Failed: {str(e)}")

# create new account/user
class RegistrationRequest(BaseModel):
    first_name: str
    last_name: str
    email: str
    phone_number: str

@app.post("/register", status_code=201)
def register_member(user: RegistrationRequest, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Registers a new Coffee Club member and returns their unique member_id.
    """
    new_member_id = str(uuid.uuid4())
    
    query = f"""
        INSERT INTO `{FULL_PATH}.members` (id, first_name, last_name, email, phone_number)
        VALUES (@mid, @fname, @lname, @email, @phone)
    """
    
    params = [
        bigquery.ScalarQueryParameter("mid", "STRING", new_member_id),
        bigquery.ScalarQueryParameter("fname", "STRING", user.first_name),
        bigquery.ScalarQueryParameter("lname", "STRING", user.last_name),
        bigquery.ScalarQueryParameter("email", "STRING", user.email),
        bigquery.ScalarQueryParameter("phone", "STRING", user.phone_number),
    ]
    
    try:
        bq.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
        return {
            "status": "success",
            "message": f"Welcome to the Coffee Club, {user.first_name}!",
            "member_id": new_member_id
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")

#delete account/user info and history
@app.delete("/members/{member_id}")
def delete_account(member_id: str, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Permanently scrubs all member data, including order history and line items.
    """
    try:
        # Step 1: Find all Order IDs belonging to this member
        # We need these to know which items to delete in the order_items table
        find_orders_query = f"SELECT order_id FROM `{FULL_PATH}.orders` WHERE member_id = @mid"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("mid", "STRING", member_id)]
        )
        order_rows = bq.query(find_orders_query, job_config=job_config).result()
        order_ids = [row.order_id for row in order_rows]

        # Step 2: Delete from order_items (The most granular data)
        if order_ids:
            delete_items_query = f"DELETE FROM `{FULL_PATH}.order_items` WHERE order_id IN UNNEST(@oids)"
            items_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ArrayQueryParameter("oids", "STRING", order_ids)]
            )
            bq.query(delete_items_query, job_config=items_config).result()

        # Step 3: Delete from orders (The transaction headers)
        delete_orders_query = f"DELETE FROM `{FULL_PATH}.orders` WHERE member_id = @mid"
        bq.query(delete_orders_query, job_config=job_config).result()

        # Step 4: Delete from members (The profile itself)
        delete_member_query = f"DELETE FROM `{FULL_PATH}.members` WHERE id = @mid"
        member_job = bq.query(delete_member_query, job_config=job_config)
        member_job.result()

        # Verification
        if member_job.num_dml_affected_rows == 0:
            raise HTTPException(status_code=404, detail="Member not found.")

        return {
            "status": "success",
            "message": f"Member {member_id} and all associated records have been wiped.",
            "records_scrubbed": {
                "profile": 1,
                "orders_deleted": len(order_ids)
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Deep delete failed: {str(e)}")

#redeem points
@app.post("/orders/redeem", status_code=201)
def place_order_with_points(data: PlaceOrderRequest, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Allows a user to pay for their entire order using points.
    Cost is calculated by rounding up each item's price.
    """
    new_order_id = str(uuid.uuid4())
    order_timestamp = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

    # 1. Lookup item prices
    item_ids = [item.item_id for item in data.items]
    lookup_query = f"SELECT id, name, size, price FROM `{FULL_PATH}.menu_items` WHERE id IN UNNEST(@ids)"
    
    try:
        lookup_results = bq.query(lookup_query, job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("ids", "STRING", item_ids)])).result()
        menu_map = {row.id: {"name": row.name, "price": float(row.price), "size": row.size} for row in lookup_results}

        total_points_cost = 0
        order_items_to_insert = []

        for requested_item in data.items:
            details = menu_map.get(requested_item.item_id)
            if not details:
                raise HTTPException(status_code=404, detail=f"Item ID {requested_item.item_id} not found.")
            
            # THE LOGIC: Round UP the price to get point cost per item
            item_point_cost = math.ceil(details['price'])
            total_points_cost += (item_point_cost * requested_item.quantity)
            
            order_items_to_insert.append({
                "menu_item_id": requested_item.item_id,
                "item_name": details['name'],
                "size": details['size'],
                "quantity": requested_item.quantity,
                "price": 0.0  # It's free in terms of cash
            })

        # 2. Check Member Balance
        member_query = f"SELECT points_balance FROM `{FULL_PATH}.members` WHERE id = @mid"
        member_res = bq.query(member_query, job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("mid", "STRING", data.member_id)])).result()
        member = next(member_res, None)
        
        if not member or (member.points_balance or 0) < total_points_cost:
            raise HTTPException(status_code=400, detail=f"Insufficient points. Need {total_points_cost}.")

        # 3. Update Member Balance (Deduct Points)
        new_balance = int(member.points_balance) - total_points_cost
        update_bal_query = f"UPDATE `{FULL_PATH}.members` SET points_balance = @nb WHERE id = @mid"
        bq.query(update_bal_query, job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("nb", "INTEGER", new_balance),
            bigquery.ScalarQueryParameter("mid", "STRING", data.member_id)
        ])).result()

        # 4. Record the Order in 'orders' table
        # We record subtotals/totals as 0 because it was a points transaction
        order_insert_query = f"""
            INSERT INTO `{FULL_PATH}.orders` 
            (order_id, member_id, store_id, order_date, items_subtotal, order_discount, order_subtotal, sales_tax, order_total)
            VALUES (@oid, @mid, @sid, @odate, 0, 0, 0, 0, 0)
        """
        bq.query(order_insert_query, job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("oid", "STRING", new_order_id),
            bigquery.ScalarQueryParameter("mid", "STRING", data.member_id),
            bigquery.ScalarQueryParameter("sid", "STRING", data.store_id),
            bigquery.ScalarQueryParameter("odate", "STRING", order_timestamp)
        ])).result()

        # 5. Record items in 'order_items' table
        # (Same loop logic as your regular place_order endpoint)
        # ... [Insert order_items logic here] ...

        return {
            "status": "success",
            "message": "Order paid with points!",
            "points_spent": total_points_cost,
            "remaining_balance": new_balance,
            "order_id": new_order_id
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))