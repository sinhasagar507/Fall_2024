from pymongo import MongoClient
import json
import re 

# Updated database and collection names
DATABASE_NAME = 'ecommerce'
COLLECTION_NAME = 'orders'

def insert_mock_data(orders):
    """Inserts the generated mock data in JSON file into the MongoDB."""
     
    # Open and load the JSON file
    try:
        with open("mock_data.json", 'r') as file:
            orders_data = json.load(file)  # Load the JSON file into a Python list
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return
    
    # Insert the loaded data into the collection in bulk
    try:
        _ = orders.insert_many(orders_data)
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Error inserting data: {e}")
        return
    
    # Retrieve and print the object IDs 
    # print("Data inserted successfully.")
    results = list(orders.find({}, {"_id": 1}))
    obj_ids = [str(result["_id"]) for result in results]
    print(f"Inserted ObjectIds: {obj_ids}")
    
    
def find_order_totals(orders):
    """Finds the total number of orders and the number of orders per state, sorted by count in ascending order."""
    # MongoDB aggregation pipeline to calculate the totals 
    pipeline = [
        {
            "$group": {
                "_id": "$state",  # Group by the 'state' field
                "order_count": { "$sum": 1 }  # Count the number of orders per state
            }
        },
        {
            "$sort": {
                "order_count": 1  # Sort in ascending order by the number of orders
            }
        }, 
        {
            "$group": {
                "_id": None,  # No grouping (we want a total count across all states)
                "total_orders": { "$sum": "$order_count" },  # Sum the order counts from the previous step
                "states": {
                "$push": {  # Keep the order counts for each state in an array
                    "state": "$_id",
                    "order_count": "$order_count"
                    }
                }
            }
        }
    ]
    
    # Execute the aggregation pipeline 
    result = list(orders.aggregate(pipeline))

    # Print the result to console 
    print(f'''Total no. of orders: {result[0]["total_orders"]}''')
    print("Number of orders per state:")
  
    for res in result[0]["states"]:
        state = res["state"]
        order_cnt = res["order_count"]
        print(f"State: {state}, Count: {order_cnt}")
    
    
def find_product_frequencies(orders):
    """Finds the products and their frequencies sorted by frequency in descending order."""
    
    # MongoDB pipeline for processing the query  
    pipeline = [
        {
            "$group": {
                "_id": "$product_id",  # Group by product name
                "frequency": { "$sum": 1 }  # Count the occurrences of each product
            }
        },
        {
            "$sort": {
                "frequency": -1  # Sort by frequency in descending order
            }
        }
    ]
    
    # Execute the aggregation pipeline 
    results = list(orders.aggregate(pipeline))
    
    print("Product Frequencies:")
    for freq_tuple in results:
        print(f'''Product ID: {freq_tuple["_id"]}, Frequency: {freq_tuple["frequency"]}''')
        
    
def ca_highvalue_orders(orders):
    """Counts and finds the orders in California where the order amount exceeds $1,000."""
    
    # MongoDB aggregation pipeline 
    pipeline = [
     {
                "$match": {
                    "$and": [
                        {"state": "California"},
                        {"total_price": {"$gt": 1000}}
                    ]
                }
            },
            {
                "$project": {
                    "_id": 0
                }
            }
    ]
    
    # Execute the aggregation pipeline 
    results = list(orders.aggregate(pipeline))
    
    # Print the results 
    print(f'''Total high-value orders in California: {len(results)}''')
    print("High value order results:")
    
    for order in results:
        print(f"Order ID: {order.get('order_id')}, "
              f"Customer ID: {order.get('customer_id')}, "
              f"Product ID: {order.get('product_id')}, "
              f"Quantity: {order.get('quantity')}, "
              f"Unit Price: {order.get('unit_price')}, "
              f"Order Date: {order.get('order_date')}, "
              f"State: {order.get('state')}, "
              f"Total Price: {order.get('total_price')}, "
              f"Premium Customer: {order.get('premium_customer')}, "
              f"City: {order.get('city')}")
    

def top_states_highvalue(orders):
    """Finds the top ten states with the most orders where the order amount exceeds $500."""
     
    # MongoDB aggregation pipeline
    pipeline = [
            {
                "$match": {
                    "total_price": {"$gt": 500}
                }
            },
            {
                "$group": {
                    "_id": "$state",
                    "count": {"$sum": 1}
                }
            },
            {
                "$setWindowFields": {
                    "sortBy": {"count": -1},
                    "output": {
                        "rankCountforState": {
                            "$rank": {}
                        }
                    }
                }
            },
            {
                "$limit": 10
            },
            {
                "$project": {
                    "Rank": "$rankCountforState",
                    "State": "$_id",
                    "Order_count": "$count",
                    "_id": 0
                }
            }
        ]
 
    # Run the aggregation pipeline
    results = list(orders.aggregate(pipeline))
    
    print("Top 10 States with High-Value Orders (>$500):")
    for i, document in enumerate(results):
        print(f'''Rank {i+1}: State: {document["State"]}, Order Count: {document["Order_count"]}''')

def find_customer_premium(orders):
    """Counts and finds the customers who have placed premium orders (order amount exceeds $2,000) in Texas."""
    
    # MongoDB aggregation pipeline 
    pipeline = [
    {
        "$match": {
            "$and": [
                {"state": "Texas"}, 
                {"total_price": {"$gt": 2000}}
            ]
        }
    }, 
        {
            "$project": {
                "_id": 0
            }
        }
    ]
    
    
    # Execute the aggregation pipeline 
    results = list(orders.aggregate(pipeline))
    
    # Print the results 
    print("Total premium customers in Texas: ", len(results))
    print("Order details:")
    for result in results:
        print(f'''Order ID: {result["order_id"]}, Customer ID: {result["customer_id"]}, Product ID: {result["product_id"]}, Quantity: {result["quantity"]}, Unit Price: {result["unit_price"]}, Order Date: {result["order_date"]}, State: {result["state"]}, Total Price: {result["total_price"]}, Premium Customer: {result["premium_customer"]}, City: {result["city"]}''')

    
def find_orders_by_date(order_date, orders):
    """Counts and finds the orders placed in New York City on a specific date."""
    
    pipeline = [  # The pipeline is thoroughly checked for and there are no duplicate order IDs in the document 
        {
            "$match": {
                "city": "New York City", 
                "order_date": order_date, 
            }
        }
    ]  
    
    # Execute the aggregation pipeline 
    results = list(orders.aggregate(pipeline))
    
    # Print the results 
    print(f"Total orders placed in New York City on {order_date}: {len(results)}")
    print("Order details:")
    for result in results: 
        print(f'''Order ID: {result["order_id"]}, Customer ID: {result["customer_id"]}, Product ID: {result["product_id"]}, Quantity: {result["quantity"]}, Unit Price: {result["unit_price"]}, Order Date: {result["order_date"]}, State: {result["state"]}, Total Price: {result["total_price"]}, Premium Customer: {result["premium_customer"]}, City: {result["city"]}''')
    

if __name__ == '__main__':
    # Connect to the ecommerce database and perform operations
    client = MongoClient('localhost', 27017)
    print(client)
    db = client[DATABASE_NAME] # Create and connect to the database
    orders = db[COLLECTION_NAME] # Create and connect to the "orders" collection 
    
    # Just delete everything from the current database 
    db.delete()
    insert_mock_data(orders)
    find_order_totals(orders)
    find_product_frequencies(orders)
    ca_highvalue_orders(orders)
    top_states_highvalue(orders)
    find_customer_premium(orders)
    
    # Example date in the format 'MM/DD/YYYY'
    specific_date = '10/21/2021' #You may change it to the date you want.
    # Call the function to find orders by date in NYC
    find_orders_by_date(specific_date, orders)
   
