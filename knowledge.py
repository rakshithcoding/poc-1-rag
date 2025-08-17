# backend/knowledge.py

schema_knowledge = [
    """
    Collection Name: customers
    Description: Stores individual customer records. Each document represents a unique customer.
    The document key (META().id) for this collection is the customer's unique ID, like 'cust::001'.
    Fields:
    - name (string): The full name of the customer.
    - city (string): The city where the customer resides, e.g., 'Bengaluru', 'Mumbai'.
    - loyalty_level (string): The customer's tier in our loyalty program, such as 'Gold', 'Silver', or 'Bronze'.
    """,
    """
    Collection Name: sales
    Description: Stores individual sales transactions. Each document is a single sale event.
    Fields:
    - product_name (string): The name of the product sold.
    - sale_amount (number): The total monetary value of the sale in INR.
    - sale_date (string): The date of the sale in 'YYYY-MM-DD' format.
    - customer_id (string): This is the foreign key that links to a customer.
    JOIN RULE: This collection can be joined with the `customers` collection using the condition: sales.customer_id = META(customers).id
    """
]
