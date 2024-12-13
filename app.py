import streamlit as st
import pandas as pd
import os
import logging
import requests
from datetime import datetime
from dotenv import load_dotenv
from typing import Dict, List, Optional, Any, Tuple
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter
import re
from functools import lru_cache

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('orders_analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration
store_name = os.getenv('SHOPIFY_STORE_NAME')
access_token = os.getenv('SHOPIFY_ACCESS_TOKEN')
storefront_token = os.getenv('SHOPIFY_STOREFRONT_TOKEN')
flores_location_id = os.getenv('LOCATION_ID_FLORES')
warehouse_location_id = os.getenv('LOCATION_ID_WAREHOUSE')

# Page config
st.set_page_config(page_title="Order Analysis", layout="wide")

def check_environment_variables():
    if not store_name or not access_token:
        st.error("Missing SHOPIFY_STORE_NAME or SHOPIFY_ACCESS_TOKEN environment variables.")
        st.stop()
    
    if not flores_location_id or not warehouse_location_id:
        st.error("Missing location environment variables.")
        st.stop()
    
    if not storefront_token:
        st.warning("SHOPIFY_STOREFRONT_TOKEN not found. International pricing might be limited.")

def execute_graphql_query(query: str, variables: Optional[Dict] = None):
    url = f"https://{store_name}.myshopify.com/admin/api/2023-01/graphql.json"
    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
    }
    
    try:
        response = requests.post(
            url,
            headers=headers,
            json={"query": query, "variables": variables}
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error executing GraphQL query: {str(e)}")
        return None

def fetch_unfulfilled_orders():
    orders_query = """
    query($cursor: String) {
        orders(first: 50, after: $cursor, query: "fulfillment_status:unfulfilled AND status:open", sortKey: CREATED_AT, reverse: true) {
            edges {
                node {
                    id
                    name
                    createdAt
                    customer {
                        firstName
                        lastName
                        email
                    }
                    shippingAddress {
                        address1
                        city
                        country
                        zip
                    }
                    lineItems(first: 50) {
                        edges {
                            node {
                                quantity
                                variant {
                                    id
                                    inventoryItem {
                                        id
                                    }
                                }
                            }
                        }
                    }
                }
            }
            pageInfo {
                hasNextPage
                endCursor
            }
        }
    }
    """
    
    orders = []
    has_next_page = True
    cursor = None
    
    with st.spinner('Fetching orders...'):
        progress_bar = st.progress(0)
        total_orders = 0
        
        while has_next_page:
            result = execute_graphql_query(orders_query, {"cursor": cursor})
            if not result:
                break
                
            data = result.get("data", {}).get("orders", {})
            current_orders = [edge["node"] for edge in data.get("edges", [])]
            orders.extend(current_orders)
            
            total_orders += len(current_orders)
            progress = min(float(len(orders)) / 100, 1.0)  # Assuming max 100 orders
            progress_bar.progress(progress)
            
            page_info = data.get("pageInfo", {})
            has_next_page = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")
    
    return orders

def determine_zone(country: str) -> str:
    europe_countries = {"Portugal", "Spain", "France", "Germany", "Italy", "UK", "Ireland"}
    usa_zone = {"United States", "USA"}
    
    if country in europe_countries:
        return "Europe"
    elif country in usa_zone:
        return "USA Zone"
    else:
        return "Rest World Zone"

def get_azul_cost(total_weight_kg: float, zone: str) -> float:
    # Simplified cost calculation logic
    base_costs = {
        "Europe": 4.47,
        "USA Zone": 5.23,
        "Rest World Zone": 4.89
    }
    
    weight_multipliers = {
        0.02: 1,
        0.05: 1,
        0.1: 1,
        0.25: 1.2,
        0.5: 1.5,
        1.0: 2,
        2.0: 3
    }
    
    base_cost = base_costs.get(zone, base_costs["Rest World Zone"])
    
    for weight_threshold, multiplier in sorted(weight_multipliers.items()):
        if total_weight_kg <= weight_threshold:
            return base_cost * multiplier
    
    return base_cost * 3  # Maximum multiplier for weights > 2.0 kg

def process_orders(orders: List[Dict]) -> List[Dict]:
    processed_data = []
    
    for order in orders:
        customer = order.get("customer", {})
        shipping = order.get("shippingAddress", {})
        
        for item in order.get("lineItems", {}).get("edges", []):
            node = item.get("node", {})
            variant = node.get("variant", {})
            
            processed_item = {
                "Order": order.get("name"),
                "Date": datetime.fromisoformat(order.get("createdAt")).strftime("%Y-%m-%d %H:%M:%S"),
                "Customer": f"{customer.get('firstName', '')} {customer.get('lastName', '')}",
                "Email": customer.get("email", ""),
                "Country": shipping.get("country", ""),
                "City": shipping.get("city", ""),
                "Postal Code": shipping.get("zip", ""),
                "Quantity": node.get("quantity", 0),
                "Variant ID": variant.get("id", ""),
                "Zone": determine_zone(shipping.get("country", "")),
            }
            
            processed_data.append(processed_item)
    
    return processed_data

def main():
    st.title("Order Analysis Dashboard")
    
    check_environment_variables()
    
    if st.button("Start Analysis"):
        orders = fetch_unfulfilled_orders()
        if orders:
            processed_data = process_orders(orders)
            
            # Convert to DataFrame
            df = pd.DataFrame(processed_data)
            
            # Display summary statistics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Orders", len(df["Order"].unique()))
            with col2:
                st.metric("Total Items", df["Quantity"].sum())
            with col3:
                st.metric("Countries", len(df["Country"].unique()))
            
            # Display data tables
            st.subheader("Orders by Country")
            country_summary = df.groupby("Country").agg({
                "Order": "count",
                "Quantity": "sum"
            }).reset_index()
            st.dataframe(country_summary)
            
            st.subheader("All Orders")
            st.dataframe(df)
            
            # Download button
            csv = df.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name="order_analysis.csv",
                mime="text/csv"
            )

if __name__ == "__main__":
    main()
