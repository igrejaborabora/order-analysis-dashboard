import os
import logging
import requests
import pandas as pd
import time
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential
import streamlit as st
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter
from dotenv import load_dotenv
import re
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor
import traceback

# Load environment variables
load_dotenv()

store_name = os.getenv('SHOPIFY_STORE_NAME')
access_token = os.getenv('SHOPIFY_ACCESS_TOKEN')
storefront_token = os.getenv('SHOPIFY_STOREFRONT_TOKEN')

flores_location_id = os.getenv('LOCATION_ID_FLORES')
warehouse_location_id = os.getenv('LOCATION_ID_WAREHOUSE')

# Validate required environment variables
if not store_name or not access_token:
    raise ValueError("SHOPIFY_STORE_NAME and SHOPIFY_ACCESS_TOKEN must be set")

location_ids = {
    "Pr. das Flores, 54": flores_location_id,
    "Warehouse": warehouse_location_id
}

base_url = f"https://{store_name}.myshopify.com/admin/api/2023-07/graphql.json"
rest_base_url = f"https://{store_name}.myshopify.com/admin/api/2024-01"
storefront_base_url = f"https://{store_name}.myshopify.com/api/2024-01/storefront/graphql.json"

inventory_cache: Dict[str, Dict[str, int]] = {}
cost_cache: Dict[str, float] = {}

@lru_cache(maxsize=1000)
def fetch_cost_for_inventory_item(inventory_item_id: str) -> float:
    if inventory_item_id in cost_cache:
        return cost_cache[inventory_item_id]
        
    inventory_item_id_numeric = re.search(r'\d+$', inventory_item_id).group()
    cost_url = f"{rest_base_url}/inventory_items/{inventory_item_id_numeric}.json"
    
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": access_token
    }
    
    while True:
        response = requests.get(cost_url, headers=headers)
        
        if response.status_code == 429:
            logging.warning("Rate limit exceeded. Retrying in 1 second...")
            time.sleep(1)
            continue
        
        response.raise_for_status()
        
        data = response.json()
        cost = float(data.get("inventory_item", {}).get("cost", 0.0))
        cost_cache[inventory_item_id] = cost
        return cost

@lru_cache(maxsize=1000)
def fetch_inventory_levels(inventory_item_id: str) -> Dict[str, int]:
    inventory_query = """
    query($inventoryItemId: ID!) {
      inventoryItem(id: $inventoryItemId) {
        inventoryLevels(first: 10) {
          edges {
            node {
              available
              location {
                id
                name
              }
            }
          }
        }
      }
    }
    """

    if inventory_item_id in inventory_cache:
        return inventory_cache[inventory_item_id]

    response = execute_graphql_query(inventory_query, {"inventoryItemId": inventory_item_id})
    inventory_levels = response['data']['inventoryItem']['inventoryLevels']['edges']
    
    stock_levels = {location: 0 for location in location_ids.values()}
    for level in inventory_levels:
        location_id = level['node']['location']['id']
        if location_id in stock_levels:
            stock_levels[location_id] = level['node']['available']
            
    inventory_cache[inventory_item_id] = stock_levels
    return stock_levels

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def execute_graphql_query(query: str, variables: Optional[Dict] = None) -> Dict[str, Any]:
    try:
        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": access_token
        }
        
        response = requests.post(base_url, json={"query": query, "variables": variables}, headers=headers)
        response.raise_for_status()
        
        result = response.json()
        
        if 'errors' in result:
            error_message = '; '.join([error.get('message', 'Unknown error') for error in result['errors']])
            logging.error(f"GraphQL query returned errors: {error_message}")
            raise Exception(f"GraphQL errors: {error_message}")
            
        if 'data' not in result:
            logging.error("GraphQL response missing 'data' field")
            logging.error(f"Full response: {result}")
            raise Exception("Invalid GraphQL response: missing 'data' field")
            
        return result
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {str(e)}")
        raise Exception(f"Failed to connect to Shopify API: {str(e)}")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        raise

orders_query = """
query($cursor: String) {
  orders(first: 50, after: $cursor, query: "fulfillment_status:unfulfilled AND status:open", sortKey: CREATED_AT, reverse: true) {
    edges {
      cursor
      node {
        id
        name
        createdAt
        customer {
          firstName
          lastName
        }
        shippingAddress {
          country
        }
        totalPriceSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        totalShippingPriceSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        displayFulfillmentStatus
        lineItems(first: 50) {
          edges {
            node {
              title
              quantity
              sku
              variant {
                id
                inventoryItem {
                  id
                }
                price
                compareAtPrice
                product {
                  vendor
                }
                weight
                weightUnit
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

count_query = """
query {
  orders(first: 250, query: "fulfillment_status:unfulfilled AND status:open") {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        id
      }
    }
  }
}
"""

def fetch_unfulfilled_orders_with_progress(progress_bar=None) -> List[Dict]:
    all_orders = []
    
    # Create a progress bar if not provided
    if progress_bar is None:
        progress_bar = st.progress(0, "Fetching orders...")
    
    # Get initial count
    result = execute_graphql_query(count_query)
    if not result or 'data' not in result or 'orders' not in result['data']:
        error_msg = "Invalid response structure from GraphQL API"
        logging.error(f"{error_msg}. Response: {result}")
        raise Exception(error_msg)
    
    initial_orders = result['data']['orders']['edges']
    total_count = len(initial_orders)
    has_more = result['data']['orders']['pageInfo']['hasNextPage']
    next_cursor = result['data']['orders']['pageInfo']['endCursor']
    
    # Count total orders
    while has_more:
        count_with_cursor = f"""
        query {{
            orders(first: 250, after: "{next_cursor}", query: "fulfillment_status:unfulfilled AND status:open") {{
                pageInfo {{
                    hasNextPage
                    endCursor
                }}
                edges {{
                    node {{
                        id
                    }}
                }}
            }}
        }}
        """
        result = execute_graphql_query(count_with_cursor)
        if not result or 'data' not in result or 'orders' not in result['data']:
            break
        batch = result['data']['orders']['edges']
        total_count += len(batch)
        has_more = result['data']['orders']['pageInfo']['hasNextPage']
        next_cursor = result['data']['orders']['pageInfo']['endCursor']
    
    # Fetch actual orders
    cursor = None
    while True:
        variables = {"cursor": cursor} if cursor else {}
        logging.info(f"Fetching orders with cursor: {cursor}")
        result = execute_graphql_query(orders_query, variables)
        
        if not result or 'data' not in result or 'orders' not in result['data']:
            break
        
        orders_data = result['data']['orders']
        if not orders_data.get('edges'):
            break
        
        all_orders.extend(orders_data['edges'])
        
        # Update progress
        progress = (len(all_orders) / total_count) if total_count > 0 else 0
        progress_bar.progress(progress, f"Fetched {len(all_orders)}/{total_count} orders")
        
        if not orders_data['pageInfo']['hasNextPage']:
            break
        cursor = orders_data['pageInfo']['endCursor']
        time.sleep(0.5)
    
    return all_orders

def check_stock_status(stock: int, required_quantity: int) -> str:
    if stock == 0:
        return "OUT OF STOCK"
    elif stock < required_quantity:
        return "NOT ENOUGH"
    else:
        return "OK"

def format_excel_output(writer: pd.ExcelWriter, df: pd.DataFrame, sheet_name: str) -> None:
    worksheet = writer.sheets[sheet_name]
    
    for idx, column in enumerate(df.columns, 1):
        col_letter = get_column_letter(idx)
        header_cell = worksheet.cell(row=1, column=idx)
        header_cell.font = Font(bold=True)
        header_cell.fill = PatternFill(start_color='CCCCCC', end_color='CCCCCC', fill_type='solid')
        header_cell.alignment = Alignment(horizontal='center', vertical='center')
        
        max_length = max(len(str((worksheet.cell(row=r, column=idx).value) or '')) for r in range(1, len(df)+2))
        adjusted_width = min(max_length + 2, 50)
        worksheet.column_dimensions[col_letter].width = adjusted_width

def apply_excel_formatting(workbook):
    # Define colors
    header_color = 'CE5870'
    colors = ['F2D5DB', 'FAEAED']  # Alternating colors for orders
    
    for sheet_name in workbook.sheetnames:
        worksheet = workbook[sheet_name]
        
        # Set row height for header and format it
        worksheet.row_dimensions[1].height = 50
        
        # Format header row
        for col in range(1, worksheet.max_column + 1):
            cell = worksheet.cell(row=1, column=col)
            cell.fill = PatternFill(start_color=header_color, end_color=header_color, fill_type='solid')
            cell.font = Font(bold=True, color='FFFFFF')
            cell.alignment = Alignment(horizontal='center', vertical='center')
        
        current_order = None
        color_index = 0
        
        for row in range(2, worksheet.max_row + 1):
            order_number = worksheet.cell(row=row, column=1).value
            if order_number != current_order:
                current_order = order_number
                color_index = (color_index + 1) % 2
            
            for col in range(1, worksheet.max_column + 1):
                cell = worksheet.cell(row=row, column=col)
                cell.fill = PatternFill(start_color=colors[color_index], end_color=colors[color_index], fill_type='solid')
                cell.alignment = Alignment(horizontal='center', vertical='center')
                
                # Format 'Margin %' column as percentage if needed
                header_cell = worksheet.cell(row=1, column=col)
                if header_cell.value == 'Margin %':
                    if cell.value is not None and row > 1:
                        try:
                            value = float(str(cell.value).replace('%',''))
                            cell.value = value / 100.0
                            cell.number_format = '0.00%'
                        except (ValueError, TypeError):
                            pass
        
        # Adjust column widths
        for col in range(1, worksheet.max_column + 1):
            max_length = 0
            column = get_column_letter(col)
            for row in range(1, worksheet.max_row + 1):
                cell = worksheet.cell(row=row, column=col)
                try:
                    if cell.value is not None:
                        max_length = max(max_length, len(str(cell.value)))
                except:
                    pass
            adjusted_width = max_length + 2
            worksheet.column_dimensions[column].width = adjusted_width

def save_excel_file(file_path: str, df_group1: pd.DataFrame, df_group2: pd.DataFrame, df_group3: pd.DataFrame) -> None:
    try:
        writer = pd.ExcelWriter(file_path, engine='openpyxl')
        
        df_group1.to_excel(writer, sheet_name='Group 1 - Full Stock', index=False)
        df_group2.to_excel(writer, sheet_name='Group 2 - Partial Stock', index=False)
        df_group3.to_excel(writer, sheet_name='Group 3 - No Stock', index=False)
        
        workbook = writer.book
        apply_excel_formatting(workbook)
        writer.close()
        
        logging.info(f"Excel file saved successfully: {file_path}")
        st.success("Analysis report saved successfully!")
    except Exception as e:
        err_trace = traceback.format_exc()
        logging.error(f"Error saving Excel file: {str(e)}\n{err_trace}")
        st.error(f"An error occurred while saving the file: {str(e)}")

def determine_zone(country: str) -> str:
    country = country.strip().lower()
    if country == 'united states':
        return 'USA'
    elif country == 'france':
        return 'Europe'
    elif country in ['canada', 'australia', 'new zealand']:
        return 'RestWorld'
    else:
        return 'RestWorld'

def get_azul_cost(total_weight_kg: float, zone: str) -> float:
    adjusted_weight = total_weight_kg * 1.10
    intervals = [
        (0.019, {"Europe":4.47,"RestWorld":4.89,"USA":5.23}),
        (0.049, {"Europe":4.47,"RestWorld":4.89,"USA":5.23}),
        (0.099, {"Europe":4.47,"RestWorld":4.89,"USA":5.23}),
        (0.249, {"Europe":5.65,"RestWorld":6.00,"USA":6.66}),
        (0.499, {"Europe":7.55,"RestWorld":10.14,"USA":11.75}),
        (0.999, {"Europe":10.65,"RestWorld":17.8,"USA":19.8}),
        (2.000, {"Europe":16.75,"RestWorld":28.00,"USA":30.35})
    ]

    for ub, costs in intervals:
        if adjusted_weight <= ub:
            return costs.get(zone, costs["RestWorld"])
    last = intervals[-1][1]
    return last.get(zone, last["RestWorld"])

def get_variant_data(session, variant_id):
    url = f"https://{store_name}.myshopify.com/admin/api/2023-01/variants/{variant_id}.json"
    response = session.get(url)
    if response.status_code == 200:
        return response.json()
    return None

def process_orders(orders: List[Dict]) -> List[Dict]:
    processed_data = []
    batch_size = 50
    session = requests.Session()
    session.headers.update({
        'X-Shopify-Access-Token': access_token,
        'Content-Type': 'application/json'
    })

    for batch_start in range(0, len(orders), batch_size):
        batch_orders = orders[batch_start:batch_start+batch_size]
        
        inventory_items = set()
        for order_edge in batch_orders:
            order = order_edge['node']
            for item_edge in order['lineItems']['edges']:
                item = item_edge['node']
                inventory_items.add(item['variant']['inventoryItem']['id'])
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            list(executor.map(fetch_cost_for_inventory_item, inventory_items))
            list(executor.map(fetch_inventory_levels, inventory_items))
        
        for order_edge in batch_orders:
            order = order_edge['node']
            logging.info(f"Processing Order: {order['name']}")

            country = order['shippingAddress']['country'] if order.get('shippingAddress') else ''
            zone = determine_zone(country)
            order_currency = order['totalPriceSet']['shopMoney']['currencyCode']
            total_value = float(order['totalPriceSet']['shopMoney']['amount'] or 0.0)

            charged_transport_cost = 0.0
            if order.get('totalShippingPriceSet') and order['totalShippingPriceSet'].get('shopMoney'):
                shipping_amount = order['totalShippingPriceSet']['shopMoney'].get('amount')
                if shipping_amount:
                    charged_transport_cost = float(shipping_amount)
            
            total_weight = 0.0
            sum_product_profit = 0.0
            
            line_items_data = []
            for item_edge in order['lineItems']['edges']:
                item = item_edge['node']
                inventory_item_id = item['variant']['inventoryItem']['id']
                cost_price = fetch_cost_for_inventory_item(inventory_item_id)
                quantity = item['quantity']
                final_price = float(item['variant']['price'] or 0.0)
                price_diff = final_price - cost_price

                variant_weight = float(item['variant'].get('weight', 0.0))
                total_weight += variant_weight * quantity
                sum_product_profit += (price_diff * quantity)
                
                line_items_data.append((item, cost_price, final_price, price_diff, quantity))
            
            real_transport_cost = get_azul_cost(total_weight, zone)
            
            transport_cost_difference = real_transport_cost - charged_transport_cost
            adjusted_total = total_value - charged_transport_cost if total_value != 0 else 0
            if adjusted_total != 0:
                margin_percent = 1 - ((sum_product_profit + transport_cost_difference) / adjusted_total)
            else:
                margin_percent = 0.0
            
            for (item, cost_price, final_price, price_diff, quantity) in line_items_data:
                inventory_item_id = item['variant']['inventoryItem']['id']
                stock_levels = fetch_inventory_levels(inventory_item_id)
                stock_flores = stock_levels[location_ids["Pr. das Flores, 54"]]
                stock_status = check_stock_status(stock_flores, quantity)

                variant_id = item['variant']['id']
                ean = ''
                if variant_id:
                    numeric_id = variant_id.split('/')[-1]
                    variant_data = get_variant_data(session, numeric_id)
                    if variant_data and 'variant' in variant_data:
                        ean = variant_data['variant'].get('barcode', '')

                processed_data.append({
                    "Order Number": order['name'],
                    "Order Date": order['createdAt'],
                    "Country": country,
                    "Currency Code": order_currency,
                    "SKU": item['sku'],
                    "EAN": ean,
                    "Product": item['title'],
                    "Quantity": quantity,
                    "Vendor": item['variant']['product']['vendor'],
                    "Cost_Per_Item": round(cost_price, 2),
                    "Unit_Price": round(final_price, 2),
                    "Compare_Price": round(float(item['variant']['compareAtPrice'] or 0.0), 2),
                    "Price_Diff": round(price_diff, 2),
                    "Real_Profit": round(price_diff * quantity, 2),
                    "Margin_Percent": round(margin_percent, 4),
                    "Total Value": round(total_value, 2),
                    "Stock_Status": stock_status,
                    "Stock_Flores": stock_flores,
                    "Stock_Warehouse": stock_levels[location_ids["Warehouse"]],
                    "Numeric_Weight": variant_weight,
                    "Transport_Cost": round(charged_transport_cost, 2),
                    "Real_Transport_Cost": round(real_transport_cost, 2),
                    "Zone": zone,
                    "group": ""
                })
                
    return processed_data

def convert_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    def margin_to_percent(x):
        if isinstance(x, str) and '%' in x:
            return x
        try:
            x_dec = str(x).replace(',', '.')
            val = float(x_dec) * 100.0
            return f"{val:.2f}%".replace('.', ',')
        except (ValueError, TypeError):
            return x

    def value_to_decimal(x):
        try:
            if isinstance(x, str):
                return float(x.replace(',', '.'))
            return float(x)
        except (ValueError, TypeError):
            return x

    numeric_columns = [
        "Total Value", "Transport_Cost", "Cost Per Item", "Unit Price",
        "Compare Price", "Price Diff", "Real Profit", "Margin %"
    ]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].apply(value_to_decimal)
            df[col] = df[col].apply(lambda x: f"{x:.2f}".replace('.', ',') if isinstance(x, (int, float)) else x)

    if "Margin %" in df.columns:
        df["Margin %"] = df["Margin %"].apply(margin_to_percent)

    return df

def add_total_weight_and_real_cost(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        df["Total_Weight"] = ""
        return df

    df = df.copy()
    
    def safe_numeric_convert(series):
        if series.dtype == object:
            return pd.to_numeric(series.str.replace(',', '.'), errors='coerce').fillna(0)
        return pd.to_numeric(series, errors='coerce').fillna(0)
    
    df['Numeric_Weight'] = safe_numeric_convert(df['Numeric_Weight'])
    
    order_weights = df.groupby("Order Number")['Numeric_Weight'].transform('sum')
    df["Total_Weight"] = order_weights
    df["Total_Weight"] = df["Total_Weight"].apply(lambda x: f"{x:.3f}".replace('.', ','))
    
    return df

def split_data_by_group(processed_data: List[Dict]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = pd.DataFrame(processed_data)
    
    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    df['Stock_Status'] = df['Stock_Status'].astype(str)
    
    df = df.copy()
    
    def determine_group(group_df):
        statuses = group_df['Stock_Status'].astype(str)
        if 'OK' in statuses.values:
            if 'OUT OF STOCK' not in statuses.values and 'NOT ENOUGH' not in statuses.values:
                return pd.Series(1, index=group_df.index)
            else:
                return pd.Series(2, index=group_df.index)
        else:
            return pd.Series(3, index=group_df.index)
    
    groups = []
    for _, group_df in df.groupby('Order Number'):
        groups.append(determine_group(group_df))
    df['group'] = pd.concat(groups)
    
    df = add_total_weight_and_real_cost(df)
    
    column_names = {
        "Order Number": "Order Number",
        "Order Date": "Order Date",
        "Country": "Country",
        "Currency Code": "Currency Code",
        "SKU": "SKU",
        "EAN": "EAN",
        "Product": "Product",
        "Total Value": "Total Value",
        "Quantity": "Quantity",
        "Vendor": "Vendor",
        "Stock_Status": "Stock Status",
        "Stock_Flores": "Stock Flores",
        "Stock_Warehouse": "Stock Warehouse",
        "Cost_Per_Item": "Cost Per Item",
        "Unit_Price": "Unit Price",
        "Compare_Price": "Compare Price",
        "Price_Diff": "Price Diff",
        "Real_Profit": "Real Profit",
        "Margin_Percent": "Margin %",
        "Transport_Cost": "Transport Cost",
        "Total_Weight": "Total Weight",
        "Real_Transport_Cost": "Real Transport Cost",
        "group": "Group"
    }
    
    df = df.rename(columns=column_names)
    
    column_order = [
        "Order Number", "Order Date", "Country", "Currency Code", "SKU", "EAN", "Product",
        "Quantity", "Vendor", "Cost Per Item", "Unit Price", "Compare Price", "Price Diff",
        "Real Profit", "Margin %", "Total Value", "Stock Status", "Stock Flores",
        "Stock Warehouse", "Total Weight", "Transport Cost", "Real Transport Cost", "Group"
    ]
    
    for col in column_order:
        if col not in df.columns:
            print(f"Missing column: {col}")
    
    df = df[column_order]
    
    df_group1 = df[df['Group'] == 1].copy()
    df_group2 = df[df['Group'] == 2].copy()
    df_group3 = df[df['Group'] == 3].copy()
    
    df_group1 = convert_numeric_columns(df_group1)
    df_group2 = convert_numeric_columns(df_group2)
    df_group3 = convert_numeric_columns(df_group3)
    
    return df_group1, df_group2, df_group3

def main():
    st.set_page_config(page_title="Order Analysis", layout="wide")
    st.title("Order Analysis Dashboard")
    
    # Check environment variables
    store_name = os.getenv('SHOPIFY_STORE_NAME')
    access_token = os.getenv('SHOPIFY_ACCESS_TOKEN')
    storefront_token = os.getenv('SHOPIFY_STOREFRONT_TOKEN')
    flores_location_id = os.getenv('LOCATION_ID_FLORES')
    warehouse_location_id = os.getenv('LOCATION_ID_WAREHOUSE')
    
    # Check required variables
    missing_vars = []
    if not store_name:
        missing_vars.append('SHOPIFY_STORE_NAME')
    if not access_token:
        missing_vars.append('SHOPIFY_ACCESS_TOKEN')
    if not flores_location_id:
        missing_vars.append('LOCATION_ID_FLORES')
    if not warehouse_location_id:
        missing_vars.append('LOCATION_ID_WAREHOUSE')
    
    if missing_vars:
        st.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        st.info("""
        Please set up the following environment variables in Streamlit Cloud:
        1. SHOPIFY_STORE_NAME - Your Shopify store subdomain
        2. SHOPIFY_ACCESS_TOKEN - Your Shopify Admin API access token
        3. LOCATION_ID_FLORES - Location ID for Pr. das Flores
        4. LOCATION_ID_WAREHOUSE - Location ID for Warehouse
        
        To set these variables:
        1. Go to your app settings in Streamlit Cloud
        2. Find the 'Secrets' section
        3. Add the variables in this format:
        ```
        SHOPIFY_STORE_NAME = "your-store-name"
        SHOPIFY_ACCESS_TOKEN = "your-access-token"
        LOCATION_ID_FLORES = "your-flores-location-id"
        LOCATION_ID_WAREHOUSE = "your-warehouse-location-id"
        ```
        """)
        return
    
    # Optional warning for storefront token
    if not storefront_token:
        st.warning("SHOPIFY_STOREFRONT_TOKEN not found. International pricing might be limited.")
    
    location_ids = {
        "Pr. das Flores, 54": flores_location_id,
        "Warehouse": warehouse_location_id
    }
    
    # Create columns for layout
    col1, col2 = st.columns([2, 1])
    
    with col1:
        if st.button("Start Analysis", disabled=st.session_state.get('is_running', False)):
            st.session_state.is_running = True
            st.session_state.start_time = time.time()
            
            # Fetch and process orders
            with st.spinner("Fetching orders..."):
                orders = fetch_unfulfilled_orders_with_progress()
                if orders:
                    processed_data = process_orders(orders)
                    st.session_state.processed_data = processed_data
                    
                    # Split data into groups
                    df_group1, df_group2, df_group3 = split_data_by_group(processed_data)
                    
                    # Display results
                    st.success("Analysis completed!")
                    
                    # Show statistics
                    st.subheader("Statistics")
                    for group_name, df in [("Group 1", df_group1), ("Group 2", df_group2), ("Group 3", df_group3)]:
                        if not df.empty:
                            count = len(df)
                            total = df['Total Value'].sum()
                            st.metric(f"{group_name}", f"{count} orders", f"€{total:.2f} total")
                    
                    # Add export button
                    if st.button("Export to Excel"):
                        save_excel_file("order_analysis.xlsx", df_group1, df_group2, df_group3)
                        st.success("Data exported to Excel!")
            
            st.session_state.is_running = False
    
    with col2:
        if st.session_state.get('start_time', 0) > 0:
            elapsed_time = time.time() - st.session_state.start_time
            st.info(f"Elapsed Time: {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    main()
