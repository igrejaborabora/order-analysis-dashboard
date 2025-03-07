import os
import logging
import requests
import pandas as pd
import time
import traceback
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple
from dotenv import load_dotenv

# Configure logging
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

# Shopify API Configuration
flores_location_id = os.getenv('LOCATION_ID_FLORES')
warehouse_location_id = os.getenv('LOCATION_ID_WAREHOUSE')

if not flores_location_id or not warehouse_location_id:
    raise ValueError("Location IDs must be set in environment variables")

def check_stock_status(stock: int, required_quantity: int) -> str:
    """
    Determine the stock status based on available stock and required quantity.
    
    Args:
        stock (int): Total available stock across all locations
        required_quantity (int): Quantity needed for the order
        
    Returns:
        str: Stock status - "OK", "NOT ENOUGH", or "OUT OF STOCK"
    """
    if stock == 0:
        return "OUT OF STOCK"
    elif stock < required_quantity:
        return "NOT ENOUGH"
    else:
        return "OK"

def fetch_unfulfilled_orders() -> List[Dict]:
    all_orders = []
    result = execute_graphql_query(count_query)
    if not result or 'data' not in result or 'orders' not in result['data']:
        logger.error("Failed to get order count")
        return []
    
    total_count = len(result['data']['orders']['edges'])
    has_more = result['data']['orders']['pageInfo']['hasNextPage']
    next_cursor = result['data']['orders']['pageInfo']['endCursor'] if has_more else None
    
    logger.info(f"Total unfulfilled orders: {total_count}")
    
    cursor = None
    while True:
        variables = {"cursor": cursor} if cursor else {}
        result = execute_graphql_query(orders_query, variables)
        
        if not result or 'data' not in result or 'orders' not in result['data']:
            logger.error("Failed to fetch orders")
            break
        
        orders_data = result['data']['orders']
        
        all_orders.extend(orders_data['edges'])
        
        if not orders_data['pageInfo']['hasNextPage']:
            break
        cursor = orders_data['pageInfo']['endCursor']
    
    # Extract the node from each edge
    processed_orders = []
    for order_edge in all_orders:
        if 'node' in order_edge:
            processed_orders.append(order_edge['node'])
    
    logger.info(f"Fetched {len(processed_orders)} orders")
    return processed_orders

def process_orders(orders: List[Dict]) -> List[Dict]:
    processed_data = []
    inventory_items = set()
    
    # Process in batches of 10 orders
    for batch_orders in [orders[i:i+10] for i in range(0, len(orders), 10)]:
        # First collect all inventory items to fetch costs and inventory levels
        for order in batch_orders:
            if 'lineItems' not in order or 'edges' not in order['lineItems']:
                logger.warning(f"Order {order.get('name', 'unknown')} has invalid lineItems structure")
                continue
                
            for item_edge in order['lineItems']['edges']:
                item = item_edge['node']
                if item.get('variant') and item['variant'].get('inventoryItem'):
                    inventory_items.add(item['variant']['inventoryItem']['id'])
        
        # Fetch costs and inventory levels in parallel
        with ThreadPoolExecutor(max_workers=10) as executor:
            list(executor.map(fetch_cost_for_inventory_item, inventory_items))
            list(executor.map(fetch_inventory_levels, inventory_items))

        # Now process each order
        for order in batch_orders:
            if 'lineItems' not in order or 'edges' not in order['lineItems']:
                continue
                
            logger.info(f"Processing Order: {order['name']}")

            country = order['shippingAddress']['country'] if order.get('shippingAddress') else ''
            zone = determine_zone(country)

            total_value_eur = float(order['totalPriceSet']['shopMoney']['amount'])
            total_value_local = float(order['totalPriceSet']['presentmentMoney']['amount'])
            local_currency_code = order['totalPriceSet']['presentmentMoney']['currencyCode']

            # We no longer need Currency Code and Override Price columns in final output
            order_currency = order['totalPriceSet']['shopMoney']['currencyCode']

            charged_transport_cost = 0.0
            if order.get('totalShippingPriceSet') and order['totalShippingPriceSet'].get('shopMoney'):
                shipping_amount = order['totalShippingPriceSet']['shopMoney'].get('amount')
                if shipping_amount:
                    charged_transport_cost = float(shipping_amount)

            total_weight = 0.0
            logger.info(f"\n=== Weight Calculation for Order {order['name']} ===")
            for item_edge in order['lineItems']['edges']:
                try:
                    item = item_edge['node']
                    quantity = item['quantity']
                    
                    # Skip items without variant information
                    if not item.get('variant'):
                        logger.warning(f"Item {item.get('title', 'unknown')} has no variant information")
                        continue
                    
                    variant = item['variant']
                    inventory_item_id = variant['inventoryItem']['id'] if variant.get('inventoryItem') else None
                    
                    if not inventory_item_id:
                        logger.warning(f"Item {item.get('title', 'unknown')} has no inventory item ID")
                        continue
                    
                    # Get inventory levels
                    stock_levels = fetch_inventory_levels(inventory_item_id)
                    
                    # Get cost
                    cost = fetch_cost_for_inventory_item(inventory_item_id)
                    
                    # Get stock status
                    flores_stock = stock_levels.get(flores_location_id, 0)
                    warehouse_stock = stock_levels.get(warehouse_location_id, 0)
                    
                    stock_status = check_stock_status(flores_stock + warehouse_stock, quantity)
                    
                    # Get pricing
                    base_price = float(variant.get('price', 0.0))
                    compare_price = variant.get('compareAtPrice')
                    compare_price = float(compare_price) if compare_price else 0.0
                    
                    # Calculate weight
                    weight = variant.get('weight', 0.0)
                    if weight is None:
                        weight = 0.0
                    
                    weight_unit = variant.get('weightUnit', 'KILOGRAMS')
                    
                    # Convert weight to kg if needed
                    weight_in_kg = weight
                    if weight_unit == 'GRAMS':
                        weight_in_kg = weight / 1000.0
                    elif weight_unit == 'POUNDS':
                        weight_in_kg = weight * 0.45359237
                    elif weight_unit == 'OUNCES':
                        weight_in_kg = weight * 0.02834952
                    
                    total_weight += weight_in_kg * quantity
                    
                    # Get local price
                    local_price = 0.0
                    if item.get('originalUnitPriceSet') and item['originalUnitPriceSet'].get('presentmentMoney'):
                        local_price = float(item['originalUnitPriceSet']['presentmentMoney'].get('amount', 0.0))
                    
                    # Calculate exchange rate
                    xe = local_price / base_price if base_price > 0 else 1.0
                    
                    # Calculate profit and margin
                    price_diff = base_price - cost
                    real_profit = price_diff * quantity
                    margin_percent = (price_diff / base_price) if base_price > 0 else 0.0
                    
                    # Get product vendor
                    vendor = variant['product']['vendor'] if variant.get('product') and variant['product'].get('vendor') else ''
                    
                    # Get barcode/EAN
                    barcode = variant.get('barcode', '')
                    
                    # Format order date
                    order_date = datetime.fromisoformat(order['createdAt'].replace('Z', '+00:00')).strftime('%Y-%m-%d')
                    
                    # Add to processed data
                    processed_data.append({
                        "Order Number": order['name'],
                        "Order Date": order_date,
                        "Country": country,
                        "SKU": item.get('sku', ''),
                        "EAN": barcode,
                        "Product": item.get('title', ''),
                        "Quantity": quantity,
                        "Vendor": vendor,
                        "Shopify Cost": cost,
                        "Base Price (€)": base_price,
                        "Compare At Price (€)": compare_price,
                        "Item Local Price": local_price,
                        "Total Value": total_value_eur,
                        "Total Local Value": total_value_local,
                        "XE": xe,
                        "Price Diff": price_diff,
                        "Real Profit": real_profit,
                        "Margin %": margin_percent,
                        "Stock Status": stock_status,
                        "Stock Flores": flores_stock,
                        "Stock Warehouse": warehouse_stock,
                        "Numeric_Weight": weight_in_kg * quantity,
                        "Transport Cost": charged_transport_cost
                    })
                    
                except Exception as e:
                    logger.error(f"Error processing item in order {order['name']}: {str(e)}")
                    logger.error(traceback.format_exc())
                    continue
            
            # Calculate real transport cost based on total weight
            logger.info(f"Total weight for order {order['name']}: {total_weight:.3f} kg")
            
            # Update transport cost for all items in this order
            real_transport_cost = 0.0
            if country.lower() == 'portugal':
                real_transport_cost = get_ctt_expresso_cost(total_weight)
            else:
                real_transport_cost = get_azul_cost(total_weight, zone)
            
            for item in processed_data:
                if item["Order Number"] == order['name']:
                    item["Real Transport Cost"] = real_transport_cost

    return processed_data

def split_data_by_group(processed_data: List[Dict]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = pd.DataFrame(processed_data)
    
    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    df['Stock Status'] = df['Stock Status'].astype(str)
    
    def determine_group(group_df):
        # Count occurrences of each status
        status_counts = group_df['Stock Status'].value_counts()
        
        # Group 1: All items are "OK"
        if len(status_counts) == 1 and 'OK' in status_counts:
            return pd.Series(1, index=group_df.index)
        
        # Group 3: Any item is "OUT OF STOCK"
        if 'OUT OF STOCK' in status_counts:
            return pd.Series(3, index=group_df.index)
        
        # Group 2: Mix of "OK" and "NOT ENOUGH" or all "NOT ENOUGH"
        return pd.Series(2, index=group_df.index)
    
    groups = []
    for _, group_df in df.groupby('Order Number'):
        groups.append(determine_group(group_df))
    df['group'] = pd.concat(groups)
    
    df = add_total_weight_and_real_cost(df)
    
    column_names = {
        "Order Number": "Order Number",
        "Order Date": "Order Date",
        "Country": "Country",
        "SKU": "SKU",
        "EAN": "EAN",
        "Product": "Product",
        "Quantity": "Quantity",
        "Vendor": "Vendor",
        "Shopify Cost": "Shopify Cost",
        "Base Price (€)": "Base Price (€)",
        "Compare At Price (€)": "Compare At Price (€)",
        "Item Local Price": "Item Local Price",
        "Total Value": "Total Value",
        "Total Local Value": "Total Local Value",
        "XE": "XE",
        "Price Diff": "Price Diff",
        "Real Profit": "Real Profit",
        "Margin %": "Margin %",
        "Stock Status": "Stock Status",
        "Stock Flores": "Stock Flores",
        "Stock Warehouse": "Stock Warehouse",
        "Total Weight": "Total Weight",
        "Transport Cost": "Transport Cost",
        "Real Transport Cost": "Real Transport Cost",
        "group": "Group"
    }
    
    df = df.rename(columns=column_names)
    
    # Removed Currency Code and Override Price from the final output
    column_order = [
        "Order Number", "Order Date", "Country", "SKU", "EAN", "Product",
        "Quantity", "Vendor", "Shopify Cost", "Base Price (€)",
        "Compare At Price (€)", "Item Local Price", "Total Value", "Total Local Value", "XE", "Price Diff",
        "Real Profit", "Margin %", "Stock Status", "Stock Flores", "Stock Warehouse",
        "Total Weight", "Transport Cost", "Real Transport Cost", "Group"
    ]

    missing_cols = [c for c in column_order if c not in df.columns]
    for c in missing_cols:
        df[c] = ""
    
    df = df[column_order]
    
    df_group1 = df[df['Group'] == 1].copy()
    df_group2 = df[df['Group'] == 2].copy()
    df_group3 = df[df['Group'] == 3].copy()
    
    df_group1 = convert_numeric_columns(df_group1)
    df_group2 = convert_numeric_columns(df_group2)
    df_group3 = convert_numeric_columns(df_group3)
    
    return df_group1, df_group2, df_group3
