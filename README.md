# Cosmetyque Automation Tools

This repository contains two main applications for automating Cosmetyque's operations:

1. Order Analysis Dashboard
2. Product Integration System
3. Automated Order Analysis Script (Headless)

## Order Analysis Dashboard

A Streamlit application for analyzing unfulfilled Shopify orders.

### Setup

1. Clone this repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file with your Shopify credentials:
```
SHOPIFY_STORE_NAME=your-store-name
SHOPIFY_ACCESS_TOKEN=your-access-token
SHOPIFY_STOREFRONT_TOKEN=your-storefront-token
LOCATION_ID_FLORES=your-flores-location-id
LOCATION_ID_WAREHOUSE=your-warehouse-location-id
```

### Running Locally

```bash
streamlit run app.py
```

### Deploying to Streamlit Cloud

1. Push your code to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Sign in with GitHub
4. Deploy your app by selecting your repository
5. Add your environment variables in the Streamlit Cloud dashboard under "Secrets"

## Product Integration System

A Python application for managing product data between Excel files, Shopify, and SAGE.

### Features

- Load and validate Excel files with product data
- Verify products against Shopify inventory
- Create and update products in Shopify
- Generate SAGE article data
- Detailed logging of all operations
- User-friendly interface with progress tracking

### Project Structure

```
├── config.py           # Configuration settings and constants
├── data_manager.py     # Data processing and transformation
├── logger.py           # Logging utilities
├── main.py            # Main application entry point
├── shopify_client.py  # Shopify API client
├── ui_manager.py      # User interface management
└── requirements.txt   # Project dependencies
```

### Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure Shopify credentials in `config.py` or set environment variables:
```python
SHOPIFY_TOKEN = "your-shopify-token"
SHOPIFY_STORE = "your-store.myshopify.com"
```

### Running the Application

```bash
python main.py
```

### Usage

1. Click "Carregar Grelha 1" to load the product data Excel file
2. Click "Carregar Grelha 2" to load the supplier data Excel file
3. Use "Verificar Shopify" to check product status
4. "Atualizar Dados no Shopify" to create/update products
5. "Criar Artigos SAGE" to generate SAGE article data

The interface provides visual feedback with color-coding:
- Light Blue: Products to update in Shopify
- Yellow: Products to create in Shopify
- Red: Products missing from Grelha 1

### Logging

The application generates detailed logs for all operations:
- Shopify operations log: `YYYYMMDD_HHMMSS_cosmetyque.log`
- SAGE data export: `YYYYMMDD_HHMMSS_sage_items.json`
- Application log: `application.log`

## Automated Order Analysis Script (Headless)

### Shopify Order Analysis Automation

This script automates the analysis of unfulfilled orders from your Shopify store, generating detailed Excel reports and sending them via email.

### Features

- Fetches unfulfilled orders from Shopify using GraphQL API
- Calculates shipping costs based on weight and destination
- Analyzes stock levels across multiple locations
- Generates detailed Excel reports with order analysis
- Automated email delivery of reports
- GitHub Actions integration for daily automated execution

### Setup

1. Clone this repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file based on `.env.example` with your credentials:
```env
# Shopify store details (required)
SHOPIFY_STORE_NAME=your_store_name
SHOPIFY_ACCESS_TOKEN=your_access_token
SHOPIFY_STOREFRONT_TOKEN=your_storefront_token

# Location IDs (required)
LOCATION_ID_FLORES=gid://shopify/Location/your_location_id
LOCATION_ID_WAREHOUSE=gid://shopify/Location/your_warehouse_id

# Email Configuration (required for automated deployment)
EMAIL_SENDER=your_email@gmail.com
EMAIL_PASSWORD=your_app_password
EMAIL_RECIPIENTS=recipient1@example.com,recipient2@example.com
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
```

4. For GitHub Actions deployment:
   - Go to your repository's Settings > Secrets and Variables > Actions
   - Add all the environment variables from your `.env` file as repository secrets

### Usage

#### Local Execution
```bash
python Windsurf_Analise_Encomendas_Auto_Grouping_v26_Revised_Working_Deployment_GITHUB.py --output "orders_report.xlsx" --email
```

#### Arguments
- `--output`: Specify the output Excel file path (optional)
- `--email`: Send the report via email (optional)

#### Automated Execution
The script is configured to run automatically every day at 6:00 AM UTC via GitHub Actions. The workflow will:
1. Fetch unfulfilled orders
2. Generate an Excel report
3. Send the report via email
4. Upload the report as a GitHub Actions artifact

### Report Structure

The Excel report contains three sheets:
1. **Group 1 - Full Stock**: Orders where all items are in stock
2. **Group 2 - Partial Stock**: Orders where some items have stock issues
3. **Group 3 - No Stock**: Orders where all items are out of stock

### Security Notes

- Store API tokens and credentials securely in environment variables
- Use Admin API access tokens (shpat_) for full access
- Keep separate `.env` files for development and production
- Never commit sensitive information to the repository

### Rate Limiting

The script includes built-in rate limiting handling for Shopify's API:
- Default limit: 2 requests/second
- Automatic retry with exponential backoff
- Parallel processing of inventory data with thread pooling
