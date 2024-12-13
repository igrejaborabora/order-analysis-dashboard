# Order Analysis Dashboard

A Streamlit application for analyzing unfulfilled Shopify orders.

## Setup

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

## Running Locally

```bash
streamlit run app.py
```

## Deploying to Streamlit Cloud

1. Push your code to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Sign in with GitHub
4. Deploy your app by selecting your repository
5. Add your environment variables in the Streamlit Cloud dashboard under "Secrets"
