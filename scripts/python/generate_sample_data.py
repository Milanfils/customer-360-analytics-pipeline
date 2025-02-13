import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import os
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Faker
fake = Faker()
Faker.seed(12345)
np.random.seed(12345)

# Constants
START_DATE = datetime(2019, 1, 1)
END_DATE = datetime(2024, 1, 1)
N_CUSTOMERS = 1000
N_ORDERS = 5000
N_CAMPAIGNS = 100
N_PRODUCT_CATEGORIES = 20
OUTPUT_DIR = Path("data/raw")

def generate_dim_date():
    """Generate date dimension table with business calendar attributes."""
    logger.info("Generating dim_date dataset...")
    
    dates = pd.date_range(start=START_DATE, end=END_DATE, freq='D')
    dim_date = pd.DataFrame({
        'date_key': dates,
        'full_date': dates,
        'year': dates.year,
        'quarter': dates.quarter,
        'month': dates.month,
        'day_of_month': dates.day,
        'day_of_week': dates.dayofweek,
        'is_weekend': dates.dayofweek.isin([5, 6])
    })
    
    return dim_date

def generate_customers():
    """Generate customer dataset with realistic attributes."""
    logger.info("Generating customers dataset...")
    
    customers = pd.DataFrame({
        'customer_id': range(1, N_CUSTOMERS + 1),
        'first_name': [fake.first_name() for _ in range(N_CUSTOMERS)],
        'last_name': [fake.last_name() for _ in range(N_CUSTOMERS)],
        'email': [fake.email() for _ in range(N_CUSTOMERS)],
        'signup_date': [fake.date_between(START_DATE, END_DATE) for _ in range(N_CUSTOMERS)],
        'region': [fake.state() for _ in range(N_CUSTOMERS)]
    })
    
    return customers

def generate_product_costs():
    """Generate product categories and their costs."""
    logger.info("Generating product_costs dataset...")
    
    categories = [f"Category_{i+1}" for i in range(N_PRODUCT_CATEGORIES)]
    
    product_costs = pd.DataFrame({
        'product_category': categories,
        'average_cogs': np.random.uniform(10, 100, len(categories))
    })
    
    return product_costs

def generate_orders(customers, product_costs):
    """Generate order data with realistic patterns."""
    logger.info("Generating orders dataset...")
    
    # Generate more orders for recent dates (recency bias)
    dates = pd.date_range(START_DATE, END_DATE)
    weights = np.linspace(0.5, 1.5, len(dates))  # More recent dates have higher weights
    order_dates = np.random.choice(dates, size=N_ORDERS, p=weights/weights.sum())
    
    orders = pd.DataFrame({
        'order_id': range(1, N_ORDERS + 1),
        'customer_id': np.random.choice(customers['customer_id'], N_ORDERS),
        'order_date': sorted(order_dates),
        'product_category': np.random.choice(product_costs['product_category'], N_ORDERS),
        'amount': np.random.lognormal(4, 1, N_ORDERS)  # Log-normal distribution for order amounts
    })
    
    return orders

def generate_campaigns():
    """Generate marketing campaign data."""
    logger.info("Generating campaigns dataset...")
    
    campaign_types = ['Email', 'Social Media', 'Search', 'Display', 'Direct Mail']
    campaign_dates = pd.date_range(START_DATE, END_DATE, periods=N_CAMPAIGNS)
    
    campaigns = pd.DataFrame({
        'campaign_id': range(1, N_CAMPAIGNS + 1),
        'campaign_name': [f"Campaign_{i+1}" for i in range(N_CAMPAIGNS)],
        'channel': np.random.choice(campaign_types, N_CAMPAIGNS),
        'start_date': campaign_dates,
        'end_date': campaign_dates + pd.Timedelta(days=30),
        'target_audience': np.random.choice(['New', 'Existing', 'Both'], N_CAMPAIGNS)
    })
    
    return campaigns

def generate_campaign_costs(campaigns):
    """Generate daily campaign cost data."""
    logger.info("Generating campaign_costs dataset...")
    
    # Create daily costs for each campaign
    campaign_costs = []
    
    for _, campaign in campaigns.iterrows():
        dates = pd.date_range(campaign['start_date'], campaign['end_date'])
        base_cost = np.random.uniform(100, 1000)
        
        # Generate daily costs with some randomness
        daily_costs = pd.DataFrame({
            'campaign_id': campaign['campaign_id'],
            'date': dates,
            'daily_cost': np.random.normal(base_cost, base_cost * 0.1, len(dates))
        })
        
        campaign_costs.append(daily_costs)
    
    campaign_costs = pd.concat(campaign_costs, ignore_index=True)
    campaign_costs['daily_cost'] = campaign_costs['daily_cost'].clip(lower=0)  # Ensure no negative costs
    
    return campaign_costs

def save_to_csv(dataframe, filename):
    """Save DataFrame to CSV with error handling."""
    try:
        output_path = OUTPUT_DIR / filename
        dataframe.to_csv(output_path, index=False)
        logger.info(f"Successfully saved {filename}")
    except Exception as e:
        logger.error(f"Error saving {filename}: {str(e)}")
        raise

def main():
    """Main function to generate and save all datasets."""
    try:
        # Create output directory if it doesn't exist
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        
        # Generate all datasets
        dim_date = generate_dim_date()
        customers = generate_customers()
        product_costs = generate_product_costs()
        orders = generate_orders(customers, product_costs)
        campaigns = generate_campaigns()
        campaign_costs = generate_campaign_costs(campaigns)
        
        # Save all datasets to CSV
        datasets = {
            'dim_date.csv': dim_date,
            'customers.csv': customers,
            'product_costs.csv': product_costs,
            'orders.csv': orders,
            'campaigns.csv': campaigns,
            'campaign_costs.csv': campaign_costs
        }
        
        for filename, df in datasets.items():
            save_to_csv(df, filename)
        
        logger.info("Successfully generated all datasets")
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()

