import csv
import random
import string
import datetime
import os

# Set the number of records
NUM_CUSTOMERS = 2000
NUM_TRANSACTIONS = 5000

# Make sure we define absolute or relative paths correctly
DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))

def random_string(length=5):
    return ''.join(random.choice(string.ascii_uppercase) for _ in range(length))

def generate_crm_data():
    crm_path = os.path.join(DATA_DIR, 'crm', 'customers.csv')
    with open(crm_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["customer_id", "first_name", "last_name", "email", "signup_date", "region"])

        for i in range(1, NUM_CUSTOMERS+1):
            first_name = "Name" + random_string(3)
            last_name = "Surname" + random_string(2)
            email = f"{first_name.lower()}@example.com"
            signup_date = datetime.date(2020,1,1) + datetime.timedelta(days=random.randint(0,365*3))
            region = random.choice(["North", "South", "East", "West"])
            writer.writerow([i, first_name, last_name, email, signup_date, region])

def generate_transaction_data():
    transactions_path = os.path.join(DATA_DIR, 'transactions', 'orders.csv')
    with open(transactions_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "customer_id", "order_date", "amount", "product_category"])

        for i in range(1, NUM_TRANSACTIONS+1):
            customer_id = random.randint(1, NUM_CUSTOMERS)
            order_date = datetime.date(2020,1,1) + datetime.timedelta(days=random.randint(0,365*3))
            amount = round(random.uniform(10.0, 500.0), 2)
            product_category = random.choice(["Electronics", "Books", "Clothing", "Home", "Sports"])
            writer.writerow([i, customer_id, order_date, amount, product_category])

def generate_marketing_data():
    marketing_path = os.path.join(DATA_DIR, 'marketing', 'campaigns.csv')
    with open(marketing_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["campaign_id", "customer_id", "channel", "sent_date", "response"])

        campaign_id = 1
        for i in range(1, NUM_CUSTOMERS+1):
            # Each customer can receive 1-3 campaigns
            for _ in range(random.randint(1, 3)):
                channel = random.choice(["Email", "SMS", "Social", "DisplayAd"])
                sent_date = datetime.date(2020,1,1) + datetime.timedelta(days=random.randint(0,365*3))
                response = random.choice([0,1])  # 1 = responded, 0 = no
                writer.writerow([campaign_id, i, channel, sent_date, response])
                campaign_id += 1

if __name__ == "__main__":
    print("Generating synthetic data...")
    generate_crm_data()
    generate_transaction_data()
    generate_marketing_data()
    print("Synthetic data generated successfully!")
