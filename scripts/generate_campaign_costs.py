import csv
import random
import os
import sys
from pathlib import Path

def ensure_directory_exists(filepath):
    """Create directory if it doesn't exist"""
    directory = os.path.dirname(filepath)
    Path(directory).mkdir(parents=True, exist_ok=True)

def read_campaign_ids(input_file):
    """Read campaign IDs from the campaigns CSV file"""
    try:
        campaign_ids = set()
        with open(input_file, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # skip header
            for row in reader:
                if row:  # ensure row is not empty
                    campaign_ids.add(row[0])  # campaign_id is first column
        return sorted(list(campaign_ids))
    except FileNotFoundError:
        print(f"Error: Input file not found: {input_file}")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading campaign IDs: {str(e)}")
        sys.exit(1)

def generate_campaign_costs():
    """Generate campaign costs data and save to CSV"""
    input_file = '/opt/data/raw/campaigns.csv'
    output_file = '/opt/data/raw/campaign_costs.csv'

    try:
        # Ensure output directory exists
        ensure_directory_exists(output_file)

        # Get unique campaign IDs
        campaign_ids = read_campaign_ids(input_file)
        
        # Generate and write costs
        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["campaign_id", "campaign_cost"])
            
            for cid in campaign_ids:
                # Generate random cost between 100 and 5000
                cost = round(random.uniform(100.0, 5000.0), 2)
                writer.writerow([cid, cost])
        
        print(f"Successfully generated campaign costs for {len(campaign_ids)} campaigns")
        print(f"Output saved to: {output_file}")

    except PermissionError:
        print(f"Error: Permission denied while writing to {output_file}")
        sys.exit(1)
    except Exception as e:
        print(f"Error generating campaign costs: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    generate_campaign_costs()

