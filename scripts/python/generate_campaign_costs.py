import csv, random

def generate_campaign_costs():
    # We'll load the existing campaigns from 'campaigns.csv'
    campaign_ids = []
    with open('../data/raw/campaigns.csv', 'r') as f:
        next(f)  # skip header
        for line in f:
            parts = line.strip().split(',')
            cid = parts[0]  # campaign_id is first column
            campaign_ids.append(cid)

    campaign_ids = list(set(campaign_ids))  # unique
    with open('../data/raw/campaign_costs.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["campaign_id", "campaign_cost"])
        for cid in campaign_ids:
            # random cost between 100 and 5000
            cost = round(random.uniform(100.0, 5000.0), 2)
            writer.writerow([cid, cost])

if __name__ == "__main__":
    generate_campaign_costs()
    print("campaign_costs.csv generated!")

