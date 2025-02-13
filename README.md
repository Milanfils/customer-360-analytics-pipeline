# Customer 360 Analytics Pipeline

End-to-end analytics pipeline for customer data analysis using Apache Airflow and PostgreSQL. This pipeline processes customer data, order history, campaign performance, and product costs to create a comprehensive 360-degree view of customer behavior and business performance.

## Overview

This project implements a data pipeline that:
- Ingests customer transaction and interaction data
- Processes marketing campaign performance
- Analyzes product costs and profitability
- Generates customer lifetime value metrics
- Creates analytical views for business intelligence

## System Requirements

- Docker (version 20.10.0 or higher)
- Docker Compose (version 2.0.0 or higher)
- 8GB RAM (minimum)
- 20GB free disk space

## Project Structure

```
.
├── data/
│   └── raw/               # Raw CSV data files
│       ├── customers.csv
│       ├── orders.csv
│       ├── campaigns.csv
│       ├── campaign_costs.csv
│       ├── product_costs.csv
│       └── dim_date.csv
├── dags/
│   └── dag_csv_to_postgres.py    # Main Airflow DAG
├── scripts/
│   ├── sql/
│   │   ├── init.sql             # Database initialization
│   │   └── transformations.sql   # Data transformations
│   └── python/
│       └── generate_sample_data.py  # Sample data generation
├── docker-compose.yaml    # Docker services configuration
└── README.md
```

## Setup Instructions

1. Clone the repository:
```bash
git clone https://github.com/yourusername/customer-360-analytics-pipeline.git
cd customer-360-analytics-pipeline
```

2. Start the services:
```bash
docker-compose up -d
```

3. Initialize Airflow connections:
```bash
docker exec customer-360-analytics-pipeline-airflow-webserver-1 airflow connections add \
    'postgres_customer360' \
    --conn-type 'postgres' \
    --conn-host 'postgres' \
    --conn-schema 'customer360' \
    --conn-login 'airflow' \
    --conn-password 'airflow' \
    --conn-port 5432
```

4. Access the Airflow web interface:
- URL: http://localhost:8080
- Username: airflow
- Password: airflow

## Data Schema

### Base Tables

1. **customers**
- customer_id (PK)
- first_name
- last_name
- email
- signup_date
- region

2. **orders**
- order_id (PK)
- customer_id (FK)
- order_date
- amount
- product_category

3. **campaigns**
- campaign_id (PK)
- name
- start_date
- end_date
- channel
- target_audience

4. **campaign_costs**
- campaign_id (FK)
- date
- cost
- impressions
- clicks

5. **product_costs**
- product_category (PK)
- cost_date
- unit_cost
- supplier

6. **dim_date**
- date_id (PK)
- full_date
- year
- quarter
- month
- day_of_month
- day_of_week
- is_weekend

### Transformed Views

1. **customer_lifetime_value**
- Aggregates customer purchase history
- Calculates average order value
- Determines purchase frequency
- Estimates customer lifetime value

2. **campaign_performance**
- Measures campaign ROI
- Tracks conversion rates
- Analyzes cost per acquisition
- Segments by channel effectiveness

3. **product_profitability**
- Calculates margins by product category
- Tracks cost trends
- Identifies high and low performers

## Usage Instructions

1. Ensure all services are running:
```bash
docker ps
```

2. Verify data files exist in data/raw/

3. Access Airflow UI at http://localhost:8080

4. Trigger the pipeline:
```bash
docker exec customer-360-analytics-pipeline-airflow-webserver-1 airflow dags trigger customer360_analytics_pipeline
```

5. Monitor progress in Airflow UI

6. Query results in PostgreSQL:
```bash
docker exec -it customer-360-analytics-pipeline-postgres-1 psql -U airflow -d customer360
```

## Monitoring and Maintenance

- Airflow logs: Available in the Airflow UI or container logs
- Pipeline runs daily at 6 AM UTC
- Failed tasks automatically retry 3 times
- Database backups should be scheduled separately
- Monitor disk usage for CSV storage

## Common Issues and Solutions

1. **Connection Issues**
- Verify PostgreSQL is running
- Check connection credentials
- Ensure network connectivity between containers

2. **Data Load Failures**
- Verify CSV file formats
- Check file permissions
- Monitor available disk space

3. **Transformation Errors**
- Review PostgreSQL logs
- Check for data quality issues
- Verify table dependencies

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit changes
4. Create pull request

## License

MIT License - See LICENSE file for details
