# Customer 360 Analytics Pipeline

End-to-end analytics pipeline for customer data analysis using Apache Airflow and PostgreSQL. This pipeline processes customer data, order history, campaign performance, and product costs to create a comprehensive 360-degree view of customer behavior and business performance.

## Overview

This project implements an end-to-end data pipeline that provides a comprehensive 360-degree view of customer behavior and business performance through:
- Customer transaction and interaction data processing
- Marketing campaign performance analysis
- Product cost and profitability tracking
- Customer lifetime value calculations
- Business intelligence analytical views
- Interactive dashboard visualization

## Tech Stack

### Data Processing & Orchestration
- Apache Airflow 2.7.1 - Workflow orchestration
- Docker & Docker Compose - Containerization

### Database & Storage
- PostgreSQL 15 - Data warehouse
- CSV files - Raw data storage

### Data Visualization
- Tableau Public - Analytics dashboard

### Programming Languages
- Python 3.11 - DAG and transformation scripts
- SQL - Data transformations and analysis

### Development Tools
- Git - Version control
- GitHub - Repository hosting

## System Requirements

- 8GB RAM (minimum)
- 20GB free disk space
- Docker and Docker Compose installed
- Internet connection for Tableau Public access
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
│   └── sql/
│       ├── init.sql             # Database initialization
│       └── transformations.sql   # Data transformations
├── dashboards/              # Tableau dashboard files
├── docker-compose.yaml      # Docker services configuration
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

The PostgreSQL database 'customer360' contains 9 tables in the public schema:

### Fact Tables
1. **orders**
- order_id (PK)
- customer_id (FK)
- order_date
- amount
- product_category

2. **campaign_costs**
- campaign_id (FK)
- date
- cost
- impressions
- clicks

3. **product_costs**
- product_category (PK)
- cost_date
- unit_cost
- supplier

### Dimension Tables
4. **customers**
- customer_id (PK)
- first_name
- last_name
- email
- signup_date
- region

5. **campaigns**
- campaign_id (PK)
- name
- start_date
- end_date
- channel
- target_audience

6. **dim_date**
- date_id (PK)
- full_date
- year
- quarter
- month
- day_of_month
- day_of_week
- is_weekend

### Analytical Views
7. **customer_lifetime_value**
8. **campaign_performance**
9. **product_profitability**

## Dashboard

The Customer 360 Analytics dashboard is available on Tableau Public:
[Customer 360 Dashboard](https://public.tableau.com/views/Customer360Dashboard_17396532555990/Customer360Dashboard?:language=en-US&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link)

### Dashboard Features
- Complete overview of customer analytics
- Interactive filtering capabilities
- Real-time data visualization
- Integration with PostgreSQL data warehouse
- Daily data refreshes from pipeline
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
