DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
    date_key       INT PRIMARY KEY,
    full_date      DATE,
    year           INT,
    quarter        INT,
    month          INT,
    day_of_month   INT,
    day_of_week    INT,
    is_weekend     INT
);

