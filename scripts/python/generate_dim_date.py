import csv
import datetime

START_DATE = datetime.date(2020, 1, 1)
END_DATE   = datetime.date(2026, 1, 1)  # generate up to end of 2025

def main():
    with open('../data/dim_date.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            "date_key", "full_date", "year", "quarter",
            "month", "day_of_month", "day_of_week", 
            "is_weekend"
        ])

        current_date = START_DATE
        while current_date < END_DATE:
            date_key = current_date.strftime("%Y%m%d")
            year = current_date.year
            quarter = (current_date.month - 1)//3 + 1
            month = current_date.month
            day_of_month = current_date.day
            day_of_week = current_date.weekday()  # 0=Monday, 6=Sunday
            is_weekend = 1 if day_of_week >= 5 else 0

            writer.writerow([
                date_key,
                current_date.isoformat(),
                year, quarter,
                month, day_of_month,
                day_of_week,
                is_weekend
            ])
            current_date += datetime.timedelta(days=1)

if __name__ == "__main__":
    main()
    print("dim_date.csv generated!")

