# ETL Process for Largest Banks Data

This repository contains a Python project that performs an ETL (Extract, Transform, Load) process on the data of the world's largest banks. The data is extracted from a web page, transformed with currency conversions, and then loaded into both a CSV file and a SQL database.

## Project Overview
The project demonstrates a simple ETL pipeline using real-world data sourced from a Wikipedia page on the largest banks by market capitalization. The steps involved are:

- **Extraction**: The data is scraped from the webpage using BeautifulSoup.
- **Transformation**: The market capitalization is converted into multiple currencies (GBP, EUR, INR) using exchange rates from a CSV file.
- **Loading**: The transformed data is saved into a CSV file and an SQLite database. Queries are run on the database to extract insights.

This project can serve as a reference for implementing ETL pipelines using Python, web scraping, data transformation with pandas, and data loading into a SQL database.

## Table of Contents
- [Technologies](#technologies)
- [Project Structure](#project-structure)
- [Setup](#setup)
- [How to Run the Code](#how-to-run-the-code)
- [Queries and Logs](#queries-and-logs)
- [Future Enhancements](#future-enhancements)

## Technologies
This project was developed with the following technologies:

- Python 
- BeautifulSoup (for web scraping)
- Requests (for HTTP requests)
- Pandas (for data transformation)
- SQLite3 (for database operations)
- NumPy (for numerical operations)

## Project Structure
```
.
├── Banks.db                  # SQLite database created after running the code 
├── Largest_banks_data.csv    # CSV file created after running the code 
├── exchange_rate.csv         # CSV file containing exchange rates 
├── code_log.txt              # Log file tracking ETL process stages 
├── banks_project.py          # Main ETL Python script 
└── README.md                 # Project documentation (this file) 
```

## Setup
### Clone the repository:
```
git clone [https://github.com/FaruqAbdurrahmanF/ETL-Largest-Banks](https://github.com/FaruqAbdurrahmanF/ETL-Largest-Banks)
cd largest-banks-etl
```

## Install the required Python libraries:
Before running the code, make sure you have the necessary Python libraries installed. You can install them using pip:
```
pip install -r requirements.txt
```

## Ensure the `exchange_rate.csv` file exists:
The `exchange_rate.csv` file should contain the exchange rates for converting USD to GBP, EUR, and INR. Below is an example of the CSV format: 
```
Currency,Rate 
GBP,0.8 
EUR,0.93 
INR,82.95
```

## How to Run the Code
### Run the ETL script:
Once you have everything set up, you can run the script:
```
python etl_banks.py
```

## Observe Logs:
During the execution of the ETL process, progress is logged in the `code_log.txt` file. Each stage of the process, from extraction to transformation and loading, is recorded with timestamps.

## Check Outputs:
- The transformed data will be saved into a CSV file named `Largest_banks_data.csv`.
- The data will also be loaded into an SQLite database file named `Banks.db`.

## Queries and Logs
The ETL script includes sample SQL queries that can be used to interact with the loaded data in the SQLite database. These queries include:
- Retrieving all records from the database.
- Calculating the average market capitalization of the banks.
- Listing the names of the top 5 banks.
Each query execution is logged, and the results are printed to the console.

## Future Enhancements
- Adding error handling for different edge cases during the web scraping process.
- Implementing additional transformation steps based on more complex business rules.
- Creating a more sophisticated logging system using Python’s logging library.

## Data Source
Data is extracted from the Wikipedia page: [List of largest banks.](https://en.wikipedia.org/wiki/List_of_largest_banks)
