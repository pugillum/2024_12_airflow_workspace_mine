# 🚀 DeepSpace Technologies

# [CONFIDENTIAL]

You're a rocket scientist (analyst) at DeepSpace Technologies. Your boss assigns you to create a data pipeline using Apache Airflow to track global rocket launches from the last 60 days and future launches via The SpaceDevs API: https://lldev.thespacedevs.com/2.3.0/launch 

The pipeline will collect and process launch data, providing insights on rocket IDs, mission names, statuses, countries, and launch providers.

You need to configure an operator to check for launches scheduled for today. If there are none, the pipeline should stop to avoid processing outdated data. You plan to convert the extracted data into parquet files for efficient access.

Some design considerations:
- How to ensure the API is operational?
- How to pass API responses to the pipeline?
- Is local storage necessary?
- How to request data for a specific date?

Your boss suggests using secure cloud storage for the organised data and integrating the pipeline with BigQuery for advanced analytics. Additionally, she requests a PostgreSQL database for team members who prefer it using a relational database.

Good luck! 
To infinity... and beyond! 🧑🏻‍🚀



