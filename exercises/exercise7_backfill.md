# Exercise 7 - Backfill

- Use a previously created DAG and use backfill to trigger runs before the start date
- Backfilling is possible via:
1. Run `docker compose ps` and find the name of the scheduler
2. Get into the scheduler via: 
    ```sh
    docker exec -it <scheduler name> /bin/bash
    ```
3. Run:
   `airflow dags backfill -s <start date> -e <end date> <dag_name>`
4. Check the DAG