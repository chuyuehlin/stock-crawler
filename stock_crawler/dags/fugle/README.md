# Web Scrawler using Airflow

There are 2 DAG files to get stock data from Fugle API.

## Installation

We are using Airflow 2.1.0, you can install it by running the following code.

```bash
python3 -m pip install apache-airflow=2.1.0
```
After installation, run the following code to start.

```bash
airflow webserver -p 8080
airflow scheduler
```
Then, go to the following url, Airflow UI interface will show up. 

```bash
localhost:8080
```


