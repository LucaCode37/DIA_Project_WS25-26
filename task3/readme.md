To run the code for task 3

1. create a virtual python environment with `python -m venv .venv/`
2. activate the virtual python environment with `source .venv/bin/activate`
3. install required dependencies (PySpark) by running `pip install -r requirements.txt`
4. run the ETL pipeline (task 3.1) with `python pipeline.py`
5. after it's done you can run the analytics (tasks 3.2, 3.3) with `python queries.py`

to deactivate the virtual python environment run `deactivate`

Configuration:

- to set a station name for task 3.2, you can choose one from the stations.txt and set it as the `target_station` in `queries.py`
