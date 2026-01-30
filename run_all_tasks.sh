set -e

echo "Task 1.1: Star-Schema"
docker exec dia_project bash -c "psql -U dia_user -d db_berlin -f /sql/task_1_star_schema.sql"

#If once builded, comment out the following two lines to avoid re-ingesting data (TIME)
echo "Task 1.2: Ingest Data"
docker exec spark-master bash -c "cd /opt/spark-apps && python3 ingest_timetables.py --week-dir /opt/data/timetables --station-json /opt/data/station_data.json"

docker exec spark-master bash -c "cd /opt/spark-apps && python3 ingest_changes.py --week-dir /opt/data/timetable_changes"


#Task 2: SQL Data Analysis/ For more examples see sql/task_2_data_analysis.sql
echo "Task 2: SQL-Analysen"
docker exec dia_project bash -c "psql -U dia_user -d db_berlin -f /sql/task_2_data_analysis.sql"




echo "Task 4: Graph-based analytics"

#Task 4: Graph-based analytics
docker exec spark-master bash -c "cd /opt/spark-apps && python3 task_4_graph_based_analytics.py"

echo "Task 1, Task 2 and Task 4 completed"
