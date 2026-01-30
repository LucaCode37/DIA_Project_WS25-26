#!/bin/bash
# Komplett-Automation für alle Tasks im Docker-Setup
# Voraussetzung: docker-compose.yaml und alle relevanten Dateien liegen im aktuellen Verzeichnis

set -e

echo "[1/6] Baue Images und starte Container ..."
docker-compose up -d --build
#sleep 10

echo "[2/6] Task 1.1: Star-Schema in Postgres anlegen ..."
#docker exec dia_project bash -c "psql -U dia_user -d db_berlin -f /sql/task_1_star_schema.sql"

echo "[3/6] Task 1.2: Datenbank-Ingest ..."
#docker exec spark-master bash -c "cd /opt/spark-apps && python ingest_timetables.py --week-dir /opt/data/timetables --station-json /opt/data/station_data.json"
#docker exec spark-master bash -c "cd /opt/spark-apps && python ingest_changes.py --week-dir /opt/data/timetable_changes"

echo "[4/6] Task 2: SQL-Analysen ..."
#docker exec dia_project bash -c "psql -U dia_user -d db_berlin -f /sql/task_2_data_analysis.sql"

echo "[5/6] Task 3: Spark/Python-Analysen ..."
docker exec spark-master bash -c "cd /opt/spark-apps && python3 pipeline.py"
#analysis delay and peak hours
docker exec spark-master bash -c "cd /opt/spark-apps && python3 queries.py"

# Optional: Task 4
if docker exec spark-master bash -c "test -f /opt/spark-apps/task_4_graph_based_analytics.py"; then
  echo "[6/6] Task 4: Graphbasierte Analyse ..."
  docker exec spark-master bash -c "cd /opt/spark-apps && python3 task_4_graph_based_analytics.py"
else
  echo "[6/6] Task 4: Kein graphbasiertes Analyseskript gefunden, überspringe."
fi

echo "Alle Tasks wurden automatisiert ausgeführt!"
