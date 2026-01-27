"""
Task 4: Graph Analytics for Routing

Implements two routing algorithms:
- Task 4.1: Shortest path by station hops (BFS)
- Task 4.2: Earliest arrival time with timetable (Dijkstra)

Note: Station hops count, not transfers. Station variants are normalized.
"""

import networkx as nx
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Set
import heapq
from collections import defaultdict
from postgres_connector import PostgresConnector

# Station Name Normalization

def normalize_station_name(name: str) -> str:
    """Normalize station names to handle variants like (S), (U) suffixes and Berlin- prefix."""
    # Remove trailing type suffixes
    name = name.replace(" (S)", "").replace(" (U)", "").replace(" (S+U)", "")
    name = name.replace("(S)", "").replace("(U)", "").replace("(S+U)", "")
    
    if name.startswith("Berlin-"):
        name = "Berlin " + name[7:]
    
    aliases = {
        "Berlin Zoo": "Berlin Zoologischer Garten",
        "Zoo": "Berlin Zoologischer Garten"
    }
    
    return aliases.get(name, name).strip()


# Create Graph 

def build_static_graph() -> nx.DiGraph:
    """Build network graph from planned paths in fact_train_movement."""
    
    connector = PostgresConnector()
    conn = connector.connect()
    cursor = conn.cursor()
    
    # Fetch distinct planned paths for departure events
    cursor.execute("""
        SELECT DISTINCT planned_path
        FROM fact_train_movement
        WHERE event_type = 'D' 
          AND planned_path IS NOT NULL
          AND planned_path != ''
    """)
    
    # Build directed graph
    G = nx.DiGraph()
    edge_set = set()
    
    for (path_str,) in cursor:
        if not path_str:
            continue
        
        stations = [normalize_station_name(s) for s in path_str.split('|') if s.strip()]
        
        for station in stations:
            if station:
                G.add_node(station)
        
        for i in range(len(stations) - 1):
            source = stations[i]
            target = stations[i + 1]
            
            if source and target and source != target:
                edge_key = (source, target)
                if edge_key not in edge_set:
                    G.add_edge(source, target)
                    edge_set.add(edge_key)
    
    cursor.close()
    connector.close()
    
    print(f"Graph built: {G.number_of_nodes()} stations, {G.number_of_edges()} connections")
    return G

#Task 4.1 Shortest path by station hops

def find_shortest_path(G: nx.DiGraph, source: str, target: str) -> Optional[Tuple[List[str], int]]:
    """Find shortest path using BFS. Returns (path, hop_count) or None."""
    source_norm = normalize_station_name(source)
    target_norm = normalize_station_name(target)
    
    def find_node(search_name: str) -> Optional[str]:
        if search_name in G.nodes():
            return search_name
        
        candidates = [n for n in G.nodes() if search_name.lower() in n.lower()]
        
        if len(candidates) == 0:
            return None
        elif len(candidates) == 1:
            return candidates[0]
        else:
            exact_end = [c for c in candidates if c.lower().endswith(search_name.lower())]
            if exact_end:
                return exact_end[0]
            return candidates[0]
    
    source_node = find_node(source_norm)
    target_node = find_node(target_norm)
    
    if not source_node:
        print(f"  Source station '{source}' (normalized: '{source_norm}') not found in graph")
        return None
    
    if not target_node:
        print(f"  Target station '{target}' (normalized: '{target_norm}') not found in graph")
        return None
    
    try:
        path = nx.shortest_path(G, source_node, target_node)
        hops = len(path) - 1
        return (path, hops)
    except nx.NetworkXNoPath:
        print(f"  No path exists between {source_node} and {target_node}")
        return None

# Create Timetable Graph

def build_timetable_graph(snapshot_date: str = "2025-09-02") -> Dict:
    """Build timetable graph from arrival/departure events."""
    print(f"Building Timetable Graph for {snapshot_date}")
    
    
    connector = PostgresConnector()
    conn = connector.connect()
    cursor = conn.cursor()
    
    # Fetch train movements for the given date
    cursor.execute("""
        SELECT 
            f.train_key,
            s.station_name,
            f.event_type,
            t.ts,
            f.stop_id
        FROM fact_train_movement f
        JOIN dim_station s ON f.station_key = s.station_key
        JOIN dim_time t ON f.planned_time_key = t.time_key
        WHERE t.date = %s
          AND f.event_type IN ('A', 'D')
          AND f.is_cancelled = false
        ORDER BY f.train_key, t.ts, f.event_type
    """, (snapshot_date,))
    
    movements = cursor.fetchall()
    cursor.close()
    connector.close()
    
    print(f"Loaded {len(movements)} movements")
    
    trains = defaultdict(list)
    for train_key, station, event_type, ts, stop_id in movements:
        trains[train_key].append({
            'station': normalize_station_name(station),
            'event_type': event_type,
            'time': ts.replace(tzinfo=timezone.utc),
            'stop_id': stop_id
        })
    
    connections = []
    
    for train_key, stops in trains.items():
        stops.sort(key=lambda x: x['time'])
        
        for i in range(len(stops) - 1):
            current = stops[i]
            next_stop = stops[i + 1]
            
            if current['event_type'] == 'D' and next_stop['event_type'] == 'A':
                connections.append({
                    'from_station': current['station'],
                    'to_station': next_stop['station'],
                    'departure_time': current['time'],
                    'arrival_time': next_stop['time'],
                    'train_id': train_key
                })
    
    print(f"Created {len(connections)} train connections from {len(trains)} trains")
    
    return {
        'connections': connections,
        'date': snapshot_date
    }

# Task 4.2 Earliest arrival time with timetable

def earliest_arrival_time(
    timetable: Dict,
    source: str,
    target: str,
    departure_time: datetime
) -> Optional[Tuple[datetime, List[Dict]]]:
    """Find earliest arrival using Dijkstra. Allows transfers without wait time."""
    source_norm = normalize_station_name(source)
    target_norm = normalize_station_name(target)
    
    outgoing = defaultdict(list)
    all_stations = set()
    
    for conn in timetable['connections']:
        outgoing[conn['from_station']].append(conn)
        all_stations.add(conn['from_station'])
        all_stations.add(conn['to_station'])
    
    
    def find_station(search: str) -> Optional[str]:
        if search in all_stations:
            return search
        candidates = [s for s in all_stations if search.lower() in s.lower()]
        if len(candidates) == 0:
            return None
        elif len(candidates) == 1:
            return candidates[0]
        else:
            exact = [c for c in candidates if c.lower().endswith(search.lower())]
            return exact[0] if exact else candidates[0]
    
    source_station = find_station(source_norm)
    target_station = find_station(target_norm)
    
    if not source_station:
        print(f"  Source '{source}' not found in timetable")
        return None
    
    if not target_station:
        print(f"  Target '{target}' not found in timetable")
        return None
    
    counter = 0
    pq = [(departure_time, counter, source_station, [], None)]
    counter += 1
    visited = {}
    
    # Dijkstra's Algorithm
    while pq:
        current_time, _, current_station, path, current_train = heapq.heappop(pq)
        
        if current_station == target_station:
            return (current_time, path)
        
        if current_station in visited and visited[current_station] <= current_time:
            continue
        visited[current_station] = current_time
        
        
        for conn in outgoing[current_station]:
            if conn['departure_time'] >= current_time:
                new_arrival = conn['arrival_time']
                new_path = path + [conn]
                new_train = conn['train_id']
                
                if conn['to_station'] not in visited or visited[conn['to_station']] > new_arrival:
                    heapq.heappush(pq, (new_arrival, counter, conn['to_station'], new_path, new_train))
                    counter += 1
    
    return None

# Demo Functions (can change test_routes, for more example)

def task_4_1_demo():
    print("\n")
    print("TASK 4.1: SHORTEST PATH")
    print("\n")
    
    
    G = build_static_graph()
    
    test_routes = [
        ("Berlin Alexanderplatz", "Berlin-Spandau"),
        ("Berlin Ostbahnhof", "Berlin Zoo"),
        ("Berlin Friedrichstraße", "Berlin Südkreuz"),
        ("Berlin Hbf", "Berlin Ostkreuz"),
        ("Alexanderplatz", "Westkreuz"),
        ("Berlin-Neukölln", "Berlin Gesundbrunnen"),
        ("Berlin-Schöneberg", "Berlin-Wedding"),
        ("Berlin-Tempelhof", "Berlin Charlottenburg"),
        ("Nürnberg Hbf", "München Hbf"),  # Works: south direction
        ("München Hbf", "Nürnberg Hbf"),  # Fails: no north direction in data (diGraph)
    ]
    
    for source, target in test_routes:
        result = find_shortest_path(G, source, target)
        
        if result:
            path, hops = result
            print(f"Shortest path ({hops} hops):")
            print(f"    {' → '.join(path)}")
        else:
            print(f"No path found")


def task_4_2_demo():
    print("\n")
    print("TASK 4.2: EARLIEST ARRIVAL TIME")
    print("\n")
    
    timetable = build_timetable_graph("2025-09-02")
    
    departure = datetime(2025, 9, 2, 12, 0, tzinfo=timezone.utc)
    
    test_routes = [
        ("Berlin Ostbahnhof", "Berlin Zoologischer Garten"),
        ("Berlin Hauptbahnhof", "Berlin Ostkreuz"),
        ("Alexanderplatz", "Berlin Spandau"),
        ("Berlin-Spandau", "Berlin Ostbahnhof"),
        ("Berlin Südkreuz", "Berlin Gesundbrunnen"),
        ("Berlin Friedrichstraße", "Berlin Charlottenburg"),
        ("Berlin Ostbahnhof", "Berlin Südkreuz"),
        ("Alexanderplatz", "Berlin Gesundbrunnen"),
    ]
    
    for source, target in test_routes:
        print(f"\nRoute: {source} → {target}")
        print(f"Departure: {departure.strftime('%Y-%m-%d %H:%M')}")
        print()
        result = earliest_arrival_time(timetable, source, target, departure)
        
        if result:
            arrival, segments = result
            duration = (arrival - departure).total_seconds() / 60
            
            print(f" Arrival: {arrival.strftime('%H:%M')} (Duration: {duration:.0f} min)")
            print(f" Segments: {len(segments)} trains, {len(set(s['to_station'] for s in segments))} stations")
            
            for i, seg in enumerate(segments, 1):
                dep_time = seg['departure_time'].strftime('%H:%M')
                arr_time = seg['arrival_time'].strftime('%H:%M')
                print(f"      {i}. {seg['from_station']} ({dep_time}) → {seg['to_station']} ({arr_time})")
        else:
            print(f"No route found")

# Run Demos

if __name__ == "__main__":
    task_4_1_demo()
    task_4_2_demo()

