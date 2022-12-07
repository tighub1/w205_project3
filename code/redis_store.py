import redis
import numpy as np
import pandas as pd
from redisgraph import Node, Edge, Graph
import argparse


def get_paths(rgraph, source_node_id):
    res = rgraph.query("MATCH (s:station)-[e:edge]->(c:station) WHERE s.id = " +
                       str(source_node_id) + " RETURN c.id, e.w").result_set
    return res


def get_stations(rgraph):
    return np.ravel(rgraph.query("MATCH (s:station) RETURN s.id").result_set)


def get_route_with_travel_time(rgraph, start_station, end_station, travel_times):
    travel_path = [end_station]
    while travel_path[0] != start_station:
        for station_id, station_time in get_paths(rgraph, travel_path[0]):
            if travel_times[travel_path[0]] - station_time == travel_times[station_id]:
                travel_path.insert(0, station_id)
                break
    return travel_path

# Used https://gitlab.com/ih.markevych/redis-dijkstra-shortestpath/-/blob/master/Dijkstra.py#L43
# as a guide for this function for testing
def get_train_route(rgraph, start_station, end_station):
    costs = {}
    selected_stations = set(get_stations(rgraph))

    for i in selected_stations:
        costs[i] = float('inf')

    costs[start_station] = 0
    while len(selected_stations) > 0:
        start_station = min([costs[station_curr] for station_curr in selected_stations])
        start_travel_time = costs[start_station]

        if start_station == end_station:
            break

        connected_stations = get_paths(rgraph, start_station)

        for station_id, station_travel_time in connected_stations:
            if costs[station_id] > start_travel_time + station_travel_time:
                costs[station_id] = start_travel_time + station_travel_time

        selected_stations.remove(start_station)

    final_cost = costs[end_station]
    path = get_route_with_travel_time(rgraph, start_station, end_station, costs)

    return path, final_cost


def modify_edge_weights(rgraph, start_node, end_node, adjusted_weight):
    rgraph.query(
        "MATCH (s:station {id:" + str(start_node) + "})-[e:edge]->(c:station {id:" + str(end_node) + "}) SET e.w=" +
        str(adjusted_weight)
    )
    rgraph.query(
        "MATCH (s:station {id:" + str(end_node) + "})-[e:edge]->(c:station {id:" + str(start_node) + "}) SET e.w=" +
        str(adjusted_weight)
    )


def get_arguments():
    """
    Getting arguments set at run-time.
    """
    parser = argparse.ArgumentParser(description='Redis Module')

    parser.add_argument(
        '-r', '--run-mode', type=str, default="traffic",
        help="traffic or init mode")

    return parser.parse_args()


if __name__ == '__main__':
    args = get_arguments()
    travel_times_df = pd.read_csv('../data/travel_times.csv')
    stations = sorted(set(travel_times_df.station_1.values.tolist() + travel_times_df.station_2.values.tolist()))
    station_dict = {j: i for i, j in enumerate(stations)}
    inv_station_dict = {v: k for k, v in sorted(station_dict.items())}

    r = redis.Redis(host='localhost', port=6379)

    if args.run_mode == "init":
        r.flushdb()
        redis_graph = Graph('acmefoods', r)
        station_node_dict = {}

        for station in station_dict.keys():
            station_node = Node(label='station', properties={'id': station_dict[station], 'name': station})
            station_node_dict[station] = station_node
            redis_graph.add_node(station_node)

        for index, row in travel_times_df.iterrows():
            station_1 = row.station_1
            station_2 = row.station_2
            travel_time = int(row.travel_time)
            station_1_node = station_node_dict[station_1]
            station_2_node = station_node_dict[station_2]
            edge_1 = Edge(station_1_node, 'edge', station_2_node, properties={'w': travel_time})
            redis_graph.add_edge(edge_1)
            edge_2 = Edge(station_2_node, 'edge', station_1_node, properties={'w': travel_time})
            redis_graph.add_edge(edge_2)

        redis_graph.commit()
    elif args.run_mode == "traffic":
        redis_graph = Graph('acmefoods', r)
        for index, row in travel_times_df.iterrows():
            station_1 = row.station_1
            station_2 = row.station_2
            travel_time = int(row.travel_time)
            modify_edge_weights(
                redis_graph,
                station_dict[station_1],
                station_dict[station_2],
                int(np.random.normal(travel_time, travel_time/10, 1)[0])
            )
    else:
        raise Exception("Pick valid run mode")
