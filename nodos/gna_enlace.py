import psycopg2
from psycopg2.extras import RealDictCursor
import numpy as np
import os
from dotenv import load_dotenv
from scipy.spatial import ConvexHull

# Load environment variables
load_dotenv()

def connect_to_db():
    """Create and return a new connection to the database."""
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )

def get_all_node_positions(cursor):
    """Fetch positions for all nodes in the topology."""
    cursor.execute("""
        SELECT id_node, ST_X(point) AS lon, ST_Y(point) AS lat FROM nodes;
    """)
    return cursor.fetchall()

def calculate_centroid(nodes):
    """Calculate the geometric centroid of a set of nodes."""
    points = np.array([[node['lon'], node['lat']] for node in nodes])
    if len(points) > 2 and len(np.unique(points, axis=0)) >= 3:
        hull = ConvexHull(points)
        centroid = np.mean(points[hull.vertices, :], axis=0)
    else:
        centroid = np.mean(points, axis=0)
    return centroid

def insert_new_node(cursor, lat, lon, topology_id=1):
    """Insert a new node into the database with the calculated position."""
    cursor.execute("""
        INSERT INTO new_nodes (id_topology, lat, lon, point)
        VALUES (%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
        RETURNING id_node;
    """, (topology_id, lat, lon, lon, lat))
    return cursor.fetchone()['id_node']

def insert_new_link(cursor, source_node_id, new_node_id, topology_id=1):
    """Insert a new link between two nodes in the database."""
    cursor.execute("""
        INSERT INTO new_links (id_topology, source, target, geom)
        VALUES (%s, %s, %s, ST_MakeLine(
            (SELECT point FROM nodes WHERE id_node = %s),
            (SELECT point FROM new_nodes WHERE id_node = %s)
        ));
    """, (topology_id, source_node_id, new_node_id, source_node_id, new_node_id))

import traceback

def main():
    conn = connect_to_db()
    target_node_id = 1  # El ID del nodo con el que quieres conectar el nuevo nodo
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            nodes = get_all_node_positions(cursor)
            if nodes:
                print("Calculating the centroid of the network.")
                centroid = calculate_centroid(nodes)
                new_node_id = insert_new_node(cursor, centroid[1], centroid[0])
                conn.commit()
                print("New node added at centroid latitude:", centroid[1], "longitude:", centroid[0], "with ID:", new_node_id)

                # Inserta el enlace entre el nuevo nodo y un nodo existente (ej., nodo 1)
                insert_new_link(cursor, target_node_id, new_node_id)
                conn.commit()
                print(f"Link created between node {target_node_id} and new node {new_node_id}.")
            else:
                print("No nodes found in the topology.")
    except Exception as e:
        print("An error occurred:", e)
        traceback.print_exc()
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
