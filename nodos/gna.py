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
        SELECT ST_X(point) AS lon, ST_Y(point) AS lat FROM nodes;
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
        VALUES (%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326));
    """, (topology_id, lat, lon, lon, lat))

def main():
    conn = connect_to_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            nodes = get_all_node_positions(cursor)
            if nodes:
                print("Calculating the centroid of the network.")
                centroid = calculate_centroid(nodes)
                insert_new_node(cursor, centroid[1], centroid[0])
                conn.commit()
                print("New node added at centroid latitude:", centroid[1], "longitude:", centroid[0])
            else:
                print("No nodes found in the topology.")
    except Exception as e:
        print("An error occurred:", e)
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
