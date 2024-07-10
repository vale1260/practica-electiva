import psycopg2
from psycopg2.extras import RealDictCursor
import numpy as np
import os
from dotenv import load_dotenv
from scipy.optimize import least_squares

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

def get_specific_node_positions(cursor, node_ids):
    """Fetch positions for specific nodes based on their IDs."""
    if not node_ids:
        raise ValueError("The list of node IDs is empty.")
    node_ids_tuple = tuple(node_ids)
    query = """
        SELECT id_node, ST_X(point) AS lon, ST_Y(point) AS lat FROM nodes
        WHERE id_node IN %s;
    """
    cursor.execute(query, (node_ids_tuple,))
    return cursor.fetchall()

def residuals(params, points):
    """Calculate the Euclidean distance from each point to the guess point."""
    x, y = params
    return np.sqrt((points[:, 0] - x)**2 + (points[:, 1] - y)**2)

def calculate_new_node_position(nodes):
    """Calculate a new node position using least squares based on existing node positions."""
    points = np.array([[node['lon'], node['lat']] for node in nodes])
    x0 = np.mean(points, axis=0)
    result = least_squares(residuals, x0, args=(points,))
    return result.x

def insert_new_node(cursor, lat, lon, topology_id=1):
    """Insert a new node into the database with the calculated position."""
    cursor.execute("""
        INSERT INTO new_nodes (id_topology, lat, lon, point)
        VALUES (%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326));
    """, (topology_id, lat, lon, lon, lat))

def insert_new_link(cursor, source_node_id, new_node_id, topology_id=1):
    """Insert a new link between two nodes in the database."""
    cursor.execute("""
        INSERT INTO new_links (id_topology, source, target, geom)
        VALUES (%s, %s, %s, ST_MakeLine(
            (SELECT point FROM nodes WHERE id_node = %s),
            (SELECT point FROM new_nodes WHERE id_node = %s)
        ));
    """, (topology_id, source_node_id, new_node_id, source_node_id, new_node_id))

import traceback  # Importa el módulo traceback

def main():
    """Main function to execute the process."""
    conn = connect_to_db()
    specific_node_ids = [3,4,5]  # Modifica esta lista según sea necesario
    target_node_id = 12  # El ID del nodo con el que quieres conectar el nuevo nodo
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            nodes = get_specific_node_positions(cursor, specific_node_ids)
            if nodes:
                print(f"Calculating the position of the new node based on nodes with IDs {specific_node_ids}.")
                new_position = calculate_new_node_position(nodes)
                insert_new_node(cursor, new_position[1], new_position[0])
                cursor.execute("""
                    SELECT id_node FROM new_nodes WHERE lat = %s AND lon = %s ORDER BY id_node DESC LIMIT 1;
                """, (new_position[1], new_position[0]))
                new_node_id = cursor.fetchone()['id_node']
                conn.commit()
                print("New node added at latitude:", new_position[1], "longitude:", new_position[0], "with ID:", new_node_id)
                
                # Inserta el enlace entre el nuevo nodo y un nodo existente (ej., nodo 12)
                insert_new_link(cursor, target_node_id, new_node_id)
                conn.commit()
                print(f"Link created between node {target_node_id} and new node {new_node_id}.")
            else:
                print("No nodes found with the specified IDs.")
    except Exception as e:
        print("An error occurred:", e)
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main()