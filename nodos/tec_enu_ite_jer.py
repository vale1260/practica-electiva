import psycopg2
from psycopg2.extras import RealDictCursor
import numpy as np
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def connect_to_db():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )

def get_all_node_positions(cursor):
    cursor.execute("SELECT id_node, ST_X(point) AS lon, ST_Y(point) AS lat FROM nodes;")
    return cursor.fetchall()

def get_specific_node_positions(cursor, node_ids):
    node_ids_tuple = tuple(node_ids)
    query = """
        SELECT id_node, ST_X(point) AS lon, ST_Y(point) AS lat FROM nodes
        WHERE id_node IN %s;
    """
    cursor.execute(query, (node_ids_tuple,))
    return cursor.fetchall()

def calculate_centroid(points):
    centroid = np.mean(points, axis=0)
    return {'lon': centroid[0], 'lat': centroid[1]}

def create_subgroups(nodes, num_groups=2):
    np.random.shuffle(nodes)
    return np.array_split(nodes, num_groups)

def insert_new_node(cursor, lat, lon, topology_id=1):
    cursor.execute("""
        INSERT INTO new_nodes (id_topology, lat, lon, point)
        VALUES (%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
        RETURNING id_node;
    """, (topology_id, lat, lon, lon, lat))
    return cursor.fetchone()['id_node']

def insert_new_link(cursor, source_node_id, target_node_id, topology_id=1):
    cursor.execute("""
        INSERT INTO new_links (id_topology, source_topology, source, target, distance, geom)
        VALUES (%s, %s, %s, %s, ST_Distance(
            (SELECT point FROM nodes WHERE id_node = %s),
            (SELECT point FROM new_nodes WHERE id_node = %s)
        ), ST_MakeLine(
            (SELECT point FROM nodes WHERE id_node = %s),
            (SELECT point FROM new_nodes WHERE id_node = %s)
        ));
    """, (topology_id, topology_id, source_node_id, target_node_id, source_node_id, target_node_id, source_node_id, target_node_id))

def calculate_distance(node1, node2):
    return np.sqrt((node1['lon'] - node2['lon'])**2 + (node1['lat'] - node2['lat'])**2)

def optimize_links(cursor, new_node_id, nearby_nodes, num_iterations=10):
    best_links = []
    best_score = float('inf')
    
    new_node_position = get_node_position(cursor, new_node_id)
    
    for _ in range(num_iterations):
        np.random.shuffle(nearby_nodes)
        selected_links = nearby_nodes[:5]
        
        for link in selected_links:
            link['distance'] = calculate_distance(new_node_position, link)
        
        score = evaluate_links(selected_links)
        
        if score < best_score:
            best_score = score
            best_links = selected_links
    
    created_links = []
    for node in best_links:
        insert_new_link(cursor, node['id_node'], new_node_id)
        created_links.append((node['id_node'], new_node_id))
    
    return created_links

def evaluate_links(links):
    total_distance = sum(link['distance'] for link in links)
    return total_distance

def get_node_position(cursor, node_id):
    cursor.execute("""
        SELECT ST_X(point) AS lon, ST_Y(point) AS lat FROM new_nodes
        WHERE id_node = %s;
    """, (node_id,))
    return cursor.fetchone()

def main():
    conn = connect_to_db()
    specific_node_ids = [1,2,3]
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            all_nodes = get_all_node_positions(cursor)
            subgroups = create_subgroups(all_nodes)

            for group in subgroups:
                points = np.array([[node['lon'], node['lat']] for node in group])
                centroid = calculate_centroid(points)
                new_node_id = insert_new_node(cursor, centroid['lat'], centroid['lon'])
                conn.commit()
                print("Nuevo nodo agregado en latitud:", centroid['lat'], "longitud:", centroid['lon'], "con ID:", new_node_id)

                nearby_nodes = get_specific_node_positions(cursor, specific_node_ids)
                if nearby_nodes:
                    created_links = optimize_links(cursor, new_node_id, nearby_nodes)
                    conn.commit()
                    print("Enlaces creados entre el nuevo nodo y los nodos cercanos:", created_links)
                else:
                    print(f"No se encontraron nodos cercanos para el grupo con centroid en {centroid['lat']}, {centroid['lon']}")

    except Exception as e:
        print("Ocurrió un error:", e)
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main()