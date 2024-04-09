
from neo4j import GraphDatabase

uri = "neo4j+s://8230265d.databases.neo4j.io"
username = "neo4j"
password = "13tZDG6oNb2kJOKNLUYdP-3P0mAHEX4O9ylU0cQ5m5o"

def create_node(label,attributes):
    cypher_query = f"CREATE (n:{label} {{"
    attributes_list = [f"{key}: ${key}" for key in attributes.keys()]
    cypher_query += ", ".join(attributes_list)
    cypher_query += "}) RETURN n"
    
    return cypher_query

def create_relationship(from_label, from_attributes, to_label, to_attributes, relationship_label, relationship_properties):
    # Construye la consulta para encontrar los nodos de inicio y fin
    match_from = f"MATCH (a:{from_label} {{"
    match_from += ", ".join([f"{key}: ${'a_' + key}" for key in from_attributes.keys()])
    match_from += "})"

    match_to = f"MATCH (b:{to_label} {{"
    match_to += ", ".join([f"{key}: ${'b_' + key}" for key in to_attributes.keys()])
    match_to += "})"
    
    # Construye la consulta para crear la relacion
    create_rel = f"CREATE (a)-[r:{relationship_label} {{"
    if relationship_properties:
        create_rel += ", ".join([f"{key}: ${'r_' + key}" for key in relationship_properties.keys()])
    create_rel += "}]->(b) RETURN a, r, b"
    
    # Combina las partes para formar la consulta Cypher completa
    cypher_query = f"{match_from} {match_to} {create_rel}"

    return cypher_query

    
class Conexion:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Cierra la conexion
        self.driver.close()

    def exec_query_node(self, label, attributes):
        query = create_node(label,attributes)
        with self.driver.session() as session:
            # Ejecuta la consulta dentro de una sesion
            resultado = session.run(query,**attributes)
            for registro in resultado:
                print(registro)
                
    def exec_create_relationship(self, from_label, from_attributes, to_label, to_attributes, relationship_label, relationship_properties=None):
        # Genera la consulta Cypher para crear la relacion
        query = create_relationship(from_label, from_attributes, to_label, to_attributes, relationship_label, relationship_properties)
    
        # Prepara los parametros para la consulta Cypher
        parameters = {}
        for key, value in from_attributes.items():
            parameters[f"a_{key}"] = value
        for key, value in to_attributes.items():
            parameters[f"b_{key}"] = value
        if relationship_properties:
            for key, value in relationship_properties.items():
                parameters[f"r_{key}"] = value

        with self.driver.session() as session:
            # Ejecuta la consulta dentro de una sesion
            resultado = session.run(query, **parameters)
            for registro in resultado:
                print(registro)

conexion = Conexion(uri,username,password)

# Populacion del grafo
conexion.exec_query_node('User',{'name':'Diego','userId':'1'})
conexion.exec_query_node('User',{'name':'Brian','userId':'2'})
conexion.exec_query_node('User',{'name':'Luis','userId':'3'})
conexion.exec_query_node('User',{'name':'Jorge','userId':'4'})
conexion.exec_query_node('User',{'name':'Javier','userId':'5'})

conexion.exec_query_node('Movie', {'title': 'Pulp Fiction', 'movieId': 1, 'year': 1994, 'plot': 'The lives of two mob hitmen, a boxer, a gangster and his wife, and a pair of diner bandits intertwine in four tales of violence and redemption.'})
conexion.exec_query_node('Movie', {'title': 'The Matrix', 'movieId': 2, 'year': 1999, 'plot': 'A computer hacker learns from mysterious rebels about the true nature of his reality and his role in the war against its controllers.'})
conexion.exec_query_node('Movie', {'title': 'Inception', 'movieId': 3, 'year': 2010, 'plot': 'A thief who steals corporate secrets through the use of dream-sharing technology is given the inverse task of planting an idea into the mind of a CEO.'})
conexion.exec_query_node('Movie', {'title': 'Interstellar', 'movieId': 4, 'year': 2014, 'plot': 'A team of explorers travel through a wormhole in space in an attempt to ensure humanity\'s survival.'})
conexion.exec_query_node('Movie', {'title': 'The Shawshank Redemption', 'movieId': 5, 'year': 1994, 'plot': 'Two imprisoned men bond over a number of years, finding solace and eventual redemption through acts of common decency.'})

conexion.exec_create_relationship('User', {'userId':'1'}, 'Movie', {'movieId':1}, 'RATED', {'rating':10,'timestamp':1617638223})
conexion.exec_create_relationship('User', {'userId':'1'}, 'Movie', {'movieId':2}, 'RATED', {'rating':5,'timestamp':1917638223})
conexion.exec_create_relationship('User', {'userId':'2'}, 'Movie', {'movieId':2}, 'RATED', {'rating':8,'timestamp':1912038221})
conexion.exec_create_relationship('User', {'userId':'2'}, 'Movie', {'movieId':3}, 'RATED', {'rating':4,'timestamp':5912038225})
conexion.exec_create_relationship('User', {'userId':'3'}, 'Movie', {'movieId':1}, 'RATED', {'rating':9,'timestamp':5112238287})
conexion.exec_create_relationship('User', {'userId':'3'}, 'Movie', {'movieId':4}, 'RATED', {'rating':7,'timestamp':9102231287})
conexion.exec_create_relationship('User', {'userId':'4'}, 'Movie', {'movieId':4}, 'RATED', {'rating':6,'timestamp':1102321237})
conexion.exec_create_relationship('User', {'userId':'4'}, 'Movie', {'movieId':2}, 'RATED', {'rating':4,'timestamp':7702901231})
conexion.exec_create_relationship('User', {'userId':'5'}, 'Movie', {'movieId':1}, 'RATED', {'rating':3,'timestamp':1722302233})
conexion.exec_create_relationship('User', {'userId':'5'}, 'Movie', {'movieId':3}, 'RATED', {'rating':7,'timestamp':9234455213})


conexion.close()