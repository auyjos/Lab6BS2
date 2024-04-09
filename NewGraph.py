
from neo4j import GraphDatabase

uri = "neo4j+s://5eb94846.databases.neo4j.io"
username = "neo4j"
password = "8UdvbHSyr8kGH177lyiixQ7bZZvo8-HU5isM5cdGIEY"


def create_node(label, attributes):
    cypher_query = f"CREATE (n:{label} {{"
    attributes_list = [f"{key}: ${key}" for key in attributes.keys()]
    cypher_query += ", ".join(attributes_list)
    cypher_query += "}) RETURN n"

    return cypher_query


def create_relationship(from_label, from_attributes, to_label, to_attributes, relationship_label, relationship_properties):
    # Construye la consulta para encontrar los nodos de inicio y fin
    match_from = f"MATCH (a:{from_label} {{"
    match_from += ", ".join(
        [f"{key}: ${'a_' + key}" for key in from_attributes.keys()])
    match_from += "})"

    match_to = f"MATCH (b:{to_label} {{"
    match_to += ", ".join([f"{key}: ${'b_' + key}" for key in to_attributes.keys()])
    match_to += "})"

    # Construye la consulta para crear la relacion
    create_rel = f"CREATE (a)-[r:{relationship_label} {{"
    if relationship_properties:
        create_rel += ", ".join(
            [f"{key}: ${'r_' + key}" for key in relationship_properties.keys()])
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
        query = create_node(label, attributes)
        with self.driver.session() as session:
            # Ejecuta la consulta dentro de una sesion
            resultado = session.run(query, **attributes)
            for registro in resultado:
                print(registro)

    def exec_create_relationship(self, from_label, from_attributes, to_label, to_attributes, relationship_label, relationship_properties=None):
        # Genera la consulta Cypher para crear la relacion
        query = create_relationship(from_label, from_attributes, to_label,
                                    to_attributes, relationship_label, relationship_properties)

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

    def find_user_movie_rating(self, user_id, movie_id):
        cypher_query = (
            "MATCH (u:User {userId: $user_id})-[r:RATED]->(m:Movie {movieId: $movie_id})<-[:RATED]-(other_user:User) "
            "RETURN u, r, m, other_user"
        )
        with self.driver.session() as session:
            result = session.run(
                cypher_query, user_id=user_id, movie_id=movie_id)
            for record in result:
                user = record['u']
                rating = record['r']
                movie = record['m']
                other_user = record['other_user']
                print(f"Usuario: {user['name']} ({user['userId']})")
                print(f"Película: {movie['title']} ({movie['movieId']})")
                print(f"Calificación: {rating['rating']}")
                print(
                    f"Otro usuario que ha calificado esta película: {other_user['name']} ({other_user['userId']})\n")


conexion = Conexion(uri, username, password)

# Populacion del grafo

# Crear nodos de personas (Actores, Directores y Usuarios)
conexion.exec_query_node('Person', {'name': 'Actor 1', 'tmdbId': 1, 'born': '1990-01-01', 'died': None,
                                    'bornin': 'USA', 'url': 'http://example.com', 'imdbId': 123, 'bio': 'Actor bio', 'poster': 'actor1.jpg'})

conexion.exec_query_node('Person', {'name': 'Actor 2', 'tmdbId': 2, 'born': '1985-03-15', 'died': '2020-05-20',
                                    'bornin': 'UK', 'url': 'http://example.com', 'imdbId': 456, 'bio': 'Actor bio', 'poster': 'actor2.jpg'})
conexion.exec_query_node('Person', {'name': 'Director 1', 'tmdbId': 3, 'born': '1975-11-20', 'died': None,
                                    'bornin': 'France', 'url': 'http://example.com', 'imdbId': 789, 'bio': 'Director bio', 'poster': 'director1.jpg'})
conexion.exec_query_node('User', {'name': 'User 1', 'userId': 1})

# Crear nodos de películas
conexion.exec_query_node('Movie', {'title': 'Movie 1', 'tmdbId': 101, 'bom': '2000-01-01', 'year': 2000, 'imdbId': 111, 'runtime': 120, 'countries': [
    'USA', 'UK'], 'imdbVotes': 1000, 'url': 'http://example.com', 'revenue': 1000000, 'plot': 'Movie plot', 'poster': 'movie1.jpg', 'budget': 500000, 'languages': ['English']})
conexion.exec_query_node('Movie', {'title': 'Movie 2', 'tmdbId': 102, 'bom': '2005-05-05', 'year': 2005, 'imdbId': 222, 'runtime': 150, 'countries': [
    'USA'], 'imdbVotes': 1500, 'url': 'http://example.com', 'revenue': 2000000, 'plot': 'Movie plot', 'poster': 'movie2.jpg', 'budget': 800000, 'languages': ['English', 'French']})

# Crear relaciones de actuación
conexion.exec_create_relationship('Person', {'tmdbId': 1}, 'Movie', {
    'tmdbId': 101}, 'ACTED_IN', {'role': 'Main actor'})
conexion.exec_create_relationship('Person', {'tmdbId': 2}, 'Movie', {
    'tmdbId': 101}, 'ACTED_IN', {'role': 'Supporting actor'})

# Crear relaciones de dirección
conexion.exec_create_relationship('Person', {'tmdbId': 3}, 'Movie', {
    'tmdbId': 101}, 'DIRECTED', None)

# Crear relaciones de calificación
conexion.exec_create_relationship('User', {'userId': 1}, 'Movie', {
    'tmdbId': 101}, 'RATED', {'rating': 4, 'timestamp': 1617638223})
conexion.exec_create_relationship('User', {'userId': 1}, 'Movie', {
    'tmdbId': 102}, 'RATED', {'rating': 5, 'timestamp': 1917638223})


conexion.close()
