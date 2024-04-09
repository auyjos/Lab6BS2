
from neo4j import GraphDatabase

uri = "neo4j+s://8230265d.databases.neo4j.io"
username = "neo4j"
password = "13tZDG6oNb2kJOKNLUYdP-3P0mAHEX4O9ylU0cQ5m5o"

def create_user_node(name,userId):
    cypher_query = f"CREATE (n:User {{"
    cypher_query += f"name:\"{name}\",userId:\"{userId}\""
    cypher_query += "}) RETURN n"
    
    return cypher_query

def create_movie_node(title,movieId,year,plot):
    cypher_query = f"CREATE (n:Movie {{"
    cypher_query += f"title:\"{title}\",movieId:{movieId},year:{year},plot:\"{plot}\""
    cypher_query += "}) RETURN n"
    
    return cypher_query

def create_usermovie_relation(userId,movieId,rating,timestamp):
    cypher_query = f"MATCH (a:User{{userId:\"{userId}\"}}),(b:Movie{{movieId:{movieId}}}) "
    cypher_query += f"CREATE (a)-[r:RATED{{rating:{rating},timestamp:{timestamp}}}]->(b) "
    cypher_query += "RETURN r"
    
    return cypher_query
    
class Conexion:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Cierra la conexion
        self.driver.close()

    def exec_query(self, query):
        with self.driver.session() as session:
            # Ejecuta la consulta dentro de una sesion
            resultado = session.run(query)
            for registro in resultado:
                print(registro)

conexion = Conexion(uri,username,password)
conexion.exec_query(create_usermovie_relation(userId='1',movieId=1,rating=10,timestamp=16))
conexion.close()

# conexion.exec_query(create_user_node(name='Diego',userId='1'))
# conexion.exec_query(create_movie_node(title='Batman',movieId=1,year=2008,plot='Batman Arkham Knight'))
# conexion.exec_query(create_usermovie_relation(userId='1',movieId=1,rating=10,timestamp=16))