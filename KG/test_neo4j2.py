from py2neo import Graph
graph = Graph("http://localhost:7474", username="neo4j", password="123456")
graph.delete_all()