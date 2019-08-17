from neo4j.v1 import GraphDatabase

# 运行之前先启动数据库 sudo neo4j start
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "123456"))

def add_friends(tx, name, friend_name):
    tx.run("MERGE (a:Person {name: $name}) "
           "MERGE (a)-[:KNOWS]->(friend:Person "
           "{name: $friend_name})",
           name=name, friend_name=friend_name)

def print_friends(tx, name):
    for record in tx.run("MATCH (a:Person)-[:KNOWS]->(friend) WHERE a.name = $name "
                         "RETURN friend.name ORDER BY friend.name", name=name):
        print(record["friend.name"])

def find(tx):
    res = tx.run("MATCH (a:director_id {director_id: $director_id}) "
                 "RETURN a",
                 director_id='tt123')
    if res:
        print('yes')
    for re in res:
        if re:
            print(re)

def clear(tx):
    tx.run("MATCH (n)"
           "DETACH DELETE n")

with driver.session() as session:
    session.read_transaction(find)
    '''
    session.write_transaction(add_friends, "Arthur", "Guinevere")
    session.write_transaction(add_friends, "Arthur", "Lancelot")
    session.write_transaction(add_friends, "Arthur", "Merlin")
    session.read_transaction(print_friends, "Arthur")
    #session.write_transaction(clear)
    '''