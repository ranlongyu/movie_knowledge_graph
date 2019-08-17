from neo4j.v1 import GraphDatabase
from mongodb_operation import database as mongodb

movie_collection = mongodb('kg', 'movie')
movies = movie_collection.get_all_movie()

people_collection = mongodb('kg', 'people')
people = people_collection.get_all_people()

# 运行之前先启动数据库 sudo neo4j start
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "123456"))

def add_movies(tx, movie):
    tx.run("MERGE (a:movie_id {movie_id: $movie_id }) "
           "MERGE (b:movie_key {movie_key: $movie_key })"
           "MERGE (a)-[:Has_key]->(b)",
           movie_id=movie['movie_id'], movie_key=movie['movie_key'])
    if movie['movie_name'] is not None:
        tx.run("MATCH (a:movie_id {movie_id: $movie_id})"
               "MERGE (b:movie_name {movie_name: $movie_name })"
               "MERGE (a)-[:Has_name]->(b)",
               movie_id=movie['movie_id'], movie_name=movie['movie_name'])
    if movie['movie_year'] is not None:
        tx.run("MATCH (a:movie_id {movie_id: $movie_id})"
               "MERGE (b:movie_year {movie_year: $movie_year })"
               "MERGE (a)-[:Has_release_year]->(b)",
               movie_id=movie['movie_id'], movie_year=movie['movie_year'])
    if movie['movie_rating'] is not None:
        tx.run("MATCH (a:movie_id {movie_id: $movie_id})"
               "MERGE (b:movie_rating {movie_rating: $movie_rating })"
               "MERGE (a)-[:Has_rating]->(b)",
               movie_id=movie['movie_id'], movie_rating=movie['movie_rating'])
    if movie['rating_count'] is not None:
        tx.run("MATCH (a:movie_id {movie_id: $movie_id})"
               "MERGE (b:rating_count {rating_count: $rating_count })"
               "MERGE (a)-[:Has_rating_count]->(b)",
               movie_id=movie['movie_id'], rating_count=movie['rating_count'])
    if movie['movie_budget'] is not None:
        tx.run("MATCH (a:movie_id {movie_id: $movie_id})"
               "MERGE (b:movie_budget {movie_budget: $movie_budget })"
               "MERGE (a)-[:Has_budget]->(b)",
               movie_id=movie['movie_id'], movie_budget=movie['movie_budget'])
    if movie['movie_time'] is not None:
        tx.run("MATCH (a:movie_id {movie_id: $movie_id})"
               "MERGE (b:movie_time {movie_time: $movie_time })"
               "MERGE (a)-[:Has_movie_time]->(b)",
               movie_id=movie['movie_id'], movie_time=movie['movie_time'])
    if movie['directors_id']:
        for director_id in movie['directors_id']:
            tx.run("MATCH (a:movie_id {movie_id: $movie_id})"
                   "MERGE (b:person_id {person_id: $director_id})"
                   "MERGE (a)-[:Has_director]->(b)",
                   movie_id=movie['movie_id'], director_id=director_id)
    if movie['writers_id']:
        for writer_id in movie['writers_id']:
            tx.run("MATCH (a:movie_id {movie_id: $movie_id})"
                   "MERGE (b:person_id {person_id: $writer_id})"
                   "MERGE (a)-[:Has_writer]->(b)",
                   movie_id=movie['movie_id'], writer_id=writer_id)
    if movie['actors_id']:
        for actor_id in movie['actors_id']:
            tx.run("MATCH (a:movie_id {movie_id: $movie_id})"
                   "MERGE (b:person_id {person_id: $actor_id})"
                   "MERGE (a)-[:Has_actor]->(b)",
                   movie_id=movie['movie_id'], actor_id=actor_id)
    if movie['movie_types']:
        for movie_type in movie['movie_types']:
            tx.run("MATCH (a:movie_id {movie_id: $movie_id})"
                   "MERGE (b:movie_type {movie_type: $movie_type})"
                   "MERGE (a)-[:Has_type]->(b)",
                   movie_id=movie['movie_id'], movie_type=movie_type)
    if movie['movie_countries']:
        for movie_country in movie['movie_countries']:
            tx.run("MATCH (a:movie_id {movie_id: $movie_id})"
                   "MERGE (b:movie_country {movie_country: $movie_country})"
                   "MERGE (a)-[:Has_country]->(b)",
                   movie_id=movie['movie_id'], movie_country=movie_country)
    if movie['movie_languages']:
        for movie_language in movie['movie_languages']:
            tx.run("MATCH (a:movie_id {movie_id: $movie_id})"
                   "MERGE (b:movie_language {movie_language: $movie_language})"
                   "MERGE (a)-[:Has_language]->(b)",
                   movie_id=movie['movie_id'], movie_language=movie_language)
    if movie['companies_id']:
        for company_id in movie['companies_id']:
            tx.run("MATCH (a:movie_id {movie_id: $movie_id})"
                   "MERGE (b:company_id {company_id: $company_id})"
                   "MERGE (a)-[:Has_company]->(b)",
                   movie_id=movie['movie_id'], company_id=company_id)

def add_people(tx, person):
    tx.run("MATCH (a:person_id {person_id: $person_id})"
           "MERGE (b:person_name {person_name: $person_name})"
           "MERGE (a)-[:Has_name]->(b)",
           person_id=person['person_id'], person_name=person['person_name'])
    if person['person_birthday']:
        tx.run("MATCH (a:person_id {person_id: $person_id})"
               "MERGE (b:person_birthday {person_birthday: $person_birthday})"
               "MERGE (a)-[:Has_birthday]->(b)",
               person_id=person['person_id'], person_birthday=person['person_birthday'])
    if 'person_birth_place' in person:
        if person['person_birth_place']:
            tx.run("MATCH (a:person_id {person_id: $person_id})"
                   "MERGE (b:person_birth_place {person_birth_place: $person_birth_place})"
                   "MERGE (a)-[:Has_birth_place]->(b)",
                   person_id=person['person_id'], person_birth_place=person['person_birth_place'])

def add_friends(tx, name, friend_name):
    tx.run("MERGE (a:Person {name: $name}) "
           "MERGE (a)-[:KNOWS]->(friend:Person {name: $friend_name})",
           name=name, friend_name=friend_name)

def clear(tx):
    tx.run("MATCH (n)"
           "DETACH DELETE n")

with driver.session() as session:
    session.write_transaction(clear)
    for movie in movies:
        session.write_transaction(add_movies, movie)
    for person in people:
        session.write_transaction(add_people, person)