## mongodb数据库

数据库名"kg"

集合名"movie"，存电影信息，字段：
单个：'movie_id','movie_key','movie_name','movie_year','movie_rating','rating_count','movie_budget','movie_time','movie_poster_link','summary_text'.
列表：'directors_id','writers_id','actors_id','movie_types','movie_countries','movie_languages','companies_id'.

集合名"people"，存电影中人物信息，字段：
单个：'person_id','person_name','person_birthday','person_birth_place','person_poster_link'.

集合名"user"，存储用户的信息，字段：
单个：'user_id','user_age','user_gender','user_occupation'.

集合名"rating"，存储评分信息，字段：
单个：'rating_user_id','rating_movie_key','rating_r','rating_time'

## neo4j数据库

movie_id Has_key movie_key
movie_id Has_name movie_name
movie_id Has_release_year movie_year
movie_id Has_rating movie_rating
movie_id Has_rating_count rating_count
movie_id Has_budget movie_budget
movie_id Has_movie_time movie_time
movie_id Has_director person_id
movie_id Has_writer person_id
movie_id Has_actor person_id
movie_id Has_type movie_type
movie_id Has_country movie_country
movie_id Has_language movie_language
movie_id Has_company company_id

person_id Has_name person_name
person_id Has_birthday person_birthday
person_id Has_birth_place person_birth_place
