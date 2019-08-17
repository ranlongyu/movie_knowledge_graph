import re
from mongodb_operation import database


def user_save_mongodb():
    # 获取用户列表
    users = []
    with open("./ml-100k/u.user") as file_object:
        for line in file_object:
            line_s = re.split('\|', line)
            users.append([line_s[0], line_s[1], line_s[2], line_s[3], line_s[4].strip()])

    collection_u = database('kg', 'user')

    # 存入mongodb
    for item in users:
        user = {}
        user['user_id'] = item[0]
        user['user_age'] = item[1]
        user['user_gender'] = item[2]
        user['user_occupation'] = item[3]
        collection_u.insert_dict(user)

def rating_save_mongodb():
    # 获取评分列表
    ratings = []
    with open("./ml-100k/u.data") as file_object:
        for line in file_object:
            line_s = re.split('\t', line)
            ratings.append([line_s[0], line_s[1], line_s[2], line_s[3].strip()])

    collection_u = database('kg', 'rating')

    # 存入mongodb
    for item in ratings:
        rating = {}
        rating['rating_user_id'] = item[0]
        rating['rating_movie_key'] = item[1]
        rating['rating_r'] = item[2]
        rating['rating_time'] = item[3]
        collection_u.insert_dict(rating)