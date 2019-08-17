import pymongo

# 运行之前先启动数据库 sudo service mongod start
class database():
    # 传入数据库名和集合名初始化
    def __init__(self, database_name, collection_name):
        # 链接数据库
        client = pymongo.MongoClient(host='localhost', port=27017)
        # 链接数据库
        db = client[database_name]
        # 指定数据库中的集合，类似于表
        self.collection = db[collection_name]

    # 查询电影是否存在
    def query_id(self, movie_id):
        result = self.collection.find_one({'movie_id': movie_id})
        return result

    # 插入字典
    def insert_dict(self, dict):
        self.collection.insert_one(dict)

    # 修改key值
    def modify_key(self, movie_key, movie_id):
        condition = {'movie_id': movie_id}
        movie = self.collection.find_one(condition)
        if movie_key not in movie['movie_key']:
            movie['movie_key'].append(movie_key)
            self.collection.update_one(condition, {'$set': movie})

    # 获取所有人的id
    def get_peoples_id(self):
        people_id = []
        result = self.collection.find({'directors_id': {'$exists': True}})
        for movie in result:
            for person in movie['directors_id']:
                if person not in people_id:
                    people_id.append(person)
            for person in movie['writers_id']:
                if person not in people_id:
                    people_id.append(person)
            for person in movie['actors_id']:
                if person not in people_id:
                    people_id.append(person)
        return people_id

    # 查询person是否存在
    def query_person_id(self, person_id):
        result = self.collection.find_one({'person_id': person_id})
        return result