import pymongo

# 运行之前先启动数据库 sudo service mongod start
# 链接数据库
client = pymongo.MongoClient(host='localhost', port=27017)
# 链接test数据库
db = client['kg']
# 指定数据库中的集合，类似于表
collection = db['people']

student = {
    'id': '20170101',
    'name': 'Jordan',
    'age': 20,
    'gender': [1, 2, 3]
}

student1 = {
    'id': '20170101',
    'name': 'Jordan',
    'age': 30,
    'gender': [1, 2, 3],
    'student2': {
        'id': '20170101',
        'name': 'Jordan',
        'age': 40,
        'gender': [1, 2, 3]
    }
}

#result1 = collection.insert([student, student1])
#collection.update_one({'name':"Jordan"},{'$set':{'id':'yyyyy'}})
result = collection.find({'person_id': {'$exists': True}})
for re in result:
    print(re)