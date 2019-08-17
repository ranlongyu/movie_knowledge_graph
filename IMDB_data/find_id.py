import requests
from bs4 import BeautifulSoup
import re
import csv

# 获取电影列表
movies = []
with open("new_u.item") as file_object:
    for line in file_object:
        line_s = re.split('\|', line)
        movies.append([line_s[0], line_s[1], line_s[2]])

'''
csvFile = open("movies.csv", "w")
try:
    writer = csv.writer(csvFile)
    writer.writerow(('movie_key', 'name', 'year', 'imdb_id'))
finally:
    csvFile.close()
'''

for movie in movies[809:]:
    print(movie[1])
    data = {
        "q": movie[1],
        "s": 'tt',
        "ttype": 'ft',
        "ref_": 'fn_ft'
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'
    }
    request = requests.get('https://www.imdb.com/find', params=data, headers=headers)
    soup = BeautifulSoup(request.text, 'lxml')
    findList = soup.find(attrs={'class': 'findList'})
    if findList != None:
        link = findList.a.attrs['href']
        result = re.search('/title/(\w+)/\S+', link)
        movie_imdb_id = result.group(1)
        movie.append(movie_imdb_id)
        print(movie_imdb_id)
    else:
        movie.append(None)

    csvFile = open("movies.csv", "a")
    try:
        writer = csv.writer(csvFile)
        writer.writerow(movie)
    finally:
        csvFile.close()