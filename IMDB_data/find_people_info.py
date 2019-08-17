import requests, re, csv, pymongo, time
from bs4 import BeautifulSoup
from database_operation import database

# 爬取数据，people包括director、writer、actor
def reptilian(person_id):
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'
    }
    base_url = 'https://www.imdb.com/name/'
    url = base_url + person_id
    response = requests.get(url=url, headers=headers)
    soup = BeautifulSoup(response.text, 'lxml')

    # 存储person信息
    person = {}

    person['person_id'] = person_id

    try:
        person['person_name'] = soup.find(name='h1', attrs={'class': "header"}).span.string
    except:
        person['person_name'] = None

    try:
        person['person_birthday'] = soup.find(name='div', attrs={'id': "name-born-info"}).time.attrs['datetime']
    except:
        person['person_birthday'] = None

    try:
        place_pre = soup.find(name='div', attrs={'id': "name-born-info"}).children
        for item in place_pre:
            if item.name == 'a':
                person['person_birth_place'] = item.string
    except:
        person['person_birth_place'] = None

    try:
        person['person_poster_link'] = soup.find(name='img', attrs={'id': "name-poster"}).attrs['src']
    except:
        person['person_poster_link'] = None

    return person

def main():
    collection_m = database('kg', 'movie')
    peoples_id = collection_m.get_peoples_id()
    collection_p = database('kg', 'people')
    for i, person_id in enumerate(peoples_id):
        if collection_p.query_person_id(person_id) is None:
            person = reptilian(person_id)
            collection_p.insert_dict(person)
            print(i)
            time.sleep(1)

if __name__ == '__main__':
    main()
