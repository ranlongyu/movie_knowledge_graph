import requests, re, csv, pymongo
from bs4 import BeautifulSoup
from database_operation import database

# 读取需要爬取的电影
def read_movie_id():
    movies_key_id = []
    filename = 'movies.csv'
    with open(filename) as f:
        reader = csv.reader(f)
        for row in reader:
            if row[3] is not '':   # 判断movie_id不为空
                movies_key_id.append([row[0], row[3]])
    del movies_key_id[0]
    return movies_key_id

# 爬取数据
def reptilian(movie_key, movie_id):
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'
    }
    base_url = 'https://www.imdb.com/title/'
    url = base_url + movie_id
    response = requests.get(url=url, headers=headers)
    soup = BeautifulSoup(response.text, 'lxml')

    # 存所有的数据
    movie = {}

    # 存key与id，key存为列表
    movie['movie_key'] = [movie_key]
    movie['movie_id'] = movie_id

    # 获取电影名称，年份
    try:
        movie_name_year_pre = soup.find(name='h1', attrs={'itemprop': "name", 'class': ""})
        movie_name_year = re.search('<h1 class="" itemprop="name">(.+?)<span.+?href="/year/(\d+?)/.+', str(movie_name_year_pre))
        movie['movie_name'] = movie_name_year.group(1).strip()
        movie['movie_year'] = movie_name_year.group(2)
    except:
        movie['movie_name'] = None
        movie['movie_year'] = None

    # 获取评分，评分人数
    try:
        movie['movie_rating'] = soup.find(name='span', attrs={'itemprop': "ratingValue"}).string
        movie['rating_count'] = soup.find(name='span', attrs={'class': 'small', 'itemprop': 'ratingCount'}).string
    except:
        movie['movie_rating'] = None
        movie['rating_count'] = None

    # 获取电影海报链接
    try:
        movie['movie_poster_link'] = soup.find(name='div', attrs={'class': 'poster'}).a.img.attrs['src']
    except:
        movie['movie_poster_link'] = None

    # 获取剧情概要，strip()把头尾的空格去掉
    try:
        movie['summary_text'] = soup.find(name='div', attrs={'class': 'summary_text'}).string.strip()
    except:
        movie['summary_text'] = None

    # 获取导演列表
    try:
        directors_id = []
        directors_link_pre = soup.find_all(name='span', attrs={'itemprop': "director", 'itemtype': "http://schema.org/Person"})
        for director_link_pre in directors_link_pre:
            director_link = director_link_pre.a.attrs['href']
            director_id = re.search('/name/(\w+)/\S+', director_link).group(1)
            directors_id.append(director_id)
        movie['directors_id'] = directors_id
    except:
        movie['directors_id'] = None

    # 获取作者列表
    try:
        writers_id = []
        writers_link_pre = soup.find_all(name='span', attrs={'itemprop': "creator", 'itemtype': "http://schema.org/Person"})
        for writer_link_pre in writers_link_pre:
            writer_link = writer_link_pre.a.attrs['href']
            writer_id = re.search('/name/(\w+)/\S+', writer_link).group(1)
            writers_id.append(writer_id)
        movie['writers_id'] = writers_id
    except:
        movie['writers_id'] = None

    # 获取演员列表
    try:
        actors_id = []
        actors_link_pre = soup.find_all(name='td', attrs={'class': "itemprop", 'itemprop': "actor", 'itemtype': "http://schema.org/Person"})
        for actor_link_pre in actors_link_pre:
            actor_link = actor_link_pre.a.attrs['href']
            actor_id = re.search('/name/(\w+)/\S+', actor_link).group(1)
            actors_id.append(actor_id)
        movie['actors_id'] = actors_id
    except:
        movie['actors_id'] = None

    # 获取电影类型列表
    try:
        movie_types = []
        movie_types_pre = soup.find(name='div', attrs={'class': "see-more inline canwrap", 'itemprop': "genre"}).find_all(name='a')
        for movie_type_pre in movie_types_pre:
            movie_types.append(movie_type_pre.string.strip())
        movie['movie_types'] = movie_types
    except:
        movie['movie_types'] = None

    # 获取国家列表
    try:
        movie_countries = []
        movie_countries_pre = soup.find(name='h4', attrs={'class': "inline"}, text='Country:').find_parent().find_all(name='a')
        for county in movie_countries_pre:
            movie_countries.append(county.string)
        movie['movie_countries'] = movie_countries
    except:
        movie['movie_countries'] = None

    # 获取语言列表
    try:
        movie_languages = []
        movie_languages_pre = soup.find(name='h4', attrs={'class': "inline"}, text='Language:').find_parent().find_all(name='a')
        for language in movie_languages_pre:
            movie_languages.append(language.string)
        movie['movie_languages'] = movie_languages
    except:
        movie['movie_languages'] = None

    # 获取预算
    try:
        movie_budget_pre = soup.find(name='h4', attrs={'class': "inline"}, text='Budget:').find_parent()
        movie['movie_budget'] = re.search('.+?Budget:</h4>(\S+).+', str(movie_budget_pre)).group(1).strip()
    except:
        movie['movie_budget'] = None

    # 获取出产公司列表
    try:
        companies_id = []
        companies_id_pre = soup.find_all(name='span', attrs={'itemprop': "creator", 'itemtype': "http://schema.org/Organization"})
        for company in companies_id_pre:
            company_link = company.a.attrs['href']
            company_id = re.search('/company/(\w+)\S+', company_link).group(1)
            companies_id.append(company_id)
        movie['companies_id'] = companies_id
    except:
        movie['companies_id'] = None

    # 获取电影时长
    try:
        movie['movie_time'] = soup.find(name='h4', attrs={'class': "inline"}, text='Runtime:').find_parent().find(name='time').string
    except:
        movie['movie_time'] = None

    # 返回字典
    return movie

def main():
    movies_key_id = read_movie_id()
    collection = database('kg', 'movie')
    for key_id in movies_key_id:
        if collection.query_id(key_id[1]) is None:
            movie = reptilian(key_id[0], key_id[1])
            collection.insert_dict(movie)
            print(key_id)
        else:
            collection.modify_key(key_id[0], key_id[1])

if __name__ == '__main__':
    main()