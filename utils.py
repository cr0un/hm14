import sqlite3
from collections import Counter

class DbConnect:
    def __init__(self, path):
         self.con = sqlite3.connect(path)
         self.cur = self.con.cursor()

    def __del__(self):
         self.cur.close()
         self.con.close()

def execute_query(query):
    """ Загрузчик базы """
    with sqlite3.connect('netflix.db') as con:
        cur = con.cursor()
        cur.execute(query)
        result = cur.fetchall()
    return result

def movies_by_title(title):
    """ Получение фильма по title """
    db_connect = DbConnect('netflix.db')
    db_connect.cur.execute(f"""
        select title, country, release_year, listed_in, description
        from netflix 
        where title like '%{title}%' 
        order by release_year desc limit 1
        """)
    result = db_connect.cur.fetchone()
    return {
        "title": result[0],
        "country": result[1],
        "release_year": result[2],
        "genre": result[3],
        "description": result[4]
    }

def movies_by_release_year(year1, year2):
    """ Получение фильма по release_year (from x to x range)"""
    query = f"select title, release_year from netflix where  release_year between {year1} and {year2} limit 100"
    result = execute_query(query)
    result_list = []
    for movie in result:
        result_list.append({"title": movie[0], "release_year": movie[1]})
    return result_list

def movies_by_rating(rating):
    """ Получение фильма по рейтингу"""
    rating_requirements = {
        "children": "'G'",
        "family": "'G', 'PG', 'PG-13'",
        "adult": "'R', 'NC-13'"
    }
    if rating not in rating_requirements:
        return "Переданной категории не существует"
    query = f"select title, rating, description from netflix where rating in ({rating_requirements[rating]})"
    result = execute_query(query)
    result_list = []
    for movie in result:
        result_list.append({"title": movie[0], "rating": movie[1], "description": movie[2]})
    return result_list

def movies_by_genre(genre):
    """ Получение фильма по жанру"""
    result = execute_query(f"""
    select title, description 
    from netflix
    where listed_in like '%{genre}%' order by release_year desc 
    limit 10;
    """)
    result_list = []
    for movie in result:
        result_list.append({
            "title": movie[0],
            "description": movie[1],
        })
    return result_list

def double_cast(actor1, actor2):
    """ Получение фильма по парным актерам (кто играет с ними в паре больше 2 раз)"""
    query = f"select `cast` from netflix where `cast` like '%{actor1}%' and `cast` like '%{actor2}%'"
    result = execute_query(query)
    actors_list = []
    for cast in result:
        actors_list.extend(cast[0].split(', '))
    counter = Counter(actors_list)
    result_list = []
    for actor, count in counter.items():
        if actor not in [actor1, actor2] and count > 2:
            result_list.append(actor)
    return result_list

def movies_by_requests(movie_type, release_year, genre):
    """ Получение фильма по параметрам - типу, году выпуска и жанру"""
    query = f"""select title, description from netflix where type = '{movie_type}' and release_year = '{release_year}' and listed_in like '%{genre}%'"""
    result = execute_query(query)
    result_list = []
    for movie in result:
        result_list.append({
            'title': movie[0],
            'description': movie[1]
        })
    return result_list
