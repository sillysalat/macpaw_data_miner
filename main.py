import psycopg2
import random
import re
import requests
import time

import hidden


class Parser:

    def __init__(self, url='https://data-engineering-interns.macpaw.io/'):

        self.url = url

    def get_list(self, resource='files_list.data', try_number=0):
        """Retrieve and parse the list of data"""
        try:
            response = requests.request('GET', url=self.url + resource)
            data_list = response.text.splitlines()
        except requests.exceptions.ConnectionError:
            time.sleep(2 ** try_number + random.random() * 0.01)
            return self.get_list(try_number=try_number + 1)
        else:
            return data_list

    def get_data(self, data_link, try_number=0):

        try:
            response = requests.request('GET', url=self.url + data_link).json()
        except (requests.exceptions.ConnectionError, json.JSONDecodeError):
            print('Making new request...')
            time.sleep(2 ** try_number + random.random() * 0.01)
            return self.get_data(try_number=try_number + 1)
        else:
            return response


class Database:
    """PostgreSQL Database class."""

    def __init__(self, secrets=hidden.secrets()):

        self.secrets = secrets
        self.conn = None
        self.cur = None

    def open(self):
        """Connect to a Postgres database."""
        try:
            self.conn = psycopg2.connect(database=self.secrets['database'],
                                         user=self.secrets['user'],
                                         password=self.secrets['pass'],
                                         host=self.secrets['host'],
                                         port=self.secrets['port'])

        except psycopg2.Error:
            print("Database do not respond")
        else:
            self.cur = self.conn.cursor()
            self.check_data_structure()
            print("Connection is open")

    def check_data_structure(self):
        """check if database has required tables"""
        self.cur.execute("CREATE TABLE IF NOT EXISTS public.processed"
                         "(id SERIAL PRIMARY KEY, data_links VARCHAR);"
                         )
        self.cur.execute("CREATE TABLE IF NOT EXISTS public.songs"
                         "(id SERIAL PRIMARY KEY, artist_name VARCHAR,"
                         "title VARCHAR, year INTEGER, release VARCHAR,"
                         "ingestion_time TIMESTAMPTZ NOT NULL DEFAULT NOW());"
                         )
        self.cur.execute("CREATE TABLE IF NOT EXISTS public.movies"
                         "(id SERIAL PRIMARY KEY, original_title VARCHAR,"
                         "original_language VARCHAR, budget INTEGER, is_adult BOOLEAN,"
                         "release_date DATE, original_title_normalized VARCHAR);"
                         )
        self.cur.execute("CREATE TABLE IF NOT EXISTS public.apps"
                         "(id SERIAL PRIMARY KEY, name VARCHAR,"
                         "genre VARCHAR, rating REAL, version VARCHAR,"
                         "size_bytes BIGINT, is_awesome BOOLEAN);"
                         )
        self.conn.commit()

    def close(self):
        """Close connection to a Postgres database."""
        if self.conn:
            self.conn.commit()
            self.cur.close()
            self.conn.close()

    def get(self, columns, table, limit=None):

        query = f"SELECT {columns} from {table};"
        self.cur.execute(query)
        rows = self.cur.fetchall()

        return rows[len(rows) - limit if limit else 0:]

    def post(self, table, columns, data):
        query = f"INSERT INTO {table} ({columns}) VALUES ({data});"
        self.cur.execute(query)

    def execute(self, action):
        self.cur.execute(action)


class DataManager:

    def __init__(self, db):
        self.request = ['song', 'movie', 'app']
        self.db = db

    def list_update(self, data_list):
        """Takes parsed data_list and returns a list of new links"""
        new_data = []
        check_list = [link[0] for link in self.db.get('data_links', 'processed')]

        for i in data_list:
            if i in check_list:
                continue
            else:
                new_data.append(i)

        if not new_data:
            print('No new links')

        return new_data

    def _post_songs(self, songs):
        """Takes a list of songs and sends data to database"""
        for song in songs:
            sql = "INSERT INTO songs (artist_name, title, year, release) VALUES (%s, %s, %s, %s)"
            insertion_data = song['artist_name'], song['title'], song['year'], song['release']
            self.db.cur.execute(sql, insertion_data)
            self.db.conn.commit()

    def _post_movies(self, movies):
        """Takes a list of movies and sends data to database"""
        for movie in movies:
            sql = "INSERT INTO movies (original_title, original_language, budget, is_adult," \
                  "release_date, original_title_normalized) VALUES (%s, %s, %s, %s, %s, %s)"

            insertion_data = (movie['original_title'], movie['original_language'], movie['budget'],
                              movie['is_adult'], movie['release_date'], self._normalize(movie['original_title']),)

            self.db.cur.execute(sql, insertion_data)
            self.db.conn.commit()

    def _post_apps(self, apps):
        """Takes a list of apps and sends data to database"""
        for app in apps:
            sql = "INSERT INTO apps (name, genre, rating, version, size_bytes, is_awesome)" \
                  "VALUES (%s, %s, %s, %s, %s, %s)"
            insertion_data = app['name'], app['genre'], app['rating'], app['version'], \
                             app['size_bytes'], bool(random.randint(0, 1))
            self.db.cur.execute(sql, insertion_data)
            self.db.conn.commit()

    def post_data(self, data, link):
        """Takes sorted dictionary and sends data to database"""
        print(f"Processing {link}")
        self._post_songs(data['songs'])
        self._post_movies(data['movies'])
        self._post_apps(data['apps'])
        self._update_processed(link)
        print(f"Successfully processed and stored {link}")

    def _update_processed(self, link):
        """Takes link and saves it to database processed links"""
        self.db.cur.execute("INSERT INTO processed (data_links) VALUES (%s)", (link,))

    def sort(self, data):
        """Takes parsed dictionary and returns sorted dict with 3 categories: songs, movies, apps"""
        sorted_content = {'songs': [], 'movies': [], 'apps': []}

        for i in data:
            if i['type'] not in self.request:
                continue
            else:
                if i['type'] == 'song':
                    sorted_content['songs'].append(i['data'])
                if i['type'] == 'movie':
                    sorted_content['movies'].append(i['data'])
                if i['type'] == 'app':
                    sorted_content['apps'].append(i['data'])

        return sorted_content

    def _normalize(self, data):
        """Takes a string and returns a string where
        non-letter and non-number characters are
        removed and spaces replaced with underscore
        """
        normalized = (re.sub("[\x00-\x2F\x3A-\x40\x5B-\x60\x7B-\x7F]+", "_", data)).lower()

        return normalized


def main():
    # create parser, database and data manager
    parser = Parser()
    db = Database()
    dm = DataManager(db)

    # connect to database
    db.open()

    # parse the list of links
    data_list = parser.get_list()

    # check if there's new entries in the list
    updated_data_list = dm.list_update(data_list)

    # parse data and send it to database from the updated link list
    for link in updated_data_list:
        # parse data
        data = parser.get_data(link)
        # sort parsed data
        sorted_data = dm.sort(data)
        # send data to db
        dm.post_data(sorted_data, link)

    db.close()


main()
