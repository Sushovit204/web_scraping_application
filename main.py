import requests
from bs4 import BeautifulSoup
import sqlite3
import logging

# Configure logging
logging.basicConfig(filename='scraping_errors.log', level=logging.ERROR)

def scrape_and_store_movies(url, db_file):
    try:
        # Connect to SQLite database (create if not exists)
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Create table to store movie data
        cursor.execute("CREATE TABLE IF NOT EXISTS movies (id INTEGER PRIMARY KEY, title TEXT, year TEXT, rating REAL)")

        response = requests.get(url)
        response.raise_for_status()  # Raise an error for non-200 status codes
        soup = BeautifulSoup(response.text, 'html.parser')

        movies = soup.select('td.titleColumn')
        ratings = soup.select('td.ratingColumn.imdbRating')

        # Iterate over movies and insert data into table
        for movie, rating in zip(movies, ratings):
            title = movie.select('a')[0].text
            year = movie.select('span')[0].text[1:-1]
            rating = float(rating.select('strong')[0].text)

            # Insert movie data into table
            sql = "INSERT INTO movies (title, year, rating) VALUES (?, ?, ?)"
            val = (title, year, rating)
            cursor.execute(sql, val)
            conn.commit()

        print("Movies added to SQLite database!")

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from {url}: {e}")
    except Exception as e:
        logging.error(f"Error processing data: {e}")
    finally:
        # Close the connection
        if 'conn' in locals():
            conn.close()

# Test the function with IMDb's top-rated movies URL and SQLite database file
if __name__ == "__main__":
    imdb_url = 'https://www.imdb.com/chart/top/?sort=ir,desc&mode=simple&page=1'
    sqlite_db_file = 'movies.db'
    scrape_and_store_movies(imdb_url, sqlite_db_file)
