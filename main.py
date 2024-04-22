import requests
from bs4 import BeautifulSoup
import sqlite3
import logging

# Configure logging
logging.basicConfig(filename='scraping_errors.log', level=logging.ERROR)

def scrape_and_store_books(url, db_file):
    """
    Scrape Book title,price and rating from the given books website sandbox URL and store it in a SQLite database.

    Parameters:
    - url (str): The URL of the Book page containing books and prices.
    - db_file (str): The filename of the SQLite database to store the data.

    Returns:
    None
    """
    try:
        # Connect to SQLite database (create if not exists)
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Create table to store product data
        cursor.execute("CREATE TABLE IF NOT EXISTS products (id INTEGER PRIMARY KEY, title TEXT, price TEXT, rating TEXT)")


        response = requests.get(url)
        response.raise_for_status()  # Raise an error for non-200 status codes
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.content, "html.parser")

        # Find all book containers
        book_containers = soup.find_all("article", class_="product_pod")

        # Loop through each book container and extract title and price
        for book in book_containers:
            # Extract the title
            title = book.h3.a.get_text(strip=True)

            # Extract the price
            price = book.find("p", class_="price_color").get_text(strip=True)

            # Extract the rating
            rating_class = book.find("p", class_="star-rating")
            rating = rating_class['class'][1]

            # Insert product data into table
            sql = "INSERT INTO products (title, price, rating) VALUES (?, ?, ?)"
            val = (title, price, rating)
            cursor.execute(sql, val)
            conn.commit()

        print("Data has been committed to the database")

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from {url}: {e}")
        print("log error")
    except sqlite3.Error as e:
        logging.error(f"SQLite error: {e}")
        print("SQLite error:", e)
    except Exception as e:
        logging.error(f"Error processing data: {e}")
        print(f"second log error {e}")
    finally:
        # Close the connection
        if 'conn' in locals():
            conn.close()

# Test the function with Amazon's top sale products URL and SQLite database file
if __name__ == "__main__":
    book_site_url = 'http://books.toscrape.com/'
    sqlite_db_file = 'books.db'

    scrape_and_store_books(book_site_url, sqlite_db_file)
