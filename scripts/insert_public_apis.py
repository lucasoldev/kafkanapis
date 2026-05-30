import psycopg2
import sys
from config import config

# Configurações do PostgreSQL
PG_HOST = 'localhost'
PG_PORT = config.POSTGRES_PORT
PG_DB = 'public_apis'
PG_USER = config.POSTGRES_USER
PG_PASSWORD = config.POSTGRES_PASSWORD

def connect_db():
    """Connect to the public_apis database."""
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )
    return conn

def create_table_if_not_exists(conn):
    """Create the public_apis table if it doesn't exist."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public_apis (
            id SERIAL PRIMARY KEY,
            site_name VARCHAR(255) NOT NULL,
            description TEXT,
            url TEXT NOT NULL,
            get_endpoint TEXT NOT NULL,
            return_example TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    cur.close()
    print("✅ Table 'public_apis' created or already exists.")

def insert_api(conn, site_name, description, url, get_endpoint, return_example=None):
    """Insert a single API record into the table."""
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO public_apis (site_name, description, url, get_endpoint, return_example)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        site_name,
        description,
        url,
        get_endpoint,
        return_example
    ))
    conn.commit()
    cur.close()
    print(f"✅ Inserted: {site_name}")

def main():
    conn = connect_db()
    create_table_if_not_exists(conn)
    
    # Lista de APIs
    apis = [
        {
            "site_name": "JSONPlaceholder",
            "description": "Fake JSON API for testing",
            "url": "https://jsonplaceholder.typicode.com/",
            "get_endpoint": "https://jsonplaceholder.typicode.com/posts",
            "return_example": "response.json()"
        },
        {
            "site_name": "Fake Store API",
            "description": "Fake e-commerce API",
            "url": "https://fakestoreapi.com/",
            "get_endpoint": "https://fakestoreapi.com/products",
            "return_example": "response.json()"
        },
        {
            "site_name": "DummyJSON",
            "description": "Fake data for testing",
            "url": "https://dummyjson.com/",
            "get_endpoint": "https://dummyjson.com/products",
            "return_example": "response.json()"
        },
        {
            "site_name": "httpbin.org",
            "description": "HTTP request testing service",
            "url": "https://httpbin.org/",
            "get_endpoint": "https://httpbin.org/get",
            "return_example": "response.json()"
        },
        {
            "site_name": "No-as-a-Service",
            "description": "API that always returns a negative response, simulating an unreliable service",
            "url": "https://naas.isalman.dev",
            "get_endpoint": "https://naas.isalman.dev/no",
            "return_example": "response.json()"
        },
        {
            "site_name": "Frankfurter",
            "description": "Free and open-source currency exchange rate API 💸",
            "url": "https://www.frankfurter.app/",
            "get_endpoint": "https://api.frankfurter.dev/v2/rates",
            "return_example": "response.json()"
        },
        {
            "site_name": "SpaceX REST API",
            "description": "Historical and real-time data about SpaceX launches 🚀",
            "url": "https://github.com/r-spacex/SpaceX-API",
            "get_endpoint": "https://api.spacexdata.com/v5/launches/latest",
            "return_example": "response.json()"
        },
        {
            "site_name": "Open Library",
            "description": "Open digital library with access to books, authors, and metadata 📚",
            "url": "https://openlibrary.org/developers/api",
            "get_endpoint": "http://openlibrary.org/search/lists.json?q=book&limit=20&offset=0",
            "return_example": "response.json()"
        },
        {
            "site_name": "Hacker News API",
            "description": "Programmatic feed of news and discussions from Y Combinator Hacker News 📰",
            "url": "https://github.com/HackerNews/API",
            "get_endpoint": "https://hacker-news.firebaseio.com/v0/newstories.json?print=pretty&limit=200",
            "return_example": "response.json()"
        },
        {
            "site_name": "IP Query IO",
            "description": "API providing information about an IP address, including geolocation and VPN detection",
            "url": "https://api.ipquery.io/?format=json",
            "get_endpoint": "https://api.ipquery.io/",
            "return_example": "response.json()"
        },
        {
            "site_name": "AwesomeAPI",
            "description": "Real-time currency exchange API with over 150 currencies!",
            "url": "https://docs.awesomeapi.com.br/api-de-moedas",
            "get_endpoint": "https://economia.awesomeapi.com.br/json/available",
            "return_example": "response.json()"
        },
        {
            "site_name": "UUID Generator API",
            "description": "Generates unique UUIDs and GUIDs via HTTP requests",
            "url": "https://www.uuidtools.com/docs?ref=freepublicapis.com",
            "get_endpoint": "https://www.uuidtools.com/api/generate/v1",
            "return_example": "response.json()"
        },
        {
            "site_name": "TheMealDB",
            "description": "Meal recipe API for developers and food lovers",
            "url": "https://www.themealdb.com/documentation",
            "get_endpoint": "https://www.themealdb.com/api/json/v1/1/categories.php",
            "return_example": "response.json()"
        }
    ]
    
    for api in apis:
        insert_api(
            conn,
            api["site_name"],
            api["description"],
            api["url"],
            api["get_endpoint"],
            api["return_example"]
        )
    
    print("\n✅ All APIs inserted successfully!")
    conn.close()

if __name__ == "__main__":
    main()