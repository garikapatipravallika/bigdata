import requests
import redis
import matplotlib.pyplot as plt
from collections import Counter
from datetime import datetime

class DataFetcher:
    """Fetches data from a specified JSON API."""
    def __init__(self, api_url):
        self.api_url = api_url
    
    def fetch_data(self):
        """Fetches and returns JSON data from the API."""
        response = requests.get(self.api_url)
        response.raise_for_status()
        return response.json()

class RedisManager:
    """Manages interactions with RedisJSON."""
    def __init__(self, host='localhost', port=6379):
        self.db = redis.Redis(host=host, port=port, decode_responses=True)
        try:
            self.db.ping()
        except redis.exceptions.ConnectionError:
            print("Redis connection error. Please ensure Redis is running and accessible.")
            exit(1)
    
    def insert_json(self, key, data):
        """Inserts JSON data into RedisJSON."""
        self.db.json().set(key, '$', data)
    
    def get_json(self, key):
        """Retrieves JSON data from RedisJSON."""
        return self.db.json().get(key)

class DataProcessor:
    """Processes data from the SpaceX API and generates outputs including charts, aggregation, and search functionalities."""
    def __init__(self, data):
        self.data = data

    def generate_launches_per_year_chart(self):
        """Generates a chart showing the number of launches per year."""
        launch_years = [datetime.strptime(launch['date_utc'], "%Y-%m-%dT%H:%M:%S.%fZ").year for launch in self.data]
        year_counts = Counter(launch_years)
        
        plt.figure(figsize=(10, 6))
        plt.bar(year_counts.keys(), year_counts.values())
        plt.xlabel('Year')
        plt.ylabel('Number of Launches')
        plt.title('SpaceX Launches Per Year')
        plt.xticks(sorted(year_counts.keys()))
        plt.show()

    def count_total_launches(self):
        """Counts and prints the total number of launches."""
        print(f"Total number of SpaceX launches: {len(self.data)}")

    def search_launches(self, keyword):
        """Searches and prints launches containing a given keyword in their name."""
        matched_launches = [launch for launch in self.data if keyword.lower() in launch['name'].lower()]
        print(f"Found {len(matched_launches)} launches matching '{keyword}':")
        for launch in matched_launches:
            print(f" - {launch['name']}")

def main():
    API_URL = 'https://api.spacexdata.com/v4/launches'
    REDIS_KEY = 'spacex:launches'

    # Fetch data from API
    fetcher = DataFetcher(API_URL)
    data = fetcher.fetch_data()
    
    # Insert data into RedisJSON
    manager = RedisManager()
    manager.insert_json(REDIS_KEY, data)
    
    # Retrieve data from RedisJSON and process it
    retrieved_data = manager.get_json(REDIS_KEY)
    processor = DataProcessor(retrieved_data)
    processor.generate_launches_per_year_chart()
    processor.count_total_launches()
    processor.search_launches('Falcon')  # Example: Search for launches with 'Falcon' in their name

if __name__ == '__main__':
    main()
