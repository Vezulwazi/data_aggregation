import requests
import json
import sqlite3
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import csv

def extract_csv_data(file_path):
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            yield row

def scrape_html_data(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    data_table = soup.find('table')
    for row in data_table.find_all('tr'):
        cols = row.find_all('td')
        if cols:
            yield [col.text.strip() for col in cols]

def parse_xml_data(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    for item in root.findall('.//item'):
        yield [item.find('name').text, item.find('age').text]

def read_json_data(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
        for entry in data:
            yield [entry['name'], entry['age']]

def create_database():
    conn = sqlite3.connect('data_aggregation.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS csv_data (
                        id INTEGER PRIMARY KEY,
                        name TEXT,
                        age INTEGER
                    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS html_data (
                        id INTEGER PRIMARY KEY,
                        name TEXT,
                        age INTEGER
                    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS xml_data (
                        id INTEGER PRIMARY KEY,
                        name TEXT,
                        age INTEGER
                    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS json_data (
                        id INTEGER PRIMARY KEY,
                        name TEXT,
                        age INTEGER
                    )''')
    conn.commit()
    conn.close()

def insert_data_into_database(data, table_name):
    conn = sqlite3.connect('data_aggregation.db')
    cursor = conn.cursor()
    for row in data:
        cursor.execute(f"INSERT INTO {table_name} (name, age) VALUES (?, ?)", row)
    conn.commit()
    conn.close()

def search_database(query, params):
    conn = sqlite3.connect('data_aggregation.db')
    cursor = conn.cursor()
    cursor.execute(query, params)
    result = cursor.fetchall()
    conn.close()
    return result

if __name__ == "__main__":
    create_database()

    # Extract data
    csv_data = list(extract_csv_data('data.csv'))
    html_data = list(scrape_html_data('https://en.wikipedia.org/wiki/Main_Page'))
    xml_data = list(parse_xml_data('data.xml'))
    json_data = list(read_json_data('data.json'))

    # Insert data into SQLite database
    insert_data_into_database(csv_data, 'csv_data')
    insert_data_into_database(html_data, 'html_data')
    insert_data_into_database(xml_data, 'xml_data')
    insert_data_into_database(json_data, 'json_data')

    # Example of a parameterized query
    name_to_search = input("Enter name to search: ")
    query = 'SELECT * FROM csv_data WHERE name = ?'
    result = search_database(query, (name_to_search,))
    print(result)
