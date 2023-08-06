import csv
import configparser

import psycopg2


parser = configparser.ConfigParser()
parser.read("pipeline.conf")
dbname = parser.get("postgres_config", "database")
user = parser.get("postgres_config", "username")
password = parser.get("postgres_config", "password")
host = parser.get("postgres_config", "host")
port = parser.get("postgres_config", "port")

conn_str = f"dbname={dbname} user={user} password={password} host={host} port={port}"
conn = psycopg2.connect(conn_str)
cursor = conn.cursor()

DATA_FOLDER = "data"

table = "addresses"
header = ["address_id", "address", "zipcode", "state", "country"]
with open(f"{DATA_FOLDER}/addresses.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerow(header)

    query = f"select * from {table}"
    cursor.execute(query)

    results = cursor.fetchall()
    for each in results:
        writer.writerow(each)

table = "order_items"
header = ["order_id", "product_id", "quantity"]
# ลองดึงข้อมูลจากตาราง order_items และเขียนลงไฟล์ CSV
with open(f"{DATA_FOLDER}/order-items.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerow(header)

    query = f"select * from {table}"
    cursor.execute(query)

    results = cursor.fetchall()
    for each in results:
        writer.writerow(each)


import configparser
import csv

import requests


parser = configparser.ConfigParser()
parser.read("pipeline.conf")
host = parser.get("api_config", "host")
port = parser.get("api_config", "port")

API_URL = f"http://{host}:{port}"
DATA_FOLDER = "data"

### Events
data = "events"
date = "2021-02-10"
response = requests.get(f"{API_URL}/{data}/?created_at={date}")
data = response.json()
with open(f"{DATA_FOLDER}/events.csv", "w") as f:
    writer = csv.writer(f)
    header = data[0].keys()
    writer.writerow(header)

    for each in data:
        writer.writerow(each.values())

### Users
data = "users"
date = "2020-10-23"
# ลองดึงข้อมูลจาก API เส้น users และเขียนลงไฟล์ CSV
response = requests.get(f"{API_URL}/{data}/?created_at={date}")
data = response.json()
with open(f"{DATA_FOLDER}/users.csv", "w") as f:
    writer = csv.writer(f)
    header = data[0].keys()
    writer.writerow(header)

    for each in data:
        writer.writerow(each.values())

### Orders
data = "orders"
date = "2021-02-10"
# ลองดึงข้อมูลจาก API เส้น orders และเขียนลงไฟล์ CSV
response = requests.get(f"{API_URL}/{data}/?created_at={date}")
data = response.json()
with open(f"{DATA_FOLDER}/orders.csv", "w") as f:
    writer = csv.writer(f)
    header = data[0].keys()
    writer.writerow(header)

    for each in data:
        writer.writerow(each.values())


import configparser

import pysftp


parser = configparser.ConfigParser()
parser.read("pipeline.conf")
username = parser.get("sftp_config", "username")
password = parser.get("sftp_config", "password")
host = parser.get("sftp_config", "host")
port = parser.getint("sftp_config", "port")


# Security risk! Don't do this on production
# You lose a protection against Man-in-the-middle attacks
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

DATA_FOLDER = "data"

# โค้ดด้านล่างจะเป็นการโหลดไฟล์ orders.csv ซึ่งเป็นข้อมูล orders
# ให้แก้โค้ดด้านล่างให้ไปโหลดไฟล์ข้อมูล products และ promos แทน
files = [
    "products.csv",
]
with pysftp.Connection(host, username=username, password=password, port=port, cnopts=cnopts) as sftp:
    for f in files:
        sftp.get(f, f"{DATA_FOLDER}/{f}")
        print(f"Finished downloading: {f}")

files = [
    "promos.csv",
]
with pysftp.Connection(host, username=username, password=password, port=port, cnopts=cnopts) as sftp:
    for f in files:
        sftp.get(f, f"{DATA_FOLDER}/{f}")
        print(f"Finished downloading: {f}")