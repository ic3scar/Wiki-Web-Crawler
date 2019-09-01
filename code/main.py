import urllib.request, urllib.parse, urllib.error
from bs4 import BeautifulSoup
import ssl
import sqlite3
from crawler_utils import crawl_with_given_page, crawl_with_existing_page
from database_utils import create_cursor, get_cited_urls_from_source, get_citing_urls_to_destination, get_most_cites, get_title_with_keyword, retrived_page_ratio

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

seed_page = "https://en.wikipedia.org"
start_page = "/wiki/Mathematics"

conn, cur = create_cursor()

crawl_with_given_page(conn, cur, start_page = "/wiki/Mathematics", time = 2000)
crawl_with_given_page(conn, cur, start_page = "/wiki/Toeplitz_operator", time = 500)
crawl_with_existing_page(conn, cur, 5000)

urls = get_cited_urls_from_source(cur, "/wiki/Toeplitz_operator")
for i in range(10):
    print(urls[i])

urls = get_citing_urls_to_destination(cur, "/wiki/Mathematics")
for i in range(10):
    print(urls[i])

urls = get_most_cites(cur, 30)
for row in urls:
    print(row)

urls = get_title_with_keyword("Operator", cur)
for row in urls:
    print(row)

retrived_page_ratio(cur)

cur.close()