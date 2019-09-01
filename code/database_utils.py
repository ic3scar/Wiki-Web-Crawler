import sqlite3

def create_cursor():
    conn = sqlite3.connect('../data/wiki_crawler.sqlite')
    cur = conn.cursor()    
    cur.execute('''
                CREATE TABLE IF NOT EXISTS WikiPages
                (id INTEGER PRIMARY KEY, title TEXT, URL TEXT UNIQUE, retrived BOOLEAN, citedCount INTEGER)
                ''')
    cur.execute("""
                CREATE TABLE IF NOT EXISTS Cites
                (from_id INTEGER, to_id INTEGER, UNIQUE(from_id, to_id))
                """)
    return conn, cur

# Get all cited urls from the given source
def get_cited_urls_from_source(cur, from_url):
    cur.execute("SELECT id FROM WikiPages WHERE URL = ? LIMIT 1", (from_url,))
    try:
        source = cur.fetchone()[0]
    except:
        print("The given url is not found!")
        return []
    res = []
    cur.execute("""
                SELECT URL, id from WikiPages JOIN Cites ON WikiPages.id = Cites.to_id
                WHERE Cites.from_id = ?
                """, (source, ))
    for row in cur:
        res.append(row)
    print("The given page cites %d different internal pages." % (len(res)))
    return res

# Get all urls that cite the given destination
def get_citing_urls_to_destination(cur, to_url):
    cur.execute("SELECT id FROM WikiPages WHERE URL = ? LIMIT 1", (to_url, ))
    try:
        dest = cur.fetchone()[0]
    except:
        print("The given url is not found!")
        return []
    res = []
    cur.execute("""
                SELECT URL, id from WikiPages JOIN Cites ON WikiPages.id = Cites.from_id
                WHERE Cites.to_id = ?
                """, (dest, ))
    for row in cur:
        res.append(row)
    print("There are %d pages found that cites the given link" % (len(res)))
    return res

def get_most_cites(cur, num = 20):
    cur.execute('SELECT URL, title, citedCount FROM WikiPages ORDER BY citedCount DESC LIMIT ?', (num, ))
    res = []
    try:
        for row in cur:
            res.append(row)
    except:
        print('No available links in WikiPages')
    return res

# Case insensitive
def get_title_with_keyword(keyword, cur):
    cur.execute("SELECT URL, title FROM WikiPages WHERE title LIKE ?", ('%'+keyword+'%', ))
    res = []
    try:
        for row in cur:
            res.append(row)
        print("There are %d titles with given keyword." % (len(res)))
    except:
        print("No such title")
    return res

def retrived_page_ratio(cur):
    cur.execute('SELECT COUNT(1), retrived FROM WikiPages GROUP BY retrived')
    uncrawled, crawled = cur
    print("Total number of pages in the dataset: ", uncrawled[0]+crawled[0])
    print("Total number of pages crawled: ", crawled[0])
    print("Crawled ratio: %5.3f" % (crawled[0]/(uncrawled[0]+crawled[0])))