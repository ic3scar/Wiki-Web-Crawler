import urllib.request, urllib.parse, urllib.error
from bs4 import BeautifulSoup
import ssl
import sqlite3
from collections import deque

# Request 30 un-crawled page based on citedCount (DESC)
def crawl_fetch_30_candidates(queue, cur):
    cur.execute('SELECT URL from WikiPages WHERE retrived = ? ORDER BY citedCount DESC LIMIT 30', (0, ))
    for row in cur:
        queue.append(row[0])  

def crawl_one_page(url, cur, adding = False, queue = None, seed_page = "https://en.wikipedia.org"):
    cur.execute("SELECT id FROM WikiPages WHERE url = ?", (url, ))
    from_id = cur.fetchone()[0]
    try:
        html = urllib.request.urlopen(seed_page+url, context=ctx).read()
    except:
        # print(seed_page+url, " can't be retrived.")
        return -1   
    count = 0
    soup = BeautifulSoup(html, 'html.parser')
    title = soup.title.string
    print(title)
    cur.execute('UPDATE WikiPages SET title = ? WHERE URL = ?', (title, url))
    # Retrieve all of the anchor tags
    tags = soup('a')
    cited = set()
    for tag in tags:
        to_url = tag.get('href', None)
        if not to_url or not to_url.startswith('/') or to_url.startswith('//') or to_url in cited:
            continue
        cited.add(to_url)
        cur.execute('SELECT citedCount, id from WikiPages WHERE URL = ? LIMIT 1', (to_url, ))
        if adding:
            queue.append(to_url)
        try:
            num = cur.fetchone()[0]
            cur.execute('UPDATE WikiPages SET citedCount = ? WHERE URL = ?', (num+1, to_url))
        except:
            count += 1
            cur.execute('INSERT INTO WikiPages (URL, retrived, citedCount) VALUES (?, 0, 1)', (to_url, ))
        cur.execute('SELECT id FROM WikiPages WHERE url = ?', (to_url, ))
        to_id = cur.fetchone()[0]
        if from_id!=to_id:
            cur.execute('INSERT OR IGNORE INTO Cites (from_id, to_id) VALUES (?, ?)', (from_id, to_id))
    return count

def crawl_with_given_page(conn, cur, start_page = "/wiki/Mathematics", time = 100, seed_page = "https://en.wikipedia.org"):
    cur.execute('SELECT retrived from WikiPages WHERE URL = ? LIMIT 1', (start_page, ))
    try:
        retrived = cur.fetchone()[0]
        if retrived == 1:
            print("The given start_page has been crawled already.")
            return
        print("Start crawling")
    except:
        print("Start crawling")
        
    queue = deque([start_page])
    count = 0
    total_count = 0
    while queue and count<time:
        print(count)
        url = queue.popleft()

        cur.execute('SELECT retrived from WikiPages WHERE URL = ? LIMIT 1', (url, ))
        try:
            retrived = cur.fetchone()[0]
            if retrived == 1:
                print("The given website has been crawled already.")
                continue
            cur.execute('UPDATE WikiPages SET retrived = ? WHERE URL = ?', (1, url))
        except:
            total_count += 1
            cur.execute('INSERT INTO WikiPages (URL, retrived, citedCount) VALUES (?, 1, 0)', (url, ))

        page_count = crawl_one_page(url, cur, adding = True, queue = queue)
        
        if page_count>-1:    
            count += 1
            total_count += page_count
            if count%100==0:
                conn.commit()
    
    conn.commit()
    print("Total new pages crawled: ", count)
    print("Total new pages found: ", total_count)
    return

def crawl_with_existing_page(conn, cur, time = 100, seed_page = "https://en.wikipedia.org"):
    queue = deque()
    crawl_fetch_30_candidates(queue, cur)
    if len(queue)==0:
        print('No avaiable link in the database to crawl')
        return
    else:
        print('Start crawling')
        
    count = 0
    total_count = 0
    while count<time:
        print(count)
        if len(queue)==0:
            crawl_fetch_30_candidates(queue, cur)
            if len(queue)==0:
                print('No available links')
                break
                       
        url = queue.popleft()
        cur.execute('UPDATE WikiPages SET retrived = ? WHERE URL = ?', (1, url))
        
        page_count = crawl_one_page(url, cur)
 
        if page_count>-1:    
            count += 1
            total_count += page_count
            if count%100==0:
                conn.commit()
    
    conn.commit()
    print("Total new pages crawled: ", count)
    print("Total new pages found: ", total_count)
    return

