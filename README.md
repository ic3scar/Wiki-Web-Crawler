# Wiki-Web-Crawler
Web Crawler (for crawling Wiki pages)  
Used to crawl and store the url, title and (internal) links of pages under the wiki domain;  
Only consider internal links, i.e., links towards other website domain won't be stored;  
Only store the link record once for each (ordered) pair of links;  
Won't store the link of a page to itself;  
Crawled information will be stored via SQLite via two tables: WikiPages and Cites.
