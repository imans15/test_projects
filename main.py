from queue import Queue
from bs4 import BeautifulSoup
import urllib.parse
import requests

class Document:
    def __init__(self, url):
        self.url = url
    def download(self):
        try:
            response = requests.get(self.url)
            if response.status_code == 200:
                self.content = response.content
                return True
            else:
                return False
        except:
            return False

class HtmlDocument(Document):
    def normalize(self, href):
        if href is not None and href[:4] != 'http':
            href = urllib.parse.urljoin(self.url, href)
        return href
    def parse(self):
        main = 'https://zakup.kbtu.kz/zakupki/sposobom-zaprosa-cenovyh-predlozheniy'
        model = BeautifulSoup(self.content)
        self.anchors = []
        a = model.find_all('a')
        for anchor in a:
            href = self.normalize(anchor.get('href'))
            text = anchor.text
            if href is not None and href[:len(main)] == main:
                self.anchors.append((text, href))

        if (self.url[:len(main)] == main and
                self.url[len(main): len(main) + 7] != "/logon&"):
            divs = model.find_all("div", {"class": "container"})
            last_table = None
            last_header = None
            for div in divs:
                table = div.find("table")
                header = div.find("h4")
                if header is not None:
                    last_header = header
                    last_table = table
            if last_table is not None and last_header is not None:
                header = last_header.text.strip()
                trs = []
                for tr in last_table.find_all("tr"):
                    trs.append(tr.find_all("td")[-1].text.strip())
                return [header] + trs
        return None

class Indexer:
    def __init__(self):
        self.csv = 'result.csv'
        self.header = ['№', 'Заголовок', 'Организатор', 'Начало', 'Окончание', 'Статус']
        self.row = 1
        self.file = open("test_parse_kbtu.csv", "w")
        self.write(self.header)

    def write(self, row):
        self.file.write('"' + '";"'.join(row) + '"\n')

    def crawl_generator(self, source, depth):
        q = Queue()
        q.put((source, 0))
        visited = set()

        while not q.empty():
            url, url_depth = q.get()
            if url not in visited:
                visited.add(url)
                doc = HtmlDocument(url)
                if doc.download():
                    pass
                else:
                    continue
                item = doc.parse()
                if item is not None:
                    self.write([str(self.row)] + item)
                    self.row += 1

                for link in doc.anchors:
                    if url_depth + 1 >= depth:
                        break
                    if url_depth + 1 < depth and link[1] not in visited:
                        q.put((link[1], url_depth + 1))
                yield doc

indexer = Indexer()
for c in indexer.crawl_generator("https://zakup.kbtu.kz/zakupki/sposobom-zaprosa-cenovyh-predlozheniy", 5):
    print(c.url)
