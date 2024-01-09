############### ASYNC DOWNLOAD #############

import requests
from multiprocessing.pool import ThreadPool
import base64
import os
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)

headers = {
    'Accept-Encoding': 'gzip',
    'Accept-Language': 'en-US,en;q=0.5',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:92.0) Gecko/20100101 Firefox/92.06',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Cache-Control': 'max-age=0',
    'Connection': 'Keep-Alive',
}

username = "adisen99"
passwd = base64.b64decode("U2FpQGJhYmE5OQ==").decode("utf-8")

def download_url(url):
    # print("downloading: ", url)
    # assumes that the last segment after the / represents the file name
    # if url is abc/xyz/file.txt, the file name will be file.txt
    file_name_start_pos = url.rfind("/") + 1
    file_name_end_pos = url.find("?")
    fname = url[file_name_start_pos:file_name_end_pos]

    if not os.path.exists(fname):
        s = requests.Session()
        s.mount('http://', adapter)
        s.mount('https://', adapter)
        s.max_redirects = 100
        r = s.get(url, stream=False, allow_redirects=True, headers = headers, timeout = 3000, auth = (username, passwd))
        if r.status_code == requests.codes.ok:
          with open(fname, 'wb') as f:
            for data in r:
              f.write(data)
        print("downloaded and saved : " + fname)
        time.sleep(1)
        return url

def down(txtfile):
    # Get the urls from the txt file as a list using readline
    file1 = open(txtfile, 'r')
    Lines = file1.readlines()

    urls = []
    count = 0
    # Strips the newline character
    for line in Lines:
        count += 1
        urls.append(line.strip())

    # Run 5 multiple threads. Each call will take the next element in urls list
    results = ThreadPool(5).imap_unordered(download_url, urls)
    for r in results:
        # print(r)
        pass

    print("COMPLETE: downloaded all files in the list")


if __name__ == "__main__":
    down('OMI_subset_2005.txt')
