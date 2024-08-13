import numpy as np
import pandas as pd
import chardet
import hashlib
import json
import os
import requests
import signal
import socket
import ssl
import tldextract
import urllib3
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

INPUT_CSV = "<your input csv file path>"
OUPUT_DIR = "<your directory path to save the hashed file>"
OUPUT_CSV_FILE = "<your input csv file name>"
TIMEOUT_OUPUT_CSV = "<path to file keeping track of urls that are timing out>"

def get_domain_dot_tld(url):
    extracted = tldextract.extract(url)
    return extracted.domain + "." + extracted.suffix

class WebpageContentGrabber:
    def __init__(self, domain_limit=1000):
        self.domain_count = {}
        self.already_parsed_paths = set()
        self.domains_not_resolving = set()
        self.urls_not_fully_scraped = set()
        self.HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36 Edg/92.0.902.67",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "en-US,en;q=0.9"
        }
        self.retry_strategy = Retry(
            total=0
        )
        self.adapter = HTTPAdapter(max_retries=self.retry_strategy)
        self.session = requests.Session()
        self.session.mount("https://", self.adapter)
        self.session.mount("http://", self.adapter)

    def get_not_fully_scraped(self):
        return self.urls_not_fully_scraped 

    def get_ssl_cert(self, url):
        scheme = urllib3.util.parse_url(url).scheme
        if scheme == "http":
            return None
        domain = get_domain_dot_tld(url)
        context = ssl.create_default_context()
        try:
            with socket.create_connection((domain, 443)) as sock:
                with context.wrap_socket(sock, server_hostname=domain) as ssock:
                    certificate = ssock.getpeercert()
                    return certificate
        except:
            return ""

    def get_robots_txt(self, url):   
        domain = get_domain_dot_tld(url) 
        scheme = urllib3.util.parse_url(url).scheme
        try:
            robots_url = scheme + "://" + domain + "/robots.txt"
            resp = self.session.get(robots_url, headers=self.HEADERS, timeout=(3,10))
            resp.raise_for_status()
            if resp.status_code != 200:
                return None
            return resp.text 
        except requests.ReadTimeout:
            self.urls_not_fully_scraped.add(domain)
            print(url + " is not fully scraped")
            return None
        except requests.RequestException as e:
            return None
        except:
            return None

    def get_response(self, url):
        try:
            resp = self.session.get(url, headers=self.HEADERS, timeout=(3,10))
            resp.raise_for_status()
            if resp.status_code != 200:
                return None
            encoding = chardet.detect(resp.content)['encoding']
            resp.encoding = encoding if encoding else "utf-8"
            return resp.text if resp.status_code == 200 else ""
        except requests.ReadTimeout:
            self.urls_not_fully_scraped.add(url)
            print(url + " is not fully scraped")
            return ""
        except requests.RequestException as e:
            return ""
        except:
            return ""

    def get_external_urls(self, soup, tag, attrs, rel=None):
        for element in soup.find_all(tag, rel=rel):
            if attrs in element.attrs:
                try:
                    yield element[attrs]
                except:
                    pass

    def get_content(self, soup, tag):
        for element in soup.find_all(tag):
            try:
                yield element.text
            except:
                pass

    def timeout_handler(self, signum, frame):
        raise TimeoutError

    def get_webpage(self, url, timeout=60):
        domain = get_domain_dot_tld(url)
        if domain in self.domains_not_resolving:
            return None
        
        if timeout:
            signal.signal(signal.SIGALRM, self.timeout_handler)
            signal.alarm(timeout)
        
        try:
            resp = self.session.get(url, headers=self.HEADERS, timeout=(3,10))

            resp.raise_for_status()
            if resp.status_code != 200:
                return None
            encoding = chardet.detect(resp.content)['encoding']
            resp.encoding = encoding if encoding else "utf-8"
            soup = BeautifulSoup(resp.text, "lxml")
            headers = resp.headers
            robots_txt = self.get_robots_txt(url)
            ssl_cert = self.get_ssl_cert(url)
        except requests.ReadTimeout:
            self.urls_not_fully_scraped.add(url)
            print(url + " is not fully scraped")
            return None
        except TimeoutError as e:
            print(e)
            self.urls_not_fully_scraped.add(url)
            return None
        except requests.RequestException as e:
            # print(e)
            self.domains_not_resolving.add(domain)
            return None
        except Exception as e:
            # print(e)
            return None
        finally:
            if timeout:
                signal.alarm(0)

        if os.path.exists(os.path.join(OUPUT_DIR, filename)):
            return filename    
        html = str(soup)
        javascript = ' '.join([script.text for script in soup.find_all('script') if script.text])
        css = ' '.join([style.text for style in soup.find_all('style') if style.text])

        external_css_list = []
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(self.get_response, external_url): external_url for external_url in self.get_external_urls(soup, "link", "href", rel="stylesheet")}
            for future in as_completed(futures):
                external_css_resp = futures[future]
                try:
                    external_css_content = future.result(timeout=30)
                    if external_css_content:
                        external_css_list.append(external_css_content)
                except TimeoutError:
                    self.urls_not_fully_scraped.add(url)
                    print("Timed out on processing external css for " + external_css_url)
                    continue
                except requests.RequestException as e:
                    print(e)
                    self.domains_not_resolving.add(domain)
                    continue
                except:
                    continue

        external_javascript_list = []
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(self.get_response, external_url): external_url for external_url in self.get_external_urls(soup, "script", "src")}
            for future in as_completed(futures):
                external_javascript_url = futures[future]
                try:
                    external_javascript_content = future.result(timeout=30)
                    if external_javascript_content:
                        external_javascript_list.append(external_javascript_content)
                except TimeoutError:
                    self.urls_not_fully_scraped.add(external_javascript_url)
                    print("Timed out on processing external javascript for " + external_javascript_url)
                    continue
                except requests.RequestException as e:
                    print(e)
                    self.domains_not_resolving.add(domain)
                    continue
                except:
                    continue
        
        external_javascript = " ".join(external_javascript_list)
        external_css = " ".join(external_css_list)
        
        webpage_json = {
            "html": html,
            "css": css,
            "javascript": javascript,
            "external_css": external_css,
            "external_javascript": external_javascript,
            "headers": str(headers),
            "robots_txt": robots_txt,
            "ssl_cert": ssl_cert
        }

        if not os.path.exists(os.path.join(OUPUT_DIR, filename)):
            with open(os.path.join(OUPUT_DIR, filename), "w") as file:
                json.dump(webpage_json, file, indent=6)

        return filename

BATCH_SIZE = 1000
df = pd.read_csv(INPUT_CSV)
webpage_scrape = WebpageContentGrabber()

batches = np.array_split(df, max(len(df) / BATCH_SIZE, 1))

for batch in batches:
    batch['webpage_scrape_file'] = batch['url'].apply(webpage_scrape.get_webpage)
    with open(OUPUT_DIR + OUTPUT_CSV_FILE, "a") as file:
        header = file.tell() == 0
        # Heartbeat
        print(batch.iloc[0]['url'])
        batch.to_csv(file, index=False, header=header, mode="a")
with open(TIMEOUT_OUPUT_CSV, "a") as file:
    header = file.tell() == 0
    pd.DataFrame(webpage_scrape.get_not_fully_scraped(), columns=['url']).to_csv(file, index=False, header=header, mode="a")
