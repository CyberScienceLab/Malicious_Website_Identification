from bs4 import BeautifulSoup
import ast
import boto3
import hashlib
import json
import os
import re
import string
import tldextract
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import signal
from contextlib import contextmanager

INPUT_CSV = "<your input csv file path>"
OUPUT_CSV = "<your output csv file path>"

def get_domain_dot_tld(url):
    extracted = tldextract.extract(url)
    return extracted.domain + "." + extracted.suffix


@contextmanager
def timeout(time):
    # Register a function to raise a TimeoutError on the signal.
    signal.signal(signal.SIGALRM, raise_timeout)
    # Schedule the signal to be sent after ``time``.
    signal.alarm(time)

    try:
        yield
    except TimeoutError:
        return 'timeout error'
    finally:
        # Unregister the signal so it won't be triggered
        # if the timeout is not reached.
        signal.signal(signal.SIGALRM, signal.SIG_IGN)

def raise_timeout(signum, frame):
    raise TimeoutError

def load_easylist():
    try:
        resp = requests.get("https://easylist.to/easylist/easylist.txt")
    except:
        return []

    if resp.status_code != 200:
        return []

    easylist = resp.text
    filter_rules = easylist.splitlines()
    return [filter_rule.strip() for filter_rule in filter_rules if filter_rule and not filter_rule.startswith(("!", "[", "@@"))]
    

class HTMLAnalysis:
    def __init__(self):
        load_dotenv()
        self.FEATURE_KEY = "html"
        self.BUCKET = os.getenv("BUCKET")
        self.PATH = os.getenv("FOLDER_PATH")
        self.RULES = load_easylist()

    def get_from_s3(self, url):
        filename = str(hashlib.sha256(url.encode('utf-8')).hexdigest()) + ".json"
        s3 = boto3.client("s3")
        try:
            s3.download_file(self.BUCKET, self.PATH + filename, filename)
        except Exception as e:
            print(e)
            return None
        
        with open(filename) as file:
            try:
                webpage_scrape = json.load(file)
            except Exception as e:
                print(e)
                return None
                
            if self.FEATURE_KEY not in webpage_scrape.keys():
                return None
        
        os.remove(filename)
                
        return webpage_scrape[self.FEATURE_KEY]
  
    def find_urls(self, string, unique=False):
        with timeout(1):
            regex = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"
            url = re.findall(regex, string)
            if unique:
                return len(set(url))
            return len(url)

    def check_hex(self, value): 
        for letter in value: 
                
            # If anything other than hexdigit 
            # letter is present, then return 
            # False, else return True 
            if letter not in string.hexdigits: 
                return False
        return True

    def get_tag_srcs(self, html, tag_name):
        try:
            soup = BeautifulSoup(html, "lxml")
        except:
            return []
        tags = soup.find_all(tag_name)
        tag_srcs = [tag.get('src') for tag in tags]
        return tag_srcs if tag_srcs else []

    def get_ood_img_srcs(self, html, domain):
        img_srcs = self.get_tag_srcs(html, "img")
        img_domains = {get_domain_dot_tld(str(img_src)) for img_src in img_srcs}
        ood_srcs = img_domains.remove(domain) if domain in img_domains else img_domains
        ood_srcs = ood_srcs.remove(".") if ood_srcs and "." in ood_srcs else ood_srcs
        return list(ood_srcs) if ood_srcs else []

    def get_script_reference_count(self, html):
        return html.count('<script')/2
    
    def is_ad(self, element):
        if element or not element.name:
            return False

        for attr in element.attrs:
            for rule in self.RULES:
                if rule.startswith("##") and rule[2:] in element.get(attr, ""):
                    return True 
        
        return False

    def get_ad_count(self, html):
        soup = BeautifulSoup(html, "lxml")
        ad_count = 0

        for element in soup.find_all():
            if self.is_ad(element):
                ad_count += 1
        
        return ad_count

html_analysis = HTMLAnalysis()
df = pd.read_csv(INPUT_CSV)
df = df.dropna(subset=['webpage_scrape_file'])
df = pd.DataFrame(df['url'])
df['domain'] = df['url'].apply(get_domain_dot_tld)
df['html'] = df['url'].apply(html_analysis.get_from_s3)
df['html'] = df['html'].fillna("")
df['html_len'] = df['html'].apply(len)
df['script_tag_count'] = df['html'].apply(html_analysis.get_script_reference_count)

df['total_urls_in_html_count'] = df['html'].apply(
    lambda js: html_analysis.find_urls(str(js), False)
)

df['unique_urls_in_html_count'] = df['html'].apply(
    lambda js: html_analysis.find_urls(str(js), True)
)

existence_of_hex = np.zeros((len(df)))
hex_length = np.zeros((len(df)))

for i in range(len(df)):
    split_array = df['html'].iloc[i].split(' ')
    for array in split_array:
        if html_analysis.check_hex(array):
            existence_of_hex[i] = 1
            hex_length[i] += len(array)

df['hex_len'] = hex_length
df['has_hex'] = existence_of_hex
df['img_srcs_count'] = df.apply(lambda row: len(html_analysis.get_tag_srcs(row['html'], "img")), axis=1)
df['ood_img_srcs'] = df.apply(lambda row: html_analysis.get_ood_img_srcs(row['html'], row['domain']), axis=1)
df['ood_img_srcs'] = df['ood_img_srcs'].apply(lambda ood_img_srcs: [] if pd.isna(ood_img_srcs) else ood_img_srcs)
df['ood_img_srcs_count'] = df['ood_img_srcs'].apply(len)
df['ad_count'] = df['html'].apply(html_analysis.get_ad_count)
df = df.drop(['html', 'ood_img_srcs'], axis=1)
df.to_csv(OUPUT_CSV, index=False)
