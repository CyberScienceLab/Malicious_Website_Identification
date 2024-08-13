import ast
import boto3
import hashlib
import json
import os
import re
import pandas as pd
import numpy as np
from dotenv import load_dotenv

INPUT_CSV = "<your input csv file path>"
OUPUT_CSV = "<your output csv file path>"

class CSSAnalysis:
    def __init__(self):
        load_dotenv()
        self.FEATURE_KEY = "css"
        self.BUCKET = os.getenv("BUCKET")
        self.PATH = os.getenv("FOLDER_PATH")
    
    def get_webpage_scrape_from_s3(self, url):
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

        return webpage_scrape

    def get_css_from_s3(self, url):
        webpage_scrape = self.get_webpage_scrape_from_s3(url)
        if not webpage_scrape:
            return ""

        return webpage_scrape[self.FEATURE_KEY]
    
    def get_external_css_from_s3(self, url):
        webpage_scrape = self.get_webpage_scrape_from_s3(url)
        if not webpage_scrape:
            return None

        return webpage_scrape['external_' + self.FEATURE_KEY]
    
    def get_hidden_css_count(self, css):
        HIDDEN_CSS_STRINGS = ["display:none", "visibility:hidden", "opacity:0"]
        count = 0
        for hidden_css_string in HIDDEN_CSS_STRINGS:
            count += css.count(hidden_css_string)

        return count

css_analysis = CSSAnalysis()
df = pd.read_csv(INPUT_CSV)
df = pd.DataFrame(df['url'])
df['css'] = df['url'].apply(css_analysis.get_css_from_s3)
df['css'] = df['css'].fillna("")
df['css_len'] = df['css'].apply(len)
df['hidden_css_count'] = df['css'].apply(css_analysis.get_hidden_css_count)
df['external_css'] = df['url'].apply(css_analysis.get_external_css_from_s3)
df['external_css'] = df['external_css'].fillna("")
df['external_css_len'] = df['external_css'].apply(len)
df['hidden_external_css_count'] = df['external_css'].apply(css_analysis.get_hidden_css_count)
df.drop(['css', 'external_css'], axis=1)
df.to_csv(OUPUT_CSV, index=False)
