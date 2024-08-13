import ast
import boto3
import hashlib
import json
import os
import re
import swifter
import tldextract
from dotenv import load_dotenv
import pandas as pd
import numpy as np

INPUT_CSV = "<your input csv file name>"
OUPUT_CSV = "<your output csv file path>"

def get_domain_dot_tld(url):
    extracted = tldextract.extract(url)
    return extracted.domain + "." + extracted.suffix

class Robots_txt_Analysis:
    def __init__(self):
        load_dotenv()
        self.FEATURE_KEY = "robots_txt"
        self.BUCKET = os.getenv("BUCKET")
        self.PATH = os.getenv("FOLDER_PATH")

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

        try:
            return ast.literal_eval(webpage_scrape[self.FEATURE_KEY])
        except:
            return None
                        
        return None

    def get_length(self, robots_txt):
        if not robots_txt:
            return None

        return len(robots_txt)

    def get_counts(self, robots_txt):
        if not robots_txt:
            return None

        disallow_rule_count = 0
        allow_rule_count = 0 
        user_agent_count = 0
        comment_count = 0
        sitemap_count = 0

        for line in robots_txt.splitlines():
            line = line.lower().strip()
            if line.startswith("disallow:"):
                disallow_rule_count += 1
            elif line.startswith("allow:"):
                allow_rule_count += 1
            elif line.startswith("user-agent:"):
                user_agent_count += 1
            elif line.startswith("#"):
                comment_count += 1
            elif line.startswith("sitemap:"):
                sitemap_count += 1

        return disallow_rule_count, allow_rule_count, user_agent_count, comment_count, sitemap_count

    def disallows_root(self, robots_txt):
        if not robots_txt:
            return None

        for line in robots_txt.splitlines():
            line = line.lower().strip()
            if line == "disallow: /":
                return True

            return False

df = pd.read_csv(INPUT_CSV)
robots_txt_analysis = Robots_txt_Analysis()
df['domain'] = df['url'].swifter.apply(get_domain_dot_tld)
if 'robots_txt' not in df.columns:
    df['robots_txt'] = None
df['robots_txt'] = df.swifter.apply(lambda row: robots_txt_analysis.get_from_s3(row['url']) if pd.isna(row['robots_txt']) else row['robots_txt'], axis=1)
df['robots_txt_len'] = df['robots_txt'].swifter.apply(robots_txt_analysis.get_length)
df[['disallow_rule_count', 'allow_rule_count', 'user_agent_count', 'comment_count', 'sitemap_count']] = df['robots_txt'].swifter.apply(lambda robots_txt: pd.Series(robots_txt_analysis.get_counts(robots_txt)))
df['disallows_root'] = df['robots_txt'].swifter.apply(robots_txt_analysis.disallows_root)
df.to_csv(OUPUT_CSV, index=False)