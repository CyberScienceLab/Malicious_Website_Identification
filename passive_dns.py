import json
import requests
import signal
import tldextract
import numpy as np
import pandas as pd

INPUT_CSV = "<your input csv file name>"
OUPUT_CSV = "<your output csv file path>"

def get_domain_dot_tld(url):
    extracted = tldextract.extract(url)
    return extracted.domain + "." + extracted.suffix

class OTXv2Grabber():
    def __init__(self, timeout_time=30):
        self.PASSIVE_DNS_API_URL = "https://otx.alienvault.com/otxapi/indicators/domain/passive_dns/"
        self.already_grabbed_passive_dns = {}

    def get_passive_dns(self, domain):
        if domain in self.already_grabbed_passive_dns:
            return self.already_grabbed_passive_dns[domain]
            
        try:
            resp = requests.get(self.PASSIVE_DNS_API_URL + domain)
            passive_dns = json.loads(resp.text)['passive_dns']
            for entry in passive_dns:
                entry.pop('indicator_link')
                entry.pop('flag_url')
                entry.pop('whitelisted_message')
            self.already_grabbed_passive_dns[domain] = json.dumps(passive_dns)
            return passive_dns
        except:
            return None
        finally:
            signal.alarm(0)
            
BATCH_SIZE = 10000
df = pd.read_csv(INPUT_CSV)
df = df.dropna(subset=['webpage_scrape_file'])
df['domain'] = df['url'].apply(get_domain_dot_tld)
df = df.drop_duplicates(subset=['domain'])
otx_grabber = OTXv2Grabber()

batches = np.array_split(df, max(len(df) / BATCH_SIZE, 1))
for batch in batches:
    batch['passive_dns'] = batch['domain'].apply(otx_grabber.get_passive_dns)
    with open(OUPUT_CSV, "a") as file:
        header = file.tell() == 0
        # Heartbeat
        print(batch.iloc[0]['url'])
        batch.to_csv(file, index=False, header=header, mode="a")
