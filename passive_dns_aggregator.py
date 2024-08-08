import ast
from dotenv import load_dotenv
import pandas as pd
import boto3
import os

INPUT_CSV_FILE = "<your input csv file name>"
OUPUT_CSV = "<your output csv file path>"

class PassiveDNSAggregator:
    def __init__(self, num_parts):
        load_dotenv()
        self.BUCKET = os.getenv("BUCKET")
        self.PATH_FRAGMENT = os.getenv("FOLDER_PATH")
        self.NUM_PARTS = num_parts
    
    def str_to_dict(self, s):
        try: 
            passive_dns = ast.literal_eval(s)
        except:
            try:
                passive_dns = json.loads(s)
            except:
                return None

        if not passive_dns:
            return None
            
        return [entry for entry in passive_dns]

    def aggregate(self):
        s3 = boto3.client('s3')
        passive_dns_parts = []

        for i in range(self.NUM_PARTS):
            s3.download_file(self.BUCKET, self.PATH_FRAGMENT + INPUT_CSV_FILE, INPUT_CSV_FILE)
            passive_dns_parts.append(pd.read_csv(INPUT_CSV_FILE).dropna(subset=['passive_dns']))
        passive_dns = pd.concat(passive_dns_parts , ignore_index=True)
        passive_dns = passive_dns.drop_duplicates()
        self.passive_dns = passive_dns

    def normalize(self, passive_dns=None):
        if passive_dns is not None:
            self.passive_dns = passive_dns

        if self.passive_dns is None:
            raise Exception("Need Passive DNS")
            
        normalized_passive_dns = []
        for i, row in self.passive_dns.iterrows():
            if not row.any() or not row['passive_dns']:
                continue

            passive_dns_list = self.str_to_dict(row['passive_dns'])
            if not passive_dns_list:
                continue
            
            for item in passive_dns_list:
                if item is None:
                    continue
                
                item['domain'] = row['domain']
                normalized_passive_dns.append(item)
        return normalized_passive_dns

passive_dns_aggregator = PassiveDNSAggregator(36)
passive_dns_aggregator.aggregate()
passive_dns = pd.DataFrame(passive_dns_aggregator.normalize())
passive_dns = passive_dns.drop_duplicates()
passive_dns.to_csv(OUPUT_CSV, index = False)
