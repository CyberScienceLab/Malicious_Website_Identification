import ast
import json
import boto3
import tldextract
import os
import pandas as pd

INPUT_CSV = "<your input csv file name>"
OUPUT_CSV = "<your output csv file path>"
NORMALIZED_PASSIVE_DNS = "<your file containing normalized passive DNS gotten from passive_dns_aggregator.py>"

def get_domain_dot_tld(url):
    extracted = tldextract.extract(url)
    return extracted.domain + "." + extracted.suffix

def str_to_dict(s):
    print(s)
    if not s:
        return None 

    try: 
        passive_dns = ast.literal_eval(s)
    except Exception as e:
        print(e)
        try:
            passive_dns = json.loads(s)
        except:
            return None

    if not passive_dns:
        return None
        
    return [entry for entry in passive_dns]

def normalize_passive_dns(passive_dns):        
    normalized_passive_dns = []
    for i, row in passive_dns.iterrows():
        if not row.any() or not row['passive_dns']:
            continue

        passive_dns_list = str_to_dict(row['passive_dns'])
        if not passive_dns_list:
            continue
        
        for item in passive_dns_list:
            if item is None:
                continue
            
            item['domain'] = row['domain']
            normalized_passive_dns.append(item)
    return pd.DataFrame(normalized_passive_dns)

class PassiveDNSAnalysis:
    def __init__(self, passive_dns_df):
        self.passive_dns_df = passive_dns_df.drop_duplicates()
        self.lengths = {}
        self.unique_addresses_counts = {}
        self.unique_hostnames_counts = {}
        self.unique_countries_counts = {}
        self.suspicious_asn_counts = {}
        self.false_positive_asn_counts = {}
        self.asn_switch_counts = {}

    def get_len(self, domain):
        if domain in self.lengths:
            return self.lengths[domain]
        
        passive_dns_df = self.passive_dns_df
        passive_dns_df = passive_dns_df[passive_dns_df['domain'] == domain]
        length = len(passive_dns_df)
        self.lengths[domain] = length
        return length 

    def get_unique_addresses_count(self, domain):
        if domain in self.unique_addresses_counts:
            return self.unique_addresses_counts[domain]

        passive_dns_df = self.passive_dns_df
        passive_dns_df = passive_dns_df[passive_dns_df['domain'] == domain]
        unique_addresses_count = passive_dns_df['address'].nunique()
        self.unique_addresses_counts[domain] = unique_addresses_count
        return unique_addresses_count

    def get_unique_hostnames_count(self, domain):
        if domain in self.unique_hostnames_counts:
            return self.unique_hostnames_counts[domain]

        passive_dns_df = self.passive_dns_df
        passive_dns_df = passive_dns_df[passive_dns_df['domain'] == domain]
        unique_hostnames_count = passive_dns_df['hostname'].nunique()
        self.unique_hostnames_counts[domain] = unique_hostnames_count
        return unique_hostnames_count

    def get_unique_countries_count(self, domain):
        if domain in self.unique_countries_counts:
            return self.unique_countries_counts[domain]

        passive_dns_df = self.passive_dns_df
        passive_dns_df = passive_dns_df[passive_dns_df['domain'] == domain]
        unique_countries_count = passive_dns_df['flag_title'].nunique()
        self.unique_countries_counts[domain] = unique_countries_count
        return unique_countries_count

    def get_suspicious_asn_count(self, domain):
        if domain in self.suspicious_asn_counts:
            return self.suspicious_asn_counts[domain]
        
        passive_dns_df = self.passive_dns_df
        passive_dns_df = passive_dns_df[passive_dns_df['domain'] == domain]
        suspicious_count = (passive_dns_df['suspicious'] == True).sum()
        self.suspicious_asn_counts[domain] = suspicious_count
        return suspicious_count
    
    def get_false_positive_asn_count(self, domain):
        if domain in self.false_positive_asn_counts:
            return self.false_positive_asn_counts[domain]
        
        passive_dns_df = self.passive_dns_df
        passive_dns_df = passive_dns_df[passive_dns_df['domain'] == domain]
        false_positive_count = (passive_dns_df['whitelisted'] == True).sum()
        self.false_positive_asn_counts[domain] = false_positive_count
        return false_positive_count

    def get_asn_switch_count(self, domain):
        if domain in self.asn_switch_counts:
            return self.asn_switch_counts[domain]

        passive_dns_df = self.passive_dns_df
        passive_dns_df = passive_dns_df[passive_dns_df['domain'] == domain]
        passive_dns_df = passive_dns_df[passive_dns_df['record_type'] == "A"]
        passive_dns_df['first'] = pd.to_datetime(passive_dns_df['first'])
        passive_dns_df.sort_values(by='first') 
        passive_dns_df['asn_switch'] = passive_dns_df['asn'] != passive_dns_df['asn'].shift()
        asn_switch_count = passive_dns_df['asn_switch'].sum() - 1
        self.asn_switch_counts[domain] = asn_switch_count
        return asn_switch_count

df = pd.read_csv(INPUT_CSV)
df = df.dropna(subset=['webpage_scrape_file'])
passive_dns_df = pd.read_csv(NORMALIZED_PASSIVE_DNS)
passive_dns_df = passive_dns_df.dropna(subset=['passive_dns'])
if passive_dns_df['passive_dns'].any():
    passive_dns_df = normalize_passive_dns(passive_dns_df)
    passive_dns_analysis = PassiveDNSAnalysis(passive_dns_df)
    df['domain'] = df['url'].apply(get_domain_dot_tld)
    df['passive_dns_len'] = df['domain'].apply(passive_dns_analysis.get_len)
    df['unique_addresses_count'] = df['domain'].apply(passive_dns_analysis.get_unique_addresses_count)
    df['unique_hostnames_count'] = df['domain'].apply(passive_dns_analysis.get_unique_hostnames_count)
    df['unique_countries_count'] = df['domain'].apply(passive_dns_analysis.get_unique_countries_count)
    df['suspicious_asn_count'] = df['domain'].apply(passive_dns_analysis.get_suspicious_asn_count)
    df['false_positive_asn_count'] = df['domain'].apply(passive_dns_analysis.get_false_positive_asn_count)
    df['asn_switch_count'] = df['domain'].apply(passive_dns_analysis.get_asn_switch_count)
df.to_csv(OUPUT_CSV)