import re
import urllib
import urllib3
import tldextract
import pandas as pd
import numpy as np
from scipy.stats import entropy
from collections import Counter
from urllib.parse import urlparse

INPUT_CSV = "<your input csv file path>"
OUPUT_CSV = "<your output csv file path>"

class DomainAnalysis():
    
    def get_domain_dot_tld(self, url):
        extracted = tldextract.extract(url)
        return extracted.domain + "." + extracted.suffix

    def get_tld(self, url):
        extracted = tldextract.extract(url)
        return extracted.suffix

    def calculate_entropy(self, domain):
        counts = Counter(domain)
        return entropy(list(counts.values()), base=2)

    def get_ratio_digits(self, domain):
        try:
            return sum(c.isdigit() for c in domain)/len(domain)
        except:
            return 0

df = pd.read_csv(INPUT_CSV)
domain_analysis = DomainAnalysis()
df['domain'] = df['url'].apply(domain_analysis.get_domain_dot_tld)
df['tld'] = df['domain'].apply(domain_analysis.get_tld)
df['domain_entropy'] = df['domain'].apply(domain_analysis.calculate_entropy)
df['domain_length'] = df['domain'].apply(lambda x: len(domain_analysis.get_domain_dot_tld(x)))
df['ratio_digits_domain'] = df['domain'].apply(domain_analysis.get_ratio_digits)
df.to_csv(OUPUT_CSV)
