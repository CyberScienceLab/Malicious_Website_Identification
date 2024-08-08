import re
import urllib
import urllib3
import tldextract
import pandas as pd
import numpy as np
from scipy.stats import entropy
from collections import Counter
from urllib.parse import urlparse

INPUT_CSV = "<your input csv file name>"
OUPUT_CSV = "<your output csv file path>"
TLD_PRICES_CSV = "<list of tlds and their full prices in USD>"

class UrlAnalysis:
    def __init__(self, tld_prices):
        self.tld_prices = tld_prices
    
    def get_tld(self, url):
        extracted = tldextract.extract(url)
        return extracted.suffix

    def get_domain_dot_tld(self, url):
        extracted = tldextract.extract(url)
        return extracted.domain + "." + extracted.suffix

    def get_num_tlds(self, url, current_tld_prices=None):
        if current_tld_prices:
            self.tld_prices = current_tld_prices
        count = 0
        hostname = urllib3.util.parse_url(url).hostname
        if not hostname:
            return count
        splits = hostname.split(".")
        for split in splits:
            split = "." + split
            count += self.tld_prices['TLD'].str.contains(split).any()
        return count

    def get_subdomains(self, url):
        extracted = tldextract.extract(url)
        return extracted.subdomain

    def get_tld_manager(self, url):
        tlds = pd.read_html("https://www.iana.org/domains/root/db")[0]
        extracted = tldextract.extract(url)
        if not extracted or not extracted.suffix:
            return None

        splits = extracted.suffix.split(".")
        if not splits:
            return None

        tld_manager = tlds.loc[tlds['Domain'] == "." + splits[-1], 'TLD Manager']
        return tld_manager.values[0] if tld_manager else None

    def get_tld_price(self, url, current_tld_prices=None):
        if current_tld_prices:
            self.tld_prices = current_tld_prices

        extracted = tldextract.extract(url)
        tld = extracted.suffix 
        row = self.tld_prices[self.tld_prices["TLD"] == "." + tld].values
        if len(row) < 1:
            return None, None, None, None
            
        tld_register_price, tld_renew_price, tld_transfer_price, tld_ICANN_fee = row[0].tolist()[1:]
        return tld_register_price, tld_renew_price, tld_transfer_price, tld_ICANN_fee
    
    def contains_pe_extension(self, url):
        pe_extensions = {".acm", ".ax", ".cpl", ".dll", ".drv", ".efi", ".exe", ".mui", ".ocx", ".scr", ".sys", ".tsp", ".mun"}
        parsed_url = urlparse(url)
        path = parsed_url.path
        if not path:
            return False

        for extension in pe_extensions:
            if path.endswith(extension):
                return True

        return False
    
    def is_shortened_url(self, url):
        http = urllib3.PoolManager()
        try:
            resp = http.urlopen('GET', url)
        except:
            return False
            
        url = urllib3.util.parse_url(url).host.replace("www.", "")
        final_url = urllib3.util.parse_url(resp.geturl()).host.replace("www.", "")
        if len(final_url) > len(url):
            return True

        return False
    
    def calculate_entropy(self, url):
        counts = Counter(url)
        return entropy(list(counts.values()), base=2)

    def urls_have_ips(self, url):
        split_string = re.split('/|.com', url)
        for word in split_string:
            try:
                IP(word)
                return True
            except:
                pass
            
        return False

    def get_number_subdomains(self, url_string):
        removed_http = url_string.replace('http://', '').replace('https://', '')
        sub_array = removed_http.split('/')[0].replace('.com','').split('.')
        return len(sub_array) - 1

    def get_hostname(self, url):
        parsed_url = urllib.parse.urlparse(url)
        return parsed_url.netloc

    def get_ratio_digits_url(self, url):
        try:
            return sum(c.isdigit() for c in url)/len(url)
        except:
            return 0

    def check_at_symbol(self, url):
        if '@' in url:
            return 1
        return 0
    
    def get_unique_characters(self, url):
        return len(''.join(set(url)))

    def get_unique_letters(self, url):
        return len(''.join([i for i in set(url) if not i.isdigit()]))

    def get_unique_numbers(self, url):
        return len(''.join([i for i in set(url) if i.isdigit()]))
    
df = pd.read_csv(INPUT_CSV)
tld_prices = pd.read_csv(TLD_PRICES_CSV)
url_analysis = UrlAnalysis(tld_prices)
df['domain'] = df['url'].apply(url_analysis.get_domain_dot_tld)
df['hostname'] = df['url'].apply(url_analysis.get_hostname)
df['url_length'] = df['url'].apply(len)
df['subdomain'] = df['url'].apply(url_analysis.get_subdomains)
df['tld'] = df['url'].apply(url_analysis.get_tld)
df[['tld_register_price', 'tld_renew_price', 'tld_transfer_price', 'tld_ICANN_fee']] = df['url'].apply(lambda url: pd.Series(url_analysis.get_tld_price(url)))
df['num_tlds_in_url'] = df['url'].apply(url_analysis.get_num_tlds)
df['contains_pe_extension'] = df['url'].apply(url_analysis.contains_pe_extension)
df['url_entropy'] = df['url'].apply(url_analysis.calculate_entropy)
df['number_subdomains'] = df['url'].apply(url_analysis.get_number_subdomains)
df['ratio_digits_url'] = df['url'].apply(url_analysis.get_ratio_digits_url)
df['having_@_in_url'] = df['url'].apply(url_analysis.check_at_symbol)
df['ratio_digits_hostname'] = df['domain'].apply(url_analysis.get_ratio_digits_url)
df['unique_url_chars'] = df['url'].apply(url_analysis.get_unique_characters)
df['unique_url_nums'] = df['url'].apply(url_analysis.get_unique_numbers)
df['unique_url_letters'] = df['url'].apply(url_analysis.get_unique_letters)
df['ratio_let_chars'] = df['unique_url_nums']/df['unique_url_chars']
df['ratio_nums_chars'] = df['unique_url_letters']/df['unique_url_chars']
df['has_IP_in_url'] = df['url'].apply(lambda x: 1 if url_analysis.urls_have_ips(x) else 0)
df['hostname_length'] = df['url'].apply(lambda x: len(url_analysis.get_hostname(x)))
df['number_underscores'] = df['url'].apply(lambda x: x.count('_'))
df['num_semicolons'] = df['url'].apply(lambda url: str(url).count(';'))
df['num_zeros'] = df['url'].apply(lambda url: str(url).count('0'))
df['num_spaces'] = df['url'].apply(lambda url: str(url).count('%20'))
df['num_hyphens'] = df['url'].apply(lambda url: str(url).count('-'))
df['num_@s'] = df['url'].apply(lambda url: str(url).count('@'))
df['num_queries'] = df['url'].apply(lambda url: str(url).count('?'))
df['num_ampersands'] = df['url'].apply(lambda url: str(url).count('&'))
df['num_equals'] = df['url'].apply(lambda url: str(url).count('='))
df.to_csv(OUPUT_CSV)

