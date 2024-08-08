import dns
import geoip2.database
import whois
import pandas as pd
import json
import ipaddress
import os
import requests
import swifter
import tldextract
from dns import resolver
from dotenv import load_dotenv

INPUT_CSV = "<your input csv file path>"
OUPUT_CSV = "<your output csv file path>"
DATABASE_FILE = "<Database file containing mappings for ip addresses to geolocation (e.g. GeoLite2-Country.mmbd)>"

class HostAnalysis:

    def __init__(self, database_file):
        load_dotenv()
        self.database_file = database_file
        self.HEADERS = {
            "Accept": "application/json",
            "Authorization": os.getenv("XFORCE_AUTHORIZATION_TOKEN")
        }

    def get_domain_dot_tld(self, url):
        extracted = tldextract.extract(url)
        return extracted.domain + "." + extracted.suffix

    def is_whois_complete(self, registrar):
        if registrar is None:
            return False

        return len(registrar) > 0
    
    def get_ip_address(self, domain):
        try:
            result = dns.resolver.resolve(domain, 'A')
            ip_addresses = []
            for ip_addr in result:
                try:
                    ipaddress.ip_address(ip_addr)
                except:
                    continue
                ip_addresses.append(str(ip_addr))

            return ip_addresses
        except dns.resolver.NXDOMAIN:
            return None
        except:
            return None

    def get_location_from_ip(self, ip_addresses):
        with (geoip2.database.Reader(self.database_file)) as reader:
            if len(ip_addresses) == 0:
                return "Unknown"
            for ip_address in ip_addresses:
                response = reader.country(ip_address)
                if response and response.country:
                    return response.country.name
            return "Unknown"

    def get_registrar(self, url):
        try: 
            resp = requests.get("https://api.xforce.ibmcloud.com/api/whois/" + url, headers=self.HEADERS)
        except Exception as e:
            print(e)
            return None

        if resp.status_code != 200:
            return False 
        
        if len(resp.text.strip()) == 0:
            return False

        try:
            whois = json.loads(resp.text)
        except:
            return None

        if 'registrarName' in whois:
            return whois['registrarName']
        return None

df = pd.read_csv(INPUT_CSV)
df = pd.DataFrame(df['url'])
host_analysis = HostAnalysis(database_file=DATABASE_FILE)
df['domain'] = df['url'].swifter.apply(host_analysis.get_domain_dot_tld)
df['registrar'] = df['url'].swifter.apply(host_analysis.get_registrar)
df['whois_complete'] = df['registrar'].swifter.apply(host_analysis.is_whois_complete)
df['ip_address'] = df['domain'].swifter.apply(host_analysis.get_ip_address)
df['location'] = df['ip_address'].swifter.apply(host_analysis.get_location_from_ip)
df.to_csv(OUPUT_CSV, index=False)
