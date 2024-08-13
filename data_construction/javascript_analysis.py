import ast
import boto3
import hashlib
import json
import os
import re
import statistics
from dotenv import load_dotenv
import pandas as pd

INPUT_CSV = "<your input csv file path>"
OUPUT_CSV = "<your output csv file path>"

class JavasScriptAnalysis:
    def __init__(self):
        load_dotenv()
        self.FEATURE_KEY = "javascript"
        self.BUCKET = os.getenv("BUCKET")
        self.PATH = os.getenv("FOLDER_PATH")

    def get_js_from_s3(self, url):
        filename = str(hashlib.sha256(url.encode('utf-8')).hexdigest()) + ".json"
        s3 = boto3.client("s3")
        try:
            s3.download_file(self.BUCKET, self.PATH + filename, filename)
        except Exception as e:
            # print(e)
            return None
        
        with open(filename) as file:
            try:
                webpage_scrape = json.load(file)
            except Exception as e:
                print(e)
                return None
        
        os.remove(filename)

        return webpage_scrape[self.FEATURE_KEY]
    
    def get_external_js_from_s3(self, url):
        filename = str(hashlib.sha256(url.encode('utf-8')).hexdigest()) + ".json"
        s3 = boto3.client("s3")
        try:
            s3.download_file(self.BUCKET, self.PATH + filename, filename)
        except Exception as e:
            # print(e)
            return None
        
        with open(filename) as file:
            try:
                webpage_scrape = json.load(file)
            except Exception as e:
                print(e)
                return None

        return webpage_scrape["external_" + self.FEATURE_KEY]

    def get_function_count(self, js):
        full_paren = len(re.findall("\(([^\)]+)\)", js))
        empty_paren = len(js.split('()'))
        return full_paren + empty_paren

    def get_malicious_function_count(self, content):
        function_list = {
            'setcookie', 'getcookie', 'createxmlhttprequest', 'unescape',
            'document.write', 'element.appendchild', 'dateobject.togmtstring',
            'new activexobject', 'document.createelement', 'getappname',
            'getuseragent', 'window.setinterval', 'window.settimeout',
            'location.assign', 'location.replace', 'eval()', 'string.indexof',
            'string.fromcharcode', 'string.charat', 'string.split',
            'string.charcodeat', 'document.writeln', 'document.appendchild',
            'element.innerhtml'
        }

        split_content = content.split(' ')
        counter = 0
        for element in split_content:
            if any(m_function in element.lower() for m_function in function_list):
                counter += 1

        return counter

    def get_max_array_length(self, js):
        array_lengths = re.findall('\(([^\)]+)\)', js)
        if array_lengths == []:
            return 0
        return max([len(i) for i in array_lengths])

    def get_avg_array_length(self, js):
        array_lengths = re.findall('\(([^\)]+)\)', js)
        if array_lengths == []:
            return 0
        return statistics.mean([len(i) for i in array_lengths])

    def get_browser_function_count(self, js):
        bom_objects = ["window.open", "window.close", "window.moveTo", "window.resizeTo", "window.innerWidth", "window.innerHeight" 
            "screen.", "location.", "history.", "navigator.", 
            "alert", "confirm", "prompt", "setTimeout", "clearTimeout", "setInterval", "clearInterval", "cookies"]
        counter = 0
        split_js = js.replace('\n', ' ').split(' ')
        for element in split_js:
            for object in bom_objects:
                if object in element.lower():
                    counter += 1
        return counter

    def get_document_function_count(self, js):
        counter = 0
        split_js = js.replace('\n', ' ').split(' ')
        for element in split_js:
            if "document." in element.lower():
                counter += 1
        return counter

javascript_analysis = JavasScriptAnalysis()
df = pd.read_csv(INPUT_CSV)
df = pd.DataFrame(df['url'])
df['js'] = df['url'].apply(javascript_analysis.get_js_from_s3)
df['js'] = df['js'].fillna("")
df['js_length'] = df['js'].apply(len)
df['js_function_count'] = df['js'].apply(javascript_analysis.get_function_count)
df['malicious_js_function_count'] = df['js'].apply(javascript_analysis.get_malicious_function_count)
df['max_array_length'] = df['js'].apply(javascript_analysis.get_max_array_length)
df['average_array_length'] = df['js'].apply(javascript_analysis.get_avg_array_length)
df['bom_js_function_count'] = df['js'].apply(javascript_analysis.get_browser_function_count)
df['dom_js_function_count'] = df['js'].apply(javascript_analysis.get_document_function_count)
df['external_js'] = df['url'].apply(javascript_analysis.get_external_js_from_s3)
df['external_js'] = df['external_js'].fillna("")
df['external_js_length'] = df['external_js'].apply(len)
df['external_js_function_count'] = df['external_js'].apply(javascript_analysis.get_function_count)
df['external_malicious_js_function_count'] = df['external_js'].apply(javascript_analysis.get_malicious_function_count)
df['external_max_array_length'] = df['external_js'].apply(javascript_analysis.get_max_array_length)
df['external_average_array_length'] = df['external_js'].apply(javascript_analysis.get_avg_array_length)
df['external_bom_js_function_count'] = df['external_js'].apply(javascript_analysis.get_browser_function_count)
df['external_dom_js_function_count'] = df['external_js'].apply(javascript_analysis.get_document_function_count)
df = df.drop(['js', 'external_js'], axis=1)
df.to_csv(OUPUT_CSV , index=False)
