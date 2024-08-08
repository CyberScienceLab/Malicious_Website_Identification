import ast
from dotenv import load_dotenv
import pandas as pd
import boto3
import os

OUPUT_PREFIX = "<your output directory path>"

load_dotenv()
BUCKET = os.getenv("BUCKET")
PATH_FRAGMENT = os.getenv("PATH_FRAGMENT")
FILE_NAME = os.getenv("FILE_NAME")
NUM_PARTS = int(os.getenv("NUM_PARTS"))
s3 = boto3.client('s3')

print(FILE_NAME)
parts = []
for i in range(NUM_PARTS):
    s3.download_file(BUCKET, PATH_FRAGMENT + str(i + 1) + "/" + FILE_NAME, FILE_NAME + "_part" + str(i + 1) + ".csv")
    parts.append(pd.read_csv(FILE_NAME + "_part" + str(i + 1) + ".csv"))
    os.remove(FILE_NAME + "_part" + str(i + 1) + ".csv")

aggregated_features = pd.concat(parts , ignore_index=True)
aggregated_features = aggregated_features.drop_duplicates()
aggregated_features.to_csv(OUPUT_PREFIX + FILE_NAME, index=False)
