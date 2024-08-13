import json
import os
import time
import pandas as pd
from OTXv2 import OTXv2
from OTXv2 import IndicatorTypes

INPUT_CSV = "<your input csv file path>"
OUPUT_CSV = "<your output csv file path>"

def save_batch(batch, file_path):
    df = pd.DataFrame(batch)
    with open(file_path, "a") as file:
        header = file.tell() == 0
        df.to_csv(file, index=False, header=header, mode="a")

def dump_granular_malicious_websites(subscriptions, file_path):
    BATCH_SIZE=100000

    indicators = []
    otx = OTXv2(os.getenv("OTX_API_KEY2"))

    for subscription in subscriptions:
        indicator_generator = otx.get_all_indicators(author_name=subscription, limit=BATCH_SIZE, indicator_types=[IndicatorTypes.URL, IndicatorTypes.HOSTNAME, IndicatorTypes.DOMAIN])
        while True:
            try:
                indicator = next(indicator_generator)
                if not indicator['role']:
                    continue
                indicators.append(indicator)
                if len(indicators) >= BATCH_SIZE:
                    save_batch(indicators, file_path)
                    indicators = []
                time.sleep(2)
            except StopIteration:
                break
            except Exception as e:
                print(e)
                return
            finally:
                if len(indicators) > 0:
                    save_batch(indicators, file_path)
                    indicators = []

with open(INPUT_CSV, "r") as file:
    subscriptions = json.load(file)

dump_granular_malicious_websites(subscriptions, OUPUT_CSV)
print("Job Completed")
