
import subprocess
import sys

import numpy as np
import pandas as pd
import torch
from tqdm import tqdm
tqdm.pandas()

from sklearn.feature_selection import SelectKBest, chi2
from sklearn.preprocessing import MinMaxScaler
from transformers import AutoTokenizer, AutoModel, pipeline
from transformers import RobertaTokenizer, RobertaConfig, RobertaModel
from transformers import LongformerTokenizer, LongformerModel

INPUT_CSV = "<your input csv file path>"
OUPUT_CSV = "<your output csv file path>"

model = AutoModel.from_pretrained('distilroberta-base')
tokenizer = AutoTokenizer.from_pretrained('distilroberta-base', truncation=True, padding='longest')
nlp = pipeline('feature-extraction', model=model, tokenizer=tokenizer)

def sliding_window(tokens, model):
    chunksize = 512

    input_id_chunks = list(tokens['input_ids'][0].split(chunksize - 2))
    mask_chunks = list(tokens['attention_mask'][0].split(chunksize - 2))

    for i in range(len(input_id_chunks)):
        input_id_chunks[i] = torch.cat([
            torch.tensor([101]), input_id_chunks[i], torch.tensor([102])
        ])
        mask_chunks[i] = torch.cat([
            torch.tensor([1]), mask_chunks[i], torch.tensor([1])
        ])
        pad_len = chunksize - input_id_chunks[i].shape[0]
        if pad_len > 0:
            input_id_chunks[i] = torch.cat([
                input_id_chunks[i], torch.Tensor([0] * pad_len)
            ])
            mask_chunks[i] = torch.cat([
                mask_chunks[i], torch.Tensor([0] * pad_len)
            ])

    input_ids = torch.stack(input_id_chunks)
    attention_mask = torch.stack(mask_chunks)

    input_dict = {
        'input_ids': input_ids.long(),
        'attention_mask': attention_mask.int()
    }
    outputs = model(**input_dict)
    return outputs

df = pd.read_csv(INPUT_CSV)

url_feats = np.zeros((len(df), 512))

for i in tqdm(range(len(url_feats))):
    tokens = tokenizer.encode_plus(df['url'].iloc[i], add_special_tokens=False, return_tensors='pt', max_length=512, truncation=True)
    url_feats[i] = sliding_window(tokens, model)[0].mean(dim=0).mean(dim=1).detach().numpy()

pd.DataFrame(url_feats, columns=[f"url_emb_{i}" for i in range(512)]).to_csv(OUPUT_CSV)
