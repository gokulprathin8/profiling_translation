from concurrent.futures import ThreadPoolExecutor
import requests
import pandas as pd
from tqdm import tqdm

df = pd.read_csv('final_data.tsv', delimiter='\t')
languages = ["ar", "az", "ca", "cs", "da", "de", "el", "en", "eo", "es", "fa", "fi", "fr", "ga", "he", "hi", "hu", "id",
             "it", "ja", "ko", "nl", "pl", "pt", "ru", "sk", "sv", "th", "tr", "uk", "zh"]

translated_list = []


def translate_row(row, pbar):
    local_list = []
    for target_language in languages:
        try:
            body = {
                'q': row['text'],
                'source': 'auto',
                'target': target_language,
                'format': 'text',
                'api_key': ''
            }
            headers = {'Content-Type': 'application/json'}
            r = requests.post("http://0.0.0.0:5000/translate", json=body)
            if r.status_code == 200:
                translated_text = r.json()['translatedText']
                local_list.append([translated_text, target_language, row['category'], row['annotator'], row['source']])
                pbar.update(1)
            else:
                print(f"Failed to translate text: {r.content}")
        except Exception as e:
            print(f"An exception occurred: {e}")
    translated_list.extend(local_list)


# Initialize tqdm progress bar
total_rows = len(df) * len(languages)

with tqdm(total=total_rows) as pbar:
    with ThreadPoolExecutor(max_workers=10) as executor:
        for idx, row in df.iterrows():
            executor.submit(translate_row, row, pbar)

# Convert the list to a DataFrame and save it
translated_df = pd.DataFrame(translated_list, columns=['text', 'language', 'category', 'annotator', 'source'])
translated_df.to_csv('translated_data1.csv', index=False)
print("Translation and DataFrame creation completed.")
