import requests
import pandas as pd
from tqdm import tqdm
from datetime import datetime
import os
from send_email import send_email

data = pd.read_csv("pop_minimal.csv")


def make_url(title, id):
    return f"https://www.olx.com.pk/items/{'-'.join(title.lower().split())}-iid-{id}"


def parse_response(x):
    resp = {
        'title': x['title'],
        'url': make_url(x['title'], x['id']),
        'price': None if x['price'] is None else x['price']['value']['raw'],
        'desc': x['description'],
        'featured': x['monetizationInfo'] is not None or 'package' in x,
        'date': x['display_date']
    }

    for i in x['parameters']:
        resp[i['key']] = i['value_name']
    return resp

#https://www.olx.com.pk/api/relevance/search?facet_limit=100&location=4060673&location_facet_limit=20&page=1&query=honda%20city&spellcheck=true&user=175924f4bafx4f0f3e14
def search(q, page):
    page_param = '' if page == 0 else f'&page={page}'
    url = f'https://www.olx.com.pk/api/relevance/search?{page_param}'
    params = {
        'category': 84,
        'query': q,
        'spellcheck': True,
        'facet_limit': 100,
        'location': 1000001,
        'location_facet_limit': 20
    }
    print(q)

    r = requests.get(url, params)
    res = r.json()
    total_ads = res['metadata']['total_ads']
    return (list(map(parse_response, res['data'])), total_ads)


all_results = []
counter = []
results = None
count = None
for row in tqdm(data.itertuples()):
    query = f'{row.Make} {row.Model}'
    for page in range(1):
        try:
            results, count = search(query, page)
            for r in results:
                r['query']=query
            all_results += results
        except:
            print("error while fetching", query, page)
    counter.append({'query': query, 'count': count})


# getting current date
now = datetime.now()
currentDate = now.strftime("%d-%m-%Y")

# getting current time
now = datetime.now()
current_time = now.strftime("%H-%M-%S")

counterFile = 'counter' + '_' + currentDate + '_' + current_time + '.csv'
resultsFile = 'results' + '_' + currentDate + '_' + current_time + '.csv'

pd.DataFrame(all_results).to_csv(resultsFile, index=False)
pd.DataFrame(counter).to_csv(counterFile, index=False)

send_email('m_haroon96@hotmail.com', f'OLX Crawl ({currentDate})', '', [counterFile, resultsFile])
