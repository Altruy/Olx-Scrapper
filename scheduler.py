import requests
import pandas as pd
from tqdm import tqdm
from datetime import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.base import MIMEBase
from email import encoders
import os


# function for sending scrapped data to provided email address
def sendMail(toSend, toSend2, rec):
    filenames = [toSend, toSend2]
    gmail_user = "datascrapper0@gmail.com"
    gmail_pwd = "datascrapper2021"
    msg = MIMEMultipart()
    msg['From'] = gmail_user
    msg['To'] = rec
    msg['Subject'] = "Olx Scrapped Data"
    msg.attach(MIMEText("Today's scrapped data from OLX"))
    for file in filenames:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(open(file, 'rb').read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"' % file)
        msg.attach(part)
    mailServer = smtplib.SMTP("smtp.gmail.com", 587)
    mailServer.ehlo()
    mailServer.starttls()
    mailServer.ehlo()
    mailServer.login(gmail_user, gmail_pwd)
    mailServer.sendmail(gmail_user, rec, msg.as_string())
    mailServer.close()


data = pd.read_csv("pop.csv")


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
for row in tqdm(data.itertuples()):
    query = f'{row.Make} {row.Model}'
    for page in range(20):
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
currentDate = now.strftime("%d_%m_%Y")

pd.DataFrame(all_results).to_csv('results' + currentDate + '.csv')
pd.DataFrame(counter).to_csv('counter' + currentDate + '.csv')

# sending as email
sendMail('results' + currentDate + '.csv', 'counter' + currentDate + '.csv', 'ayeshahanifrao@gmail.com')
sendMail('results' + currentDate + '.csv', 'counter' + currentDate + '.csv', 'snnakamura@ucdavis.edu')

os.remove('results' + currentDate + '.csv')
os.remove('counter' + currentDate + '.csv')
