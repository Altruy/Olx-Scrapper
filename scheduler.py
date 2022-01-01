import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
from send_email import send_email


def func():
    cn = 0
    requests_session = requests.Session()
    # url template for constructing the page url
    URL_TEMPLATE = 'https://www.olx.com.pk/items/q-{}-{}?page={}'
    # read the input csv containing vehicles to search for
    searchCsv = pd.read_csv('pop.csv')
    # create an array for storing the results
    results = []
    counts = []
    failed = []
    # iterate over the input csv row-by-row
    for row in searchCsv.itertuples():
        cn += 1
        # extract make and model from the row
        make = row.Make
        model = row.Model
        # iterate over the result pages
        # change the parameters below to configure how many pages you want
        for pageNum in range(1, 21):
            print("\rCollecting results for {} {} {} (Page {})...t = {:.2f} ".format(cn,
                                                                                     make, model, pageNum, time.time()-start), end='')
            try:
                # construct the page url
                url = URL_TEMPLATE.format(make, model, pageNum)
                # request the page html
                r = requests_session.get(url)
                # soupify the response html
                soup = BeautifulSoup(r.text, 'html.parser')
                # select all listings from the results
                listings = soup.find_all('li', attrs={'aria-label': 'Listing'})
                try:
                    count = soup.find(
                        'div', attrs={'class': '_76047990'}).text.strip()
                    if pageNum == 1:
                        counts.append({'query': make + ' ' + model,
                                       'count': int(count.split()[0].replace(',', ''))})
                except:
                    pass
                # extract required info from each listing
                for rank, listing in enumerate(listings):
                    try:
                        # extract the listing href by looking for the <a> tag that has 'car-name' class
                        # print(rank)
                        href = 'https://www.olx.com.pk' + \
                            listing.find('a')['href']
                        title = listing.find('a')['title']
                        featured = "No"
                        try:
                            check = listing.find(
                                'span', attrs={'class': '_959c44c1 _2e82a662 a695f1e9'}).text.strip()
                            if check == "Featured":
                                featured = "Yes"
                        except:
                            pass
                        price = ''
                        try:
                            price = listing.find(
                                'div', attrs={'aria-label': 'Price'}).find('span').text.strip()
                        except:
                            pass
                        location = listing.find(
                            'div', attrs={'aria-label': 'Location'}).find('span').text.strip()
                        date = listing.find('div', attrs={'class': '_2e28a695'}).find(
                            'span').text.strip()
                        year = ''
                        mileage = ''
                        leased = 'No'
                        try:
                            dets = listing.find(
                                'div', attrs={'aria-label': 'Subtitle'}).text.strip()
                            if "Rs" in dets.split('-')[0]:
                                price = dets.split('-')[0]
                            else:
                                year = dets.split('-')[0]
                            if 'km' in dets.split('-')[1].lower():
                                mileage = dets.split('-')[1]
                            else:
                                leased = dets.split('-')[1]
                        except:
                            pass
                        if price == '':
                            continue

                        result = {
                            'query': make + ' ' + model,
                            'make': make,
                            'model': model,
                            'page num': pageNum,
                            'rank': rank + 1,
                            'id': '',
                            'title': title,
                            'description': '',
                            'last_updated': date,
                            'member name': '',
                            'member since': '',
                            'url': href,
                            'featured': featured,
                            'price': price,
                            'location': location,
                            'year': year,
                            "km's driven": mileage,
                            'registered in': '',
                            'leased': leased,
                            'fuel': '',
                            'condition': '',
                        }
                        r2 = requests_session.get(href)
                        soup2 = BeautifulSoup(r2.text, 'html.parser')
                        try:
                            result['description'] = soup2.find('div', attrs={'class': '_0f86855a'}).find(
                                'span').text.strip()
                        except:
                            pass
                        try:
                            seller = soup2.find(
                                'div', attrs={'aria-label': 'Seller description'})
                            result['member name'] = seller.find(
                                'span', attrs={'class': '_261203a9 _2e82a662'}).text.strip()
                        except:
                            pass
                        try:
                            result['member since'] = seller.find(
                                'span', attrs={'class': '_34a7409b'}).text.strip().replace('Member since ', '')
                        except:
                            pass
                        try:
                            result['id'] = soup2.find(
                                'div', attrs={'class': '_171225da'}).text.strip().split(' ')[-1]
                        except:
                            pass
                        for prt in soup2.find_all('div', attrs={'class': 'b44ca0b3'}):
                            try:
                                x = prt.find_all('span')
                                if x[0].text.strip().lower() in ['make', 'model', 'fuel', 'registered in', 'condition', "km's driven", 'type']:
                                    result[x[0].text.strip().lower()
                                           ] = x[1].text.strip().lower()
                            except:
                                pass
                        if 'type' in result or (result['fuel'] == '' and result['leased'] == 'No' and result['year'] == ''):
                            continue
                        results.append(result)
                    except Exception as e:
                        failed.append({'model': model, 'page': pageNum,
                                       'rank': rank, 'url': href})

            except Exception as e:
                failed.append({'model': model, 'page': pageNum,
                               'rank': rank, 'url': href})
        #     break
        # break

    return results, counts, failed


start = time.time()
date = time.strftime("%d-%m-%Y")
current_time = time.strftime("%H-%M-%S")
results, counts, failed = func()
resultFile = 'results_{}_{}.csv'.format(date, current_time)
countFile = 'counts_{}_{}.csv'.format(date, current_time)
pd.DataFrame(results).to_csv(resultFile, index=False)
pd.DataFrame(counts).to_csv(countFile, index=False)
f = open('log.txt', 'a')
f.write('Date: {} - Time: {:.2f} - Results : {} - Errors: {}\n'.format(date,
        time.time() - start, len(results), len(failed)))
f.close()

if len(failed) > 0:
    failedFile = 'failed_{}_{}.csv'.format(date, current_time)
    pd.DataFrame(failed).to_csv(failedFile, index=False)
    send_email('datascrapper121@gmail.com',
               f'OLX Crawl ({date})', '', [countFile, resultFile, failedFile, 'log.txt'])
else:
    send_email('datascrapper121@gmail.com',
               f'OLX Crawl ({date})', '', [countFile, resultFile, 'log.txt'])
