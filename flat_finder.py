import os
import re

import bs4
import pandas as pd
import requests
from tqdm import tqdm

CLASSES = (
    ('title', 'h1', 'css-bg3pmc er34gjf0'),
    ('price', 'h2', 'css-5ufiv7 er34gjf0'),
    ('description', 'div', 'css-bgzo2k er34gjf0'),
)


def phrases_in_str(*phrases, target):
    for p in phrases:
        if re.search(p, target):
            return True
    return False



class HouseMetaData:
    base_url = "https://www.olx.pl/"
    url = base_url + '/nieruchomosci/mieszkania/wynajem/krakow/'


    def __init__(self, url=None):
        self.page = None
        self.max_pages = 10
        if url is None:
            self.url = HouseMetaData.url
        
        self.offers_hrefs = []
        self.offers_df = pd.DataFrame(
            {'title': [], 'price': [], 'description': [], 'contains-sept': [], 'contains-oct': []}
        ) 

    def source_href(self, page=0):
        page = requests.get(self.url + f'?page={page}')
        soup = bs4.BeautifulSoup(page.content, 'html.parser')
        res = soup.find(**{'class': 'css-oukcj3'})
        refs = res.find_all('a', class_= 'css-rc5s2u')
        for item in refs:
            self.offers_hrefs.append(item.attrs.get('href'))
            
            
    def extract_all_offer_data(self):
        print(f'Extracting flats from page {self.page}')

        for link in tqdm(self.offers_hrefs):
            page = requests.get(self.base_url + link)
            soup = bs4.BeautifulSoup(page.content, 'html.parser')
            
            record = dict(link=(self.base_url + link))
            broken = False
            for thing, element, class_ in CLASSES:
                content = soup.find(element, class_=class_)
                if not content:
                    broken = True
                    break
                record[thing] = [content.string]
            
            if broken:
                continue
            
            for place in ['title', 'description']:
                record['contains-sept'] = [phrases_in_str('wrzes', 'wrześ', target=str(record[place]))]
                record['contains-oct'] = [phrases_in_str('pazd', 'paźd', target=str(record[place]))]
                
                # tu możesz dodawać różne filtry
                
            record = pd.DataFrame(record)
            self.offers_df = pd.concat([self.offers_df, record])
            
        self.offers_df.reset_index(inplace=True, drop=True)


h = HouseMetaData()
h.max_pages = 2

for i in range(h.max_pages):
    print(f'Analyzing page {i}')
    h.source_href(i)
    h.extract_all_offer_data()

print('DONE')

h.offers_df[['price', 'contains-sept', 'contains-oct', 'title', 'description', 'link']].to_csv(os.path.join(os.getcwd(), 'homes.csv'))

