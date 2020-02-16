import requests
import bs4
import pandas as pd
import os
# import futures

class HouseMetaData():

    url = \
    """
    https://www.olx.pl/nieruchomosci/mieszkania/krakow/?search%5Bfilter_float_price%3Afrom%5D=1500
    &search%5Bfilter_float_price%3Ato%5D=2500&search%5Bfilter_float_m%3Afrom%5D=25&
    search%5Bfilter_float_m%3Ato%5D=50&search%5Bprivate_business%5D=private
    """

    def __init__(self, url=None):
        self.page = None
        self.max_pages = 10
        if url is None:
            self.url = HouseMetaData.url
        
        self.offers_df = pd.DataFrame(
            {'title': [], 'price': [], 'link': [], 'contains_april': [], 'page': []}
            ) 

    def next_page(self):
        print(self.page)
        if self.page == None:
            self.page = 0
        elif isinstance(self.page, int) and self.page < self.max_pages:
            self.page += 1
            self.url = self.url + f'&page={self.page+1}'
    
    def source_page(self):
        page = requests.get(self.url)
        soup = bs4.BeautifulSoup(page.content, 'html.parser')
        res = soup.find(id='offers_table')
        self.offers = res.find_all('div', class_= 'offer-wrapper')
            
    def extract_all_offer_data(self):
        print(f'Extracting homes from page {self.page}')

        for i in self.offers:
            link = i.find('a', "marginright5 link linkWithHash detailsLinkPromoted", href=True)
            if link is None:
                link = i.find('a', "marginright5 link linkWithHash detailsLink", href=True)
            price = i.find('p', class_="price")
            record = pd.DataFrame(
                {
                    'title': [link.text.strip()],
                    'price': [price.text.strip()], 
                    'link': [link['href'].strip()], 
                    'contains_april': ['-'], 
                    'page': [self.page]
                }
            )
            self.offers_df = pd.concat([self.offers_df, record])
        self.offers_df.reset_index(inplace=True, drop=True)
    
    def inspect_offer(self, idx):
        page = requests.get(str(self.offers_df.loc[idx, 'link']))
        soup = bs4.BeautifulSoup(page.content, 'html.parser')
        details = soup.find('table', class_='details fixed marginbott20 margintop5 full')
        details = [d.text for d in soup.find_all('th')]
        det_values = [d.text.strip() for d in soup.find_all('td', class_='value')]
        dets = zip(details, det_values)
        text_content = soup.find('div', class_="clr lheight20 large", id="textContent")
        
        if text_content is None:
            text_content = ''
        else:
            text_content = text_content.text

        for det, value in dets:
            self.offers_df.loc[idx, det] = value
        # input(self.offers_df.iloc[idx])
        # input(text_content)
        # self.offers_df['text'] = ''
        # input(self.offers_df.columns)
        self.offers_df.at[idx, 'text'] = text_content
    
    def search_for_april(self, idx):
        phrases = ['kwiet', '01.04', '01/04', '1/04', '01/4', '01-04', '01-4', '1-04', '1-4']
        match = False

        if any(phrase in self.offers_df.at[idx, 'text'] for phrase in phrases):
            match = True
        if any(phrase in self.offers_df.at[idx, 'title'] for phrase in phrases):
            match = True

        self.offers_df.at[idx, 'contains_april'] = match
    
    def analyze(self):
        for idx, _ in self.offers_df.iterrows():
            print(f'Analyzing {idx}')
            self.inspect_offer(idx)
            self.search_for_april(idx)



h = HouseMetaData()
# h.max_pages = 1
for i in range(h.max_pages):
    h.next_page()
    h.source_page()
    h.extract_all_offer_data()
print('analyzing')
h.analyze()
h.offers_df.drop('text', 1, inplace=True)
print('DONE')
# print(h.offers_df.columns)
# print(h.offers_df[['title', 'price', 'contains_april', 'text']])

h.offers_df.to_csv(os.path.join(os.getcwd(), 'homes.csv'))

