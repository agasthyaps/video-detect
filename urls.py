import pandas as pd
import numpy as np
import re
import itertools
from bs4 import BeautifulSoup
import time

data = pd.read_csv('Documents/VideoDetect/fac')

entities = data.person_id.unique()
print("OG num people:",len(entities))
def get_embedded(snippet):
    snippet = snippet.replace('data-src','src')
    if snippet.find('>,') > -1:
        snippet = snippet.split('>,')
        snippet = [BeautifulSoup(i,'lxml').find('iframe')['src'] for i in snippet]
    else:
        snippet = [BeautifulSoup(snippet,'lxml').find('iframe')['src']]
    
    return snippet

def get_hyper(snippet):
    if snippet.find('>,') > -1:
        snippet = snippet.split('>,')
        snippet = [BeautifulSoup(i,'lxml').find('a')['href'] if BeautifulSoup(i,'lxml').find('a') else "" for i in snippet]
    else:
        snippet = [BeautifulSoup(snippet,'lxml').find('a')['href']]
    return snippet

print("poop")
urls_dict = {}
print("wuddup")
for name in entities:
    if name != 'C. Roland Christensen Center for Teaching & Learning':
        print(name)
        df = data[data.person_id==name]
        df.dropna(subset = ["num_embedded","num_hyperlinked"], inplace=True)
        urls = []

        for i in range(df.shape[0]):
    #         print("working on", df.iloc[i].url)
            if df.iloc[i].num_embedded > 0:
                urls.append(get_embedded(df.embedded_html.iloc[i]))

            if df.iloc[i].num_hyperlinked > 0:
                urls.append(get_hyper(df.hyperlinked_html.iloc[i]))
        del df

        # flatten the nested list
        urls = list(itertools.chain.from_iterable(urls))


        urls_dict[name] = pd.DataFrame({"person_id":name,"url":urls})
    else:
        print("skipped ya dummy")

video_urls = pd.concat(urls_dict)
print(len(video_urls.person_id.unique()))
print(video_urls.shape)

video_urls = video_urls.reset_index().drop(["level_0","level_1"],axis=1).drop_duplicates(subset="url")

print("new shape", video_urls.shape)
print("new num people", len(video_urls.person_id.unique()))
video_urls.to_csv("fac_video_urls{}.csv".format(time.time()))