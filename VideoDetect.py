#import packages
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup, SoupStrainer
import boto3
import requests
from sqlalchemy import create_engine

# functions
def has_embedded_video(file, method):
    
    video_flags = ['youtube','video','.mp4','player']
    unless = ['soundcloud']
    html_snippet = []
    
    if method == "s3":
	s3 = boto3.resource('s3')
        obj = s3.Object('vpal-link-data','files/'+file)
        soup = BeautifulSoup(obj.get()['Body'].read(),'lxml',parse_only=SoupStrainer("iframe"))
    elif method == "scrape":
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.0; WOW64; rv:24.0) Gecko/20100101 Firefox/24.0' }
        req = requests.get(file, headers = headers)
        page = req.content
        soup = BeautifulSoup(page, 'lxml',parse_only=SoupStrainer("iframe"))
        
    if soup.find("iframe"):
        for frame in soup.find_all("iframe"):
            for key in frame.attrs:
                if any(flag in frame.attrs[key] for flag in video_flags) and any(u not in frame.attrs[key] for u in unless):
                    html_snippet.append(frame)
    
    html_snippet = list(set(html_snippet))               
    hasit = len(html_snippet)
    
    return hasit, html_snippet


def has_hyperlinked_video(file, method):
    
    video_flags = ['youtube','vimeo']
    html_snippet = []
    
    if method == "s3":
	s3 = boto3.resource('s3')
        obj = s3.Object('vpal-link-data','files/'+file)
        soup = BeautifulSoup(obj.get()['Body'].read(),'lxml',parse_only=SoupStrainer("a"))
    elif method == "scrape":
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.0; WOW64; rv:24.0) Gecko/20100101 Firefox/24.0' }
        req = requests.get(file, headers = headers)
        page = req.content
        soup = BeautifulSoup(page, 'lxml',parse_only=SoupStrainer("a"))
        
    if soup.find("a"):
        for a in soup.find_all("a"):
            for key in a.attrs:
                if any(flag in a.attrs[key] for flag in video_flags):
                    html_snippet.append(a)
                    
    html_snippet = list(set(html_snippet))               
    hasit = len(html_snippet)
    
    return hasit, html_snippet


def prune_df(df):
    bad = ['user','login','pin1','calendar',
       '0%2C0%2C0%2C0%2C0%2C1','0%2C0%2C0%2C0%2C0%2C2','tagged',
       'lang=','/?', 'event/',
       '.aif','.mp3','.gif','.pdf','.gz','.wtt','.mswd','.dd','.key','.ppt','.txt',
       '.ppiv','.m','.ps~','.prev','search','ppframe.htm',
      '.fits','.tgz','.dat','.presentation','.pages','bagelsite','AirlineSite']
    
    msk = [False if any(b in i for b in bad) else True for i in df.url]
    pruned_df = df[msk]
    
    inv_dup_msk = pruned_df.duplicated(subset="checksum")
    pruned_df = pruned_df[~inv_dup_msk]
    
    return pruned_df


def find_all_videos(data_df,method,logging=False):

    if method == "s3":
        files_or_urls = data_df.id
    elif method == "scrape":
        files_or_urls = data_df.url
        
    num_embedded_videos = []
    num_hyperlinked_videos = []
    embedded_snippet = []
    hyperlinked_snippet = []
    
    total_urls = data_df.shape[0]
    
    # check for videos
    print("beginning search. ({} urls)".format(total_urls))
    
    for ind, file in enumerate(files_or_urls):
        embedded = has_embedded_video(file, method)
        hyperlinked = has_hyperlinked_video(file, method)
        
        num_embedded_videos.append(embedded[0])
        embedded_snippet.append(embedded[1])
        
        num_hyperlinked_videos.append(hyperlinked[0])
        hyperlinked_snippet.append(hyperlinked[1])
        
        if logging and total_urls >= 100:
            pct_done = (ind/total_urls)*100
            if pct_done % 10 == 0:
                print("Done with {}% of urls.".format(pct_done))
                
    print("finished searching all urls.")
    return num_embedded_videos, embedded_snippet, num_hyperlinked_videos, hyperlinked_snippet

### actually do it

# access database
# s3 = boto3.resource('s3')
# ssm = boto3.client('ssm',region_name='us-east-1')
# db_connection_string = ssm.get_parameter(Name='/link/dev/db/connection_string')['Parameter']['Value']
# engine = create_engine(db_connection_string)
# person_resource = pd.read_sql_table('person_resource',engine)
# resource = pd.read_sql_table('resource',engine)

# person_resource.columns = ['person_id','id']
# data = person_resource.merge(resource)

# if not using s3:
data = pd.read_csv('data/person_resource.csv')

# here's where it happens
faculty_dict = {}
faculty_ids = data.person_id.unique()
file = open("log.txt","w")

for i in faculty_ids:
    
    file.write(str(i))
    temp_df = prune_df(data[data.person_id ==i])
    temp_vids = find_all_videos(temp_df, method = "s3")
    temp_df['num_embedded'] = temp_vids[0]
    temp_df['embedded_html'] = temp_vids[1]
    temp_df['num_hyperlinked'] = temp_vids[2]
    temp_df['hyperlinked_html'] = temp_vids[3]
    temp_df['total_videos'] = temp_df.num_embedded + temp_df.num_hyperlinked
    temp_df = temp_df[['person_id','url','total_videos','num_embedded','embedded_html','num_hyperlinked','hyperlinked_html']]
    
    faculty_dict[i] = temp_df
    del temp_df
    file.write("\n")

file.close()

dfx = pd.concat([df for df in faculty_dict.values()], ignore_index=True)
dfx.to_csv('data/urls_with_videos.csv')

print("job complete!")