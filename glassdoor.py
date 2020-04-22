#!/usr/bin/python





import requests 
import re
import time as temps 
import pandas as pd
from pprint import pprint  
from bs4 import BeautifulSoup 
from datetime import datetime, timedelta
from os import path
import urllib3
import numpy as np
import os
import pdb
import sys



def master(key):
    print('starting scraping of glassdoor jobs with the following key : ' + key)
    t0= temps.time()
    df = do_get_job_info(key)
    df = clean_df(df)
    save_csv(df)
    t1= temps.time()
    print('done. time elapsed', round((t1-t0)/60, 3), 'minutes')
    return('done')

def do_get_job_info(key):
    jobContainer = get_jobContainer(key)
    jobInfos = list(map(get_job_info, jobContainer['jobContainer'], np.repeat(jobContainer['session'], len(jobContainer['jobContainer']))))  
    df = pd.DataFrame(jobInfos)
    return(df)

def get_jobContainer(key):
    session = requests.Session()
    session.post(glassdoor()['url'] + "/profile/login_input.htm", data=glassdoor()['credentials'])
    s = session.get(browse(key), headers={'User-Agent':'Mozilla/5.0'})
    s = BeautifulSoup(s.text, 'html.parser') 
    s = s.find(id='JobResults')
    jobContainer = s.find_all(class_='jobContainer')
    return({'jobContainer':jobContainer, 'session':session})

def glassdoor():
    u = 'https://www.glassdoor.com'
    c = {'_username':'[e.tenezakis@gmail.com]', '_password':'[aimilios1908]'}
    return({'url':u,'credentials':c})

def browse(key):
    req  = glassdoor()['url'] + '/Job/jobs.htm?suggestCount=0&suggestChosen=false&clickSource=searchBtn&typedKeyword='+ \
    key + '&sc.keyword='+ key + '&locT=C&locId=3021489&jobType='
    return(req)

def get_job_info(job, session):
    id = job.find('span')['data-job-id']
    days_ago = job.find('div', class_='jobLabels')
    job_url = job.find('a')['href']
    url = glassdoor()['url'] + job_url 
    employer = job.find('div', class_='jobInfoItem jobEmpolyerName') #NOT  a typo
    location1 = 'Seoul'
    location2 = job.find('span', class_='subtle loc')
    jobdesc = 'JobDesc' + id
    detail = session.get(url, headers={'User-Agent':'Mozilla/5.0'})
    detail = BeautifulSoup(detail.text, 'html.parser') 
    title = detail.find('title')
    description = detail.find(id='JobDescriptionContainer')
    if None in (id, url, employer,location1,location2, title,description):
        jobinfo = None
    else:
        jobinfo = {'title':title.text, \
           'id':id,\
           'url':url,\
           'employer':employer.text.lstrip(),\
           'location1':location1,\
           'location2':location2.text,\
           'description':description.text,\
           'days_ago':days_ago.text}
    return(jobinfo)


def clean_df(df):
    df['title'] = df['title'].str.replace('|'.join(['/ ', ' in ','_','\| Glassdoor','Job']),'') 
    df['title'] = df.apply(lambda row: row['title'].replace(row['employer'],''), axis=1)
    df['title'] = df.apply(lambda row: row['title'].replace(row['location1'],''), axis=1)
    df['title'] = df.apply(lambda row: row['title'].replace(row['location2'],''), axis=1)    
    df['title'] = df['title'].str.strip()
    df['description'] = df.apply(lambda row: row['description'][row['description'].rindex('}')+1:], axis=1)
    df['description'] = df['description'].str.strip()
    df['id'] = df['id'].str.strip()
    df['id'] = df['id'].astype(int)
    df['days_ago'] = df.apply(lambda row: int(re.findall(r'\d+', row['days_ago'])[0]), axis=1)
    df['published_on'] = df.apply(lambda row: (datetime.now()-timedelta(row['days_ago'])).strftime("%d/%m/%Y"), axis=1)
    df['scraped_on'] = datetime.now().strftime("%d/%m/%Y")
    del df['days_ago']
    return(df)

def dupl_remover(df):
    df = df.sort_values(['id', 'scraped_on']) 
    unique = df.drop_duplicates(subset ='id', keep = 'first')
    return(unique)

def save_csv(new):
    if os.path.exists(path()):
        old = open_csv()
        new = old.append(new, ignore_index=True)
        new = dupl_remover(new)
    new[list(new)] = new[list(new)].astype(str)
    new.to_csv(path(), index=False)

def path():
    p = '/Users/emile/Documents/web_scraping/glassdoor_jobs.csv'
    return(p)

def open_csv():
    csv = pd.read_csv(path())
    return(csv)

def hp(h):
  print(h.prettify())


master(sys.argv[1])