import pymongo
import pandas as pd
import numpy as np
from numpy.core.numeric import NaN
import smtplib,ssl
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import os

def function_1(movies):
    cursor=movies.find().limit(10000)
    col=list(cursor[0].keys())
    movies_with_comments=pd.DataFrame(columns=col)
    movies_with_no_comments=pd.DataFrame(columns=col)
    for i in cursor:
        if('num_mflix_comments' in i.keys()):
            movies_with_comments=movies_with_comments.append(i,ignore_index=True)
        else:
            movies_with_no_comments=movies_with_no_comments.append(i,ignore_index=True)
    movies_with_no_comments.to_csv('./movies/movies_with_no_comments.csv')
    movies_with_comments.to_csv('./movies/movies_with_comments.csv')

def function_2():
    movies_with_comments=pd.read_csv("./movies/movies_with_comments.csv")
    movies_with_comments['low_runtime']=np.where(movies_with_comments['runtime']<=50,'Yes','No')
    movies_with_comments['high_runtime']=np.where(movies_with_comments['runtime']>50,'Yes','No')
    movies_with_comments.to_csv('./movies/movies_with_comments.csv')

def function_3(movies):
    cursor=movies.find().limit(10000)
    col=list(cursor[0].keys())
    temp=pd.DataFrame(columns=col)
    movies_rating_8_released_aft_2000=pd.DataFrame(columns=col)

    for j in cursor:
        temp=temp.append(j,ignore_index=True)
    if(len(movies_rating_8_released_aft_2000)):
        movies_rating_8_released_aft_2000.drop(movies_rating_8_released_aft_2000.index, inplace=True)

    for i in range(len(temp)):
        if(temp.loc[i]['imdb']['rating']!='' and float(temp.loc[i]['imdb']['rating'])>8 ):
            if(str(temp.loc[i]['year'])[:4]!=NaN and int(str(temp.loc[i]['year'])[:4])>2000):
                if(temp.loc[i]['awards']['wins']>3):
                    res = dict(zip(list(temp.loc[i].keys()), list(temp.loc[i])))
                    movies_rating_8_released_aft_2000=movies_rating_8_released_aft_2000.append(res,ignore_index=True)
    movies_rating_8_released_aft_2000=movies_rating_8_released_aft_2000.sort_values(['released'], ascending=True)
    movies_rating_8_released_aft_2000.to_csv("./movies/movies_rating_8_released_aft_2000.csv")

def function_4(theaters):
    cursor=theaters.find().limit(10000)
    theatre_simplified=pd.DataFrame(columns=['theaterId' , 'street1' , 'city', 'state', '0' , '1'])

    for i in cursor:
        ele=i
        ele.pop('_id')
        loc=ele.pop('location')
        ele['street1']=loc['address']['street1']
        ele['city']=loc['address']['city']
        ele['state']=loc['address']['state']
        ele['0']=loc['geo']['coordinates'][0]
        ele['1']=loc['geo']['coordinates'][1]
        theatre_simplified=theatre_simplified.append(ele,ignore_index=True)
    
    theatre_simplified.to_csv("./movies/theatre_simplified.csv")

def function_5(movies,EMAIL,PASSWORD,TO_EMAIL):
    cursor=movies.find().limit(10000)
    k=list(cursor[0].keys())
    released_outside_usa=pd.DataFrame(columns=k)
    for i in cursor:
        if("USA" not in i['countries']):
            released_outside_usa=released_outside_usa.append(i,ignore_index=True)

    released_outside_usa.to_csv("./movies/released_outside_usa.csv")

    
    msg = MIMEMultipart()
    msg['Subject'] = 'Movies Released Outside USA'
    msg['From'] = EMAIL 
    msg['To'] = TO_EMAIL


    with open("./movies/released_outside_usa.csv",'rb') as file:
            msg.attach(MIMEApplication(file.read(), Name="released_outside_usa.csv"))

    smtpObj = smtplib.SMTP('smtp.gmail.com', 587)
    smtpObj.ehlo()
    smtpObj.starttls()
    smtpObj.login(EMAIL, PASSWORD)


    text = msg.as_string()
    smtpObj.sendmail(EMAIL, TO_EMAIL , text)
    smtpObj.quit()


#---------------------------------------------------------------
#Main Starts From Here

if not os.path.exists('movies'):
    os.makedirs('movies')

client=pymongo.MongoClient("mongodb+srv://assignment:wlF8axz8N4ZvqH6B@assignment.h251h.mongodb.net/")
db=client.sample_mflix
movies=db.movies
comments=db.comments
theaters=db.theaters

EMAIL="YOUR-EMAIL@GMAIL.COM"
PASSWORD="YOUR-PASSWORD"
TO_EMAIL="RECEIVER-EMAIL"
function_1(movies)
function_2()
function_3(movies)
function_4(theaters)
function_5(movies,EMAIL,PASSWORD,TO_EMAIL)