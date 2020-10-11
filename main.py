#Create a crawler which recursively scrapes all the links

from cfg import variables
from pymongo import MongoClient
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import requests
import time
import datetime
from datetime import timedelta

def is_valid(link):
    parsed = urlparse(link)
    ret = (bool(parsed.netloc) and bool(parsed.scheme))
    return ret

def get_random_file_name(content_type, j):
    random_name = "random_file_"
    extension = ''
    content_type = content_type.split(';')[0]
    if content_type == "application/octet-stream":
        extension = ".bin"
    elif content_type == "text/css":
        extension = ".css"
    elif content_type == "text/csv":
        extension = ".csv"
    elif content_type == "application/msword":
        extension = ".doc"
    elif content_type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
        extension = ".docx"
    elif content_type == "image/gif":
        extension = ".gif"
    elif content_type == "image/vnd.microsoft.icon":
        extension = ".ico"
    elif content_type == "image/jpeg":
        extension = ".jpeg"    
    elif content_type == "text/javascript" or "javascript":
        extension = ".js"
    elif content_type == "application/json":
        extension = ".json"
    elif content_type == "audio/mpeg":
        extension = ".mp3"
    elif content_type == "video/mpeg":
        extension = ".mpeg"
    elif content_type == "font/otf":
        extension = ".otf"
    elif content_type == "image/png":
        extension = ".png"
    elif content_type == "application/pdf":
        extension = ".pdf"
    elif content_type == "application/x-httpd-php":
        extension = ".php"
    elif content_type == "application/vnd.rar":
        extension = ".rar"
    elif content_type == "image/svg+xml":
        extension = ".svg"
    elif content_type == "application/x-shockwave-flash":
        extension = ".swf"
    elif content_type == "text/plain":
        extension = ".txt"
    elif content_type == "video/webm":
        extension = ".webm"
    elif content_type == "application/vnd.ms-excel":
        extension = ".xls"
    elif content_type == "application/xml" or content_type == "text/xml" :
        extension = ".xml"
    elif content_type == "application/zip":
        extension = ".zip"            
    return (random_name + str(j) + extension)                     
#In an infinite loop
while True:
    client = MongoClient('localhost', 27017)
    db = client['crawler_db']
    db_list = db['links']
    i=1
    j=1

    
    #Get all links from databaseS
    links_list=[]
    for x in db_list.find():
        links_list.append(x)


    #If number of links in database is >=5000
    if len(links_list) >= 3000:
        #Print the relevant message
        print("Maximum limit reached")
        #Ignore everything and continue
        break

    #If number of links in database is less than 5000
    else:
        #For each valid link
        for link_dict in links_list:
            if is_valid(link_dict['link']):
                #If link is not crawled before in the last 24 hrs or not crawled at all:
                if (link_dict['isCrawled']== False):
                    link_1 = link_dict['link']
                    #for a particular exception
                    if (link_dict['link'][0]==link_dict['link'][1]):
                        link_1 = link_dict['link'][1:]
                    #Do web request
                    r = requests.get(link_1)
                    #Check status code
                    #If status code is not 200
                    if r.status_code != 200:
                        #mark link as isCrawled=True and continue
                        my_query = { "_id":link_dict['_id'] }
                        updated_query = { "$set":{"isCrawled" : True, "responseStatus" : r.status_code, 'lastCrawlDt' : datetime.datetime.now()}}
                        db_list.update_one(my_query, updated_query)

                        continue
                    #If status code=200
                    elif r.status_code == 200:
                        #if content is HTML
                        k = r.headers['content-type'].split(';')[0]
                        if  k == 'text/html':
                            #crawl all the <a href="">links and save to database
                            #first use beautifulsoup to parse and then collect the links and store them

                            soup = BeautifulSoup(r._content, 'html.parser')

                            for a_tag in soup.findAll("a"):
                                href = a_tag.attrs.get("href")
                                if not( href == "" or (href == None) or href == '#' ) and href[0]=="/":
                                    link_add = {"link" : link_dict['link']+str(href),"sourceLink": link_1, "isCrawled" : False,  'lastCrawlDt' : None,  'responseStatus' : 0, "contentType" : '', 'contentLength' : 0, 'filePath' : '', 'createdAt' : datetime.datetime.now()}        
                                    db_list.insert_one(link_add)
                                elif not( href == "" or (href == None) or href == '#' ):        
                                    link_add = {"link" : str(href),"sourceLink" : link_1, "isCrawled" : False, 'lastCrawlDt' : None,  'responseStatus' : 0, "contentType" : '', 'contentLength' : 0, 'filePath' : '', 'createdAt' : datetime.datetime.now()}        
                                    db_list.insert_one(link_add)    

                            #save HTML file to disk
                            y = open("html_file_"+ str(i)+ ".html", "w",encoding='utf-8').write(r.text)
                            i+=1
                            #mark the link as isCrawled=True and continue
                            my_query = {"_id":link_dict['_id']}
                            updated_query = {"$set":{"isCrawled" : True, "responseStatus" : 200, "contentType" : k, 'contentLength' : len(r.content),'lastCrawlDt' : datetime.datetime.now(), 'filePath' : 'D:\\Crawler' }}
                            db_list.update_one(my_query, updated_query)
                        
                        #If content type is not html
                        else:    
                            #save file as its respective extension
                            p = get_random_file_name(r.headers['content-type'], j)
                            print(p)
                            z = open(p, "wb")
                            z = z.write(r.content)
                            j+=1
                            #mark the link as isCrawled=True and continue
                            my_query = { "_id":link_dict['_id'] }
                            updated_query = { "$set":{"isCrawled" : True, "responseStatus" : 200 , "contentType" : 'text/html', 'contentLength' : len(r.content),'lastCrawlDt' : datetime.datetime.now()}}
                            db_list.update_one(my_query, updated_query)
                elif link_dict['isCrawled']== True:
                    date_time_obj = datetime.datetime.strptime(str(link_dict['lastCrawlDt']), '%Y-%m-%d %H:%M:%S.%f')
                    if (date_time_obj- datetime.datetime.now() != datetime.timedelta(days=0)):
                        link_1 = link_dict['link']
                        #for a particular exception
                        if (link_dict['link'][0]==link_dict['link'][1]):
                            link_1 = link_dict['link'][1:]
                        #Do web request
                        r = requests.get(link_1)
                        #Check status code
                        #If status code is not 200
                        if r.status_code != 200:
                            #mark link as isCrawled=True and continue
                            my_query = { "_id":link_dict['_id'] }
                            updated_query = { "$set":{"isCrawled" : True, "responseStatus" : r.status_code, 'lastCrawlDt' : datetime.datetime.now()}}
                            db_list.update_one(my_query, updated_query)

                            continue
                        #If status code=200
                        elif r.status_code == 200:
                            #if content is HTML
                            k = r.headers['content-type'].split(';')[0]
                            if  k == 'text/html':
                                #crawl all the <a href="">links and save to database
                                #first use beautifulsoup to parse and then collect the links and store them

                                soup = BeautifulSoup(r._content, 'html.parser')

                                for a_tag in soup.findAll("a"):
                                    href = a_tag.attrs.get("href")
                                    if not( href == "" or (href == None) or href == '#' ) and href[0]=="/":
                                        link_add = {"link" : link_1 +str(href),"sourceLink" : link_1, "isCrawled" : False,  'lastCrawlDt' : None,  'responseStatus' : 0, "contentType" : '', 'contentLength' : 0, 'filePath' : '', 'createdAt' : datetime.datetime.now()}        
                                        db_list.insert_one(link_add)
                                    elif not( href == "" or (href == None) or href == '#' ):        
                                        link_add = {"link" : str(href),"sourceLink": link_1, "isCrawled" : False, 'lastCrawlDt' : None,  'responseStatus' : 0, "contentType" : '', 'contentLength' : 0, 'filePath' : '', 'createdAt' : datetime.datetime.now()}        
                                        db_list.insert_one(link_add)    

                                #save HTML file to disk
                                y = open("html_file_"+ str(i)+ ".html", "w",encoding='utf-8').write(r.text)
                                i+=1
                                #mark the link as isCrawled=True and continue
                                my_query = {"_id":link_dict['_id']}
                                updated_query = {"$set":{"isCrawled" : True, "responseStatus" : 200, "contentType" : k, 'contentLength' : len(r.content),'lastCrawlDt' : datetime.datetime.now(), 'filePath' : 'D:\\Crawler' }}
                                db_list.update_one(my_query, updated_query)
                            
                            #If content type is not html
                            else:    
                                #save file as its respective extension
                                p = get_random_file_name(r.headers['content-type'], j)
                                print(p)
                                z = open(p, "wb")
                                z = z.write(r.content)
                                j+=1
                                #mark the link as isCrawled=True and continue
                                my_query = { "_id":link_dict['_id'] }
                                updated_query = { "$set":{"isCrawled" : True, "responseStatus" : 200 , "contentType" : 'text/html', 'contentLength' : len(r.content),'lastCrawlDt' : datetime.date.today()}}
                                db_list.update_one(my_query, updated_query)
                        
                    


                    

                #If link is crawled before in 24hrs
                else:
                    #Ignore link and continue
                    continue
    #Sleep for 5sec  
    #time.sleep(sleep_time)

