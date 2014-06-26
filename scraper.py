#this scraper is intended to grab the addresses listed on the stackexchange profile pages,
#and then map them, taken from question http://meta.gis.stackexchange.com/questions/544/map-of-user-locations

import scraperwiki
from bs4 import BeautifulSoup
import re

#looping through the user pages

user_details = True #cheat so that the scraper starts
i = 1

while user_details and i < 2: #want this to time out eventually...
    url = "http://gis.stackexchange.com/users?page=" + str(i)
    webpage = scraperwiki.scrape(url)
    soup = BeautifulSoup(webpage)
    user_details = soup.findAll("div","user-details") #within this set of tags is the info I want

#scraping the user info 
    for user in user_details:
        temp_name = user.find(name='a', attrs={"href" : re.compile("/users/")})
        data = {}
        data['name'] = unicode(temp_name.string)
        temp_location = user.find("span", "user-location")
        data['location'] = unicode(temp_location.string)
        #print data
        scraperwiki.sqlite.save(unique_keys=['name'], data=data)    #saving the data

    i += 1

