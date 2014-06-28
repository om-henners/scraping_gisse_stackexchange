#this scraper is intended to grab the addresses listed on the stackexchange profile pages,
#and then map them, taken from question http://meta.gis.stackexchange.com/questions/544/map-of-user-locations

import re
import os
import requests
import functools
import geopy
import sqlite3
import time

def memoize(obj):
    #From https://wiki.python.org/moin/PythonDecoratorLibrary#Memoize
    cache = obj.cache = {}

    @functools.wraps(obj)
    def memoizer(*args, **kwargs):
        key = str(args) + str(kwargs)
        if key not in cache:
            cache[key] = obj(*args, **kwargs)
        return cache[key]
    return memoizer

class LimitRequestsPerHour(object):
    def __init__(self, max_requests_per_hour):
        self.max_requests = max_requests_per_hour
        self.count = 0
    def __call__(self, obj):
        def wrapee(*args, **kwargs):
            if self.count > self.max_requests:
                print "Sleeping for an hour"
                time.sleep(60*60) #pause for an hour
                self.count = 1
            else:
                self.count += 1
            print "Request {}/{}".format(self.count, self.max_requests)
            return obj(*args, **kwargs)
        return wrapee

@memoize
@LimitRequestsPerHour(1900)
def resolve_location(location_name):
    if location_name in {None, "-", "None"}:
        return None, None #Neither longitude or latitude should be returned
    m = re.match("(-?\d+\.\d+),\s?(-?\d+\.\d+)", location_name)
    if m:
        return float(m.group(2)), float(m.group(1))
    print "Resolving location {}".format(location_name)
    try:
        gn = geopy.geocoders.GeoNames(username=os.environ["MORPH_GEONAMES_USERNAME"])
        resolved_name, (latitude, longitude) = gn.geocode(location_name)
        # print "Resolved as {}".format(resolved_name)
        return longitude, latitude
    except TypeError as e:
        print "EXCEPTION:", e
        return None, None


def get_gis_se_users():
    stack_exchange_users = "http://api.stackexchange.com/2.2/users"
    stack_exchange_params = {
        "order": "desc",
        "sort": "reputation",
        "filter": "default",
        "site": "gis"
    }
    if "MORPH_SE_KEY" in os.environ:
        stack_exchange_params["key"] = os.environ["MORPH_SE_KEY"]

    print "Searching users"

    for page in xrange(1, 1000): #At some point we want to stop
        print "Requesting page {}".format(page)
        stack_exchange_params["page"] = page
        r = requests.get(stack_exchange_users, params=stack_exchange_params)
        users = r.json()
        print "Quota remaining {}".format(users["quota_remaining"])
        for user in users["items"]:
            # print user
            user_id = user["user_id"]
            location_name = user.get("location", None)
            display_name = user["display_name"]

            longitude, latitude = resolve_location(location_name)

            yield user_id, display_name, location_name, longitude, latitude

        if not users["has_more"]:
            break


def scrape_data():
    conn = sqlite3.connect("data.sqlite")
    conn.execute("""drop table if exists data""")
    conn.execute("""
    create table if not exists data (
    id int primary key on conflict replace,
    display_name varchar,
    location varchar,
    longitude float,
    latitude float
    )
    """)
    conn.commit()
    for row in get_gis_se_users():
        conn.execute(
            """insert into data(id, display_name, location, longitude, latitude) values (?, ?, ?, ?, ?)""",
            row
        )
        conn.commit()
    conn.close()


if __name__ == "__main__":
    print "Let's go"
    scrape_data()
