# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""


# coding: utf-8
import json
import requests
import numpy as np
import pandas as pd
import time

from datetime import datetime
from utils.db_access import get_db_connection
from utils.helpers import parseResponseToJson

from itertools import groupby
from operator import itemgetter

QUERY_GET_SOUNDCLOUD_IDS = """
    SELECT cu.account_id, cac.ss_followers, cac.id
	FROM cm_url cu
	JOIN cm_artist_cache cac ON cac.id::INTEGER = cu.target_id::INTEGER
	WHERE type = '7'
	AND account_id IS NOT NULL
	AND active is TRUE
	AND cac.ss_followers IS NOT NULL
	ORDER BY cac.ss_followers DESC 
"""

QUERY_DOES_USER_EXIST = """
    SELECT account_id
    FROM soundcloud_user
    WHERE account_id=%s
"""

QUERY_DOES_ARTIST_FOLLOWER_RELATIONSHIP_EXIST = '''
    SELECT artist_id
    FROM l_soundcloud_artist_user
    WHERE artist_id = %s 
    AND user_id = %s 
    AND rank = %s
'''

QUERY_ADD_USER = '''
    INSERT INTO soundcloud_user
    (account_id, avatar_url, full_name, followers_count, city, country_code, artist)
    VALUES
    (%s, %s, %s, %s, %s, %s,%s)
'''

QUERY_ADD_USER_ARTIST_RELATIONSHIP = '''
    INSERT INTO l_soundcloud_artist_user
    (artist_id, user_id, rank)
    VALUES
    (%s,%s,%s)
'''

# make constant ALL_CAPS
EXTRA_PARAMS = insert client_id
USER_ID_URL = 'https://api-v2.soundcloud.com/users/%s?'
NEXT_HREF_URL = 'https://api-v2.soundcloud.com/users/%s/followers?offset=0&limit=200'
RANK_MAX = 5000 # returns the top 5000 folllowers for each artist


"""This script takes SoundCloud ID's and returns artist ID's, i.e. those who have uploaded tracks.
It then returns follower imformation for each artist: soundcloud_id, avatar_url, full_name, followers_count,
city, country_code, as well as if that follower is an artist themselves.

It also returns the top 5000 notable followers for each artist.
"""

def insert_data_to_linking_table(con, user_data):
    '''
    @desc   : inserts data for relationship between artist and user 
    @params : con - database connection object
            user_data - dictionary of SoundCloud artist to follower metadata
    '''
    cur = con.cursor()
    artist_id = user_data['artist_id']
    user_id = user_data['user_id']
    rank = user_data['rank']
    cur.execute(QUERY_DOES_ARTIST_FOLLOWER_RELATIONSHIP_EXIST, (artist_id, user_id, rank))
    res = cur.fetchone()

    if not res:
        print("\t Adding artist follower relationship %s to DB" % user_data['artist_id'])
        cur.execute(QUERY_ADD_USER_ARTIST_RELATIONSHIP,(artist_id, user_id, rank))
        con.commit()
    else:
        print("\t Artist follower relationship already added %s to DB" % user_data['artist_id'])


def insert_user_to_db(con, user_data):
    '''
    @desc   : inserts data for a SoundCloud user into the database
    @params : con - database connection object
            user_data - dictionary of SoundCloud user metadata
    '''
    cur = con.cursor()
    account_id = user_data["user_id"]
    avatar_url = user_data["image_url"]
    full_name = user_data["full_name"]
    followers_count = user_data["followers_count"]
    city = user_data["city"]
    country_code = user_data["country_code"]
    artist = user_data["artist"]

    # does this user exist in the database?
    cur.execute(QUERY_DOES_USER_EXIST, (account_id,))
    res = cur.fetchone()

    if not res:
        print("\t\t Adding SoundCloud user ID %s to DB" % user_data['user_id'])
        cur.execute(QUERY_ADD_USER, (account_id, avatar_url, full_name, followers_count, city, country_code, artist))
        con.commit()
    else:
        print("\t\t SoundCloud user %s already in DB" % user_data['user_id'])


def get_soundcloud_ids(con):
    '''
    @desc   : gets soundcloud ids from the cm_url table
    @params : con - database connection object
    @return : list of soundcloud ids if they exist, otherwise None
    '''

    cur = con.cursor()
    cur.execute(QUERY_GET_SOUNDCLOUD_IDS)
    response = cur.fetchall()
    return [x[0] for x in response if x] if response else None


def get_followers_from_artist(artist_id):
    '''
    @desc   : gets the follower information for followers following an artist
    @params : artist id i.e soundcloud id with tracks
    @return : follower_filtered_data: list of dictionaries of follower information
                artist_follower_rank: list of dictionaries that ranks notable followers
    '''
    
    print("\t- Getting followers data for SoundCloud artist: %s" % artist_id)
    followers_raw_data = []
    next_href = (NEXT_HREF_URL % artist_id)
    count = 0
    while (next_href) and count <= 25: # only calling the first 5000 followers info
        count += 1 
        next_href_credentials = next_href + EXTRA_PARAMS # adding credentials to the url for the API call
        try:   
            response = requests.get(next_href_credentials)
        except ValueError as e:
            print('ERROR: Decoding next_href failed -', e) # catching errors
            continue
        except ConnectionError as c:
            print('ERROR: Connection error -', c)
            continue
        json_data = parseResponseToJson(response.text)
        if not json_data:
            continue
        followers_raw_data += json_data['collection']
        next_href = json_data['next_href']

    follower_filtered_data = []
    
    for follower in followers_raw_data:
        follower_filtered_data.append({
            "followers_count": follower['followers_count'],
            "user_id": follower["id"],
            "image_url": follower['avatar_url'],
            "full_name": follower['full_name'],
            "city": follower["city"],
            "country_code": follower["country_code"],
            "artist": 'track_count' in follower and follower["track_count"] >= 1
        })
        
        
    print("\t- Gathering notable followers info for artist: %s" % artist_id)   
    artist_follower_rank = []
    rank = 0
    follower_filtered_data.sort(key = itemgetter('followers_count'), reverse = True) # puts it in decsending order to rank followers for each artist
    for users in follower_filtered_data[:RANK_MAX]:
        rank += 1
        artist_follower_rank.append({
            "artist_id": artist_id,
            "user_id": users['user_id'],
            "rank": rank
        })

    return follower_filtered_data, artist_follower_rank


def process_artists(con, soundcloud_ids):
    ''' 
    @desc   : processes artist and inserts artist and follower information into the DB.
    @params : con - database connection object, soundcloud_ids - soundcloud artist id
    @return : None
    '''
    for i in range(len(soundcloud_ids)):
        soundcloud_id = soundcloud_ids[i]
        print(" > Index %s | Checking if user_id is artist: %s" % (str(i), soundcloud_id))
        url = (USER_ID_URL % soundcloud_id) + EXTRA_PARAMS
        
        try:   
            response = requests.get(url) # request_helpers, returns proxy error
        except ValueError as e:
            print('ERROR: Decoding url failed -', e) # catching errors
            continue
        except ConnectionError as c:
            print('ERROR: Connection error -', c)
            continue
        json_data = parseResponseToJson(response.text)

        if not json_data:
            continue
        if 'track_count' in json_data and json_data["track_count"] >= 1: # If user has tracks then user is artist
            print("\t- matched as artist")
            followers, followers_relationship = get_followers_from_artist(soundcloud_id) 
            for follower in followers:
                insert_user_to_db(con, follower)
            for follower_relationship in followers_relationship:
                insert_data_to_linking_table(con, follower_relationship)
        
            
if __name__ == "__main__":
    start_time = datetime.now()
    with get_db_connection() as con:
        soundcloud_ids = get_soundcloud_ids(con)
        print("Retrieved %s soundcloud ids" % str(len(soundcloud_ids)))
        process_artists(con, soundcloud_ids) 
    print('Done at %s.' % time.strftime('%c'))
    end_time = datetime.now()
    print('Duration:',(end_time - start_time))
