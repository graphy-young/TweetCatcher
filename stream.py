import tweepy
import pymysql
import keys # contains sensitive values
import json
from emojiRemover import deEmoji # removes all emoji inside twitter contents
import time
import datetime
from dateutil.parser import parse

# Setup tweepy API using keys & tokens from Twitter Developer
consumer_key = keys.consumer_key
consumer_secret = keys.consumer_secret

access_token = keys.access_token
access_token_secret = keys.access_token_secret

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

# Connect to MySQL Database
mysql = pymysql.connect(
    host = keys.mysql_host, 
    port = keys.mysql_port, 
    user = keys.mysql_user, 
    password = keys.mysql_password, 
    database = keys.mysql_database
    )
cursor = mysql.cursor()

### The default access level allows up to 400 track keywords, 5,000 follow userids and 25 0.1-360 degree location boxes.
# https://developer.twitter.com/en/docs/tweets/filter-realtime/api-reference/post-statuses-filter.html

keywords_list = input('검색할 키워드를 입력하세요. 최대 400개 키워드까지 입력 가능합니다. 다중 검색은 쉼표를 통해 구분합니다.\n: ').split(',')
for keyword in keywords_list : keywords_list[keywords_list.index(keyword)] = keyword.strip() # remove left and right side space for each keyword

for keyword in keywords_list:
    query = 'CREATE TABLE IF NOT EXISTS `%s`.' % keys.mysql_database + '''`keyword_%s` (
                username VARCHAR(15) NOT NULL,
                uploaded_time DATETIME NOT NULL,
                content TEXT NOT NULL,
                place VARCHAR(50) DEFAULT NULL,
                latitude DECIMAL(10,7) DEFAULT NULL,
                longtitude DECIMAL(10,7) DEFAULT NULL
            );''' % keyword
    cursor.execute(query)

# 키워드 예외를 적용할 예외 테이블 생성
query = 'CREATE TABLE IF NOT EXISTS `%s`.' % keys.mysql_database + '''`exception` (
            username VARCHAR(15) NOT NULL,
            uploaded_time DATETIME NOT NULL,
            content TEXT NOT NULL,
            place VARCHAR(50) DEFAULT NULL,
            latitude DECIMAL(10,7) DEFAULT NULL,
            longtitude DECIMAL(10,7) DEFAULT NULL
        );'''
cursor.execute(query)

# 로그 테이블 생성
query = 'CREATE TABLE IF NOT EXISTS `%s`.' % keys.mysql_database + '''`log` (
            time DATETIME NOT NULL,
            error TEXT NOT NULL,
            etc TEXT DEFAULT NULL
        );'''
cursor.execute(query)


class TwitterStreamListener(tweepy.StreamListener):

    counter = 0

    def on_status(self, status):
        print('Twitter Stream connected successfully.')
        print(status.text)

    def on_data(self, data):
        tweet = json.loads(data)
        
        ### Tweet data
        username = tweet['user']['screen_name'] # @'username'
        #timestamp = tweet['created_at'] # 'Wed Sep 11 16:14:04 +0000 2019', this is original timestamp based on UTC+0
        uploaded_time = (parse(tweet['created_at']) + datetime.timedelta(hours=9)).strftime('%Y-%m-%d %H:%M:%S')
        content = deEmoji(tweet['text']) # Tweet's content (not include media)
        if tweet['geo'] is not None: # When the tweet doesn't include user's location value
            latitude = tweet['geo']['coordinates'][0] # latitude (위도)
            longtitude = tweet['geo']['coordinates'][1] # longtitude (경도)
        else: latitude = longtitude = None
        if tweet['place'] is not None: # If user doen't tag any location in tweet
            place = tweet['place']['full_name'] # name of tagged places
        else: place = None
        if len(keywords_list) == 1: contain_keywords = keywords_list
        else:
            keyword = ''
            contain_keywords = []
            for keyword_candidate in keywords_list:
                if keyword_candidate.lower() in str(tweet).lower(): contain_keywords.append(keyword_candidate)
            if len(contain_keywords) == 0:
                query = '''INSERT INTO `exception`(username, uploaded_time, content, place, latitude, longtitude)
                    VALUES (%s, %s, %s, %s, %s, %s);'''
                values = (username, uploaded_time, content, place, latitude, longtitude)
                cursor.execute(query, values)
                mysql.commit()
            else: 
                for keyword in contain_keywords:
                    query = '''INSERT INTO `keyword_%s`(username, uploaded_time, content, place, latitude, longtitude) ''' % keyword + '''
                    VALUES (%s, %s, %s, %s, %s, %s);'''
                    values = (username, uploaded_time, content, place, latitude, longtitude)
                    cursor.execute(query, values)
                    mysql.commit()

        print('\nKeyword :', contain_keywords, 
        '\nUsername :', username, 
        '\nTimestamp :', uploaded_time, 
        '\nContent :', content, 
        '\nWhere :', place, 
        '\nGeo :', latitude, longtitude, '\n')

        self.counter += 1
        print(self.counter, 'tweet(s) captured successfully.\n')

    def on_error(self, status_code):
        print(status_code)
        return False

while True:
    try:
        TwitterStreamListener = TwitterStreamListener()
        TweetStream = tweepy.Stream(auth = api.auth, listener=TwitterStreamListener)
        TweetStream.filter(track=keywords_list, is_async=True)
    except Exception as e:
        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print("[WARNING]", current_time, "error occured")
        print(e)
        query = '''INSERT INTO `exception`(time, error, etc) VALUES (%s, %s, %s);'''
        values = (current_time, str(e), '')
        cursor.execute(query, values)
        mysql.commit()
        #time.sleep(15 * 60)
        time.sleep(60)