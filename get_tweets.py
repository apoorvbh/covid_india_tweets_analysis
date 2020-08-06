import wget
import os
from datetime import datetime, timedelta


class GetTweet:

    def __init__(self, tweets_dir, tweets_file_base_url,
                 tweets_collection_start_date, tweets_collection_end_date,
                 date_format):
        self.tweets_dir = tweets_dir
        self.tweets_file_base_url = tweets_file_base_url
        self.tweets_collection_start_date = tweets_collection_start_date
        self.tweets_collection_end_date = tweets_collection_end_date
        self.date_format = date_format

    @staticmethod
    def create_dir(dir_path):
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)

    def get_tweets(self):
        start_date = datetime.strptime(self.tweets_collection_start_date, self.date_format)
        end_date = datetime.strptime(self.tweets_collection_end_date, self.date_format)

        while start_date <= end_date:
            tweet_file_name = 'en_geo_{}.zip'.format(datetime.strftime(start_date, self.date_format))
            self.download_file(tweet_file_name)
            start_date = start_date + timedelta(days=1)

    def download_file(self, tweet_file_name):
        tweet_file_path = self.tweets_file_base_url + '/' + tweet_file_name
        print(tweet_file_path)
        if not os.path.exists(os.path.join(self.tweets_dir, tweet_file_name)):
            print('File not available. Downloading...')
            wget.download(tweet_file_path, out=self.tweets_dir)
            print()
        else:
            print('File already exists')


def get_tweet():
    tweets_dir = './tweets'
    tweets_base_url = 'https://crisisnlp.qcri.org/covid_data/en_geo_files'

    tweets_collection_start_date = '2020-02-01'
    tweets_collection_end_date = '2020-02-29'
    date_format = '%Y-%m-%d'

    tweet = GetTweet(tweets_dir, tweets_base_url,
                     tweets_collection_start_date,
                     tweets_collection_end_date,
                     date_format)
    tweet.create_dir(tweets_dir)
    tweet.get_tweets()
