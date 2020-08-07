import wget
import os
import zipfile
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


class TweetsFiles:

    def __init__(self, downloaded_tweets_files_dir, extracted_tweets_file_dir,
                 tweets_collection_start_date, tweets_collection_end_date):
        self.downloaded_tweets_files_dir = downloaded_tweets_files_dir
        self.extracted_tweets_file_dir = extracted_tweets_file_dir
        self.tweets_collection_start_date = tweets_collection_start_date
        self.tweets_collection_end_date = tweets_collection_end_date

        self.tweets_file_base_url = 'https://crisisnlp.qcri.org/covid_data/en_geo_files'
        self.date_format = '%Y-%m-%d'
        self.raw_tweets_file_type = 'zip'
        self.tweet_file_name_template = 'en_geo_{}.{}'
        self.extracted_tweets_file_type = 'json'

    @staticmethod
    def create_dir(dir_path):
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)

    def get_tweets_files(self):
        self.create_dir(self.downloaded_tweets_files_dir)
        start_date = datetime.strptime(self.tweets_collection_start_date, self.date_format)
        end_date = datetime.strptime(self.tweets_collection_end_date, self.date_format)

        while start_date <= end_date:
            raw_tweet_file_name = self.tweet_file_name_template.format(datetime.strftime(start_date, self.date_format),
                                                                       self.raw_tweets_file_type)
            extracted_tweet_file_name = self.tweet_file_name_template.format(
                datetime.strftime(start_date, self.date_format),
                self.extracted_tweets_file_type)
            self.download_tweet_file(raw_tweet_file_name)
            self.extract_tweet_file(raw_tweet_file_name, extracted_tweet_file_name)
            self.read_tweet_file(extracted_tweet_file_name)
            start_date = start_date + timedelta(days=1)

    def download_tweet_file(self, raw_tweet_file_name):
        tweet_file_path = self.tweets_file_base_url + '/' + raw_tweet_file_name
        print(tweet_file_path)
        if not os.path.exists(os.path.join(self.downloaded_tweets_files_dir, raw_tweet_file_name)):
            print('File not available. Downloading...')
            wget.download(tweet_file_path, out=self.downloaded_tweets_files_dir)
            print()
        else:
            print('File already exists')

    def extract_tweet_file(self, raw_tweet_file_name, extracted_tweet_file_name):
        downloaded_tweets_file = os.path.join(self.downloaded_tweets_files_dir, raw_tweet_file_name)

        if not os.path.exists(os.path.join(self.extracted_tweets_file_dir, extracted_tweet_file_name)):
            print('File not available. Extracting...')
            with zipfile.ZipFile(downloaded_tweets_file, 'r') as zip_ref:
                zip_ref.extractall(self.extracted_tweets_file_dir)
            print()
        else:
            print('File already exists')

    def read_tweet_file(self, extracted_tweet_file_name):
        extracted_tweets_file = os.path.join(self.extracted_tweets_file_dir, extracted_tweet_file_name)
        if os.path.exists(extracted_tweets_file):
            print('File available. Reading...')
            tweets_df = pd.read_json(extracted_tweets_file)
            print(tweets_df.describe())


def get_all_tweets_files():
    downloaded_tweets_files_dir = './raw_tweets_file'
    extracted_tweets_files_dir = './extracted_tweets_file'
    tweets_collection_start_date = '2020-02-01'
    tweets_collection_end_date = '2020-02-01'

    tweet = TweetsFiles(downloaded_tweets_files_dir, extracted_tweets_files_dir,
                        tweets_collection_start_date,
                        tweets_collection_end_date)

    tweet.get_tweets_files()
