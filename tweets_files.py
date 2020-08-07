import os
import zipfile
from datetime import datetime, timedelta

import pandas as pd
import wget

import utilities


class TweetsFiles:

    def __init__(self, downloaded_tweets_files_dir, extracted_tweets_files_dir,
                 filtered_tweets_files_dir, hydrated_tweets_files_dir,
                 merged_tweets_files_dir,
                 tweets_collection_start_date, tweets_collection_end_date):

        self.downloaded_tweets_files_dir = downloaded_tweets_files_dir
        self.extracted_tweets_files_dir = extracted_tweets_files_dir
        self.filtered_tweets_files_dir = filtered_tweets_files_dir
        self.hydrated_tweets_files_dir = hydrated_tweets_files_dir
        self.merged_tweets_files_dir = merged_tweets_files_dir

        self.tweets_collection_start_date = tweets_collection_start_date
        self.tweets_collection_end_date = tweets_collection_end_date

        self.tweets_file_base_url = 'https://crisisnlp.qcri.org/covid_data/en_geo_files'
        self.date_format = '%Y-%m-%d'
        self.tweet_file_name_template = 'en_geo_{}.{}'

        self.downloaded_tweets_file_type = 'zip'
        self.extracted_tweets_file_type = 'json'
        self.filtered_tweets_file_type = 'txt'
        self.merged_tweets_file_type = 'csv'

        utilities.create_dir(self.downloaded_tweets_files_dir)
        utilities.create_dir(self.extracted_tweets_files_dir)
        utilities.create_dir(self.filtered_tweets_files_dir)
        utilities.create_dir(self.hydrated_tweets_files_dir)
        utilities.create_dir(self.merged_tweets_files_dir)

    def create_file_name(self, file_name_suffix, file_type):
        return self.tweet_file_name_template.format(file_name_suffix, file_type)

    @staticmethod
    def read_json_file_to_dataframe(file_path):
        if os.path.exists(file_path):
            print('File available. Reading...')
            with open(file_path) as f:
                df = pd.read_json(f, lines=True)
        else:
            print("File doesn't exists")
            df = None

        return df

    @staticmethod
    def populate_custom_fields(tweets_df):
        tweets_df['is_user_india_based'] = tweets_df['user_location_country_code'] \
            .apply(lambda x: 1 if x == 'in' else 0)

        tweets_df['is_tweet_locations_inc_india'] = tweets_df['tweet_locations_country_code'] \
            .apply(lambda x: 1 if 'in' in x else 0)

        return tweets_df

    @staticmethod
    def rename_raw_columns(tweets_df):
        tweets_df.rename(columns={'user_location': 'user_location_raw', 'geo': 'geo_raw',
                                  'place': 'place_raw',
                                  'tweet_locations': 'tweet_locations_raw'}, inplace=True)
        return tweets_df

    @staticmethod
    def create_location_dataframe(tweets_df, column_name):
        location_df = pd.DataFrame()

        location_df[column_name + '_country_code'] = tweets_df[column_name].apply(lambda x: x.get('country_code'))
        location_df[column_name + '_state'] = tweets_df[column_name].apply(lambda x: x.get('state'))
        location_df[column_name + '_county'] = tweets_df[column_name].apply(lambda x: x.get('county'))
        location_df[column_name + '_city'] = tweets_df[column_name].apply(lambda x: x.get('city'))

        return location_df

    def populate_location_columns(self, tweets_df):
        user_location_df = self.create_location_dataframe(tweets_df, 'user_location')
        geo_df = self.create_location_dataframe(tweets_df, 'geo')
        place_df = self.create_location_dataframe(tweets_df, 'place')

        refined_tweets_df = pd.concat([tweets_df, user_location_df, geo_df, place_df], axis=1)

        return refined_tweets_df

    @staticmethod
    def populate_tweet_location_column(tweets_df):
        tweets_df['tweet_locations_country_code'] = tweets_df['tweet_locations'] \
            .apply(lambda x: list(map(lambda y: y.get('country_code'), x)))

        tweets_df['tweet_locations_state'] = tweets_df['tweet_locations'] \
            .apply(lambda x: list(map(lambda y: y.get('state'), x)))

        tweets_df['tweet_locations_county'] = tweets_df['tweet_locations'] \
            .apply(lambda x: list(map(lambda y: y.get('county'), x)))

        tweets_df['tweet_locations_city'] = tweets_df['tweet_locations'] \
            .apply(lambda x: list(map(lambda y: y.get('city'), x)))

        return tweets_df

    @staticmethod
    def filter_india_specific_tweets(tweets_df):
        india_tweets_df = tweets_df.loc[tweets_df['is_tweet_locations_inc_india'] == 1]
        return india_tweets_df

    def process_tweets_files(self):

        start_date = datetime.strptime(self.tweets_collection_start_date, self.date_format)
        end_date = datetime.strptime(self.tweets_collection_end_date, self.date_format)

        while start_date <= end_date:
            self.process_tweet_file(start_date)
            start_date = start_date + timedelta(days=1)

    def process_tweet_file(self, start_date):
        file_name_suffix = datetime.strftime(start_date, self.date_format)

        downloaded_tweet_file_name = self.tweet_file_name_template.format(file_name_suffix,
                                                                          self.downloaded_tweets_file_type)
        download_tweets_file_path = self.tweets_file_base_url + '/' + downloaded_tweet_file_name
        downloaded_tweets_file = os.path.join(self.downloaded_tweets_files_dir, downloaded_tweet_file_name)

        extracted_tweet_file_name = self.tweet_file_name_template.format(file_name_suffix,
                                                                         self.extracted_tweets_file_type)
        extracted_tweets_file = os.path.join(self.extracted_tweets_files_dir, extracted_tweet_file_name)

        print('Download File Url Path : {}'.format(download_tweets_file_path))
        self.download_tweet_file(download_tweets_file_path, downloaded_tweets_file)
        print('Downloaded File Path : {}'.format(downloaded_tweets_file))

        self.extract_tweet_file(downloaded_tweets_file, extracted_tweets_file)
        print('Extracted File Path : {}'.format(extracted_tweets_file))

        self.read_extracted_tweet_file(extracted_tweets_file, file_name_suffix)

    def read_extracted_tweet_file(self, extracted_tweets_file, file_name_suffix):
        filtered_tweet_file_name = self.create_file_name(file_name_suffix,
                                                         self.filtered_tweets_file_type)
        filtered_tweets_file = os.path.join(self.filtered_tweets_files_dir, filtered_tweet_file_name)

        hydrated_tweet_file_name = self.create_file_name(file_name_suffix,
                                                         self.filtered_tweets_file_type)
        hydrated_tweets_file = os.path.join(self.hydrated_tweets_files_dir, hydrated_tweet_file_name)

        merged_tweet_file_name = self.create_file_name(file_name_suffix,
                                                       self.merged_tweets_file_type)
        merged_tweets_file = os.path.join(self.merged_tweets_files_dir, merged_tweet_file_name)

        tweets_df = self.read_json_file_to_dataframe(extracted_tweets_file)

        if tweets_df:
            refined_tweets_df = self.populate_location_columns(tweets_df)
            refined_tweets_df = self.populate_tweet_location_column(refined_tweets_df)
            refined_tweets_df = self.populate_custom_fields(refined_tweets_df)
            refined_tweets_df = self.rename_raw_columns(refined_tweets_df)

            filtered_tweets_df = self.filter_india_specific_tweets(refined_tweets_df)
            self.create_filtered_tweet_ids_file(filtered_tweets_df, filtered_tweets_file)

            self.hydrate_tweets_from_file(filtered_tweets_file, hydrated_tweets_file)

            merged_tweets_df = self.create_hydrated_dataframe(filtered_tweets_df, hydrated_tweets_file)
            self.create_merged_tweet_ids_file(merged_tweets_df, merged_tweets_file)

    def download_tweet_file(self, download_tweets_file_path, downloaded_tweets_file):
        if not os.path.exists(downloaded_tweets_file):
            print('File not available. Downloading...')
            wget.download(download_tweets_file_path, out=self.downloaded_tweets_files_dir)
            print()
        else:
            print('File already exists')

    def extract_tweet_file(self, downloaded_tweets_file, extracted_tweets_file):
        if os.path.exists(downloaded_tweets_file) and not os.path.exists(extracted_tweets_file):
            print('File not available. Extracting...')
            with zipfile.ZipFile(downloaded_tweets_file, 'r') as zip_ref:
                zip_ref.extractall(self.extracted_tweets_files_dir)
            print()
        else:
            print('File already exists')

    def create_hydrated_dataframe(self, filtered_tweets_df, hydrated_tweets_file):
        if filtered_tweets_df and not os.path.exists(hydrated_tweets_file):
            hydrated_tweets_df = self.read_json_file_to_dataframe(hydrated_tweets_file)
            merged_tweets_df = filtered_tweets_df.merge(hydrated_tweets_df.rename(columns={'id': 'tweet_id'}),
                                                        how='left', on='tweet_id')
        else:
            print("Dataframe doesn't exist or File already exists")

        return merged_tweets_df

    @staticmethod
    def create_filtered_tweet_ids_file(filtered_tweets_df, filtered_tweets_file):
        print('Filtered File Path : {}'.format(filtered_tweets_file))
        if filtered_tweets_df and not os.path.exists(filtered_tweets_file):
            filtered_tweets_df.to_csv(filtered_tweets_file,
                                      columns=['tweet_id'], header=False, index=False)
        else:
            print("Dataframe doesn't exist or File already exists")

    @staticmethod
    def create_merged_tweet_ids_file(merged_tweets_df, merged_tweets_file):
        print('Merged File Path : {}'.format(merged_tweets_file))
        if merged_tweets_df and not os.path.exists(merged_tweets_file):
            merged_tweets_df.to_csv(merged_tweets_file, index=False)
        else:
            print("Dataframe doesn't exist or File already exists")

    @staticmethod
    def hydrate_tweets_from_file(filtered_tweets_file, hydrated_tweets_file):
        print('Hydrated File Path : {}'.format(hydrated_tweets_file))
        if not os.path.exists(hydrated_tweets_file):
            print('File not available. Hydrating...')
            os.system('twarc hydrate ' + filtered_tweets_file + '> ' + hydrated_tweets_file)
            print()
        else:
            print('File already exists')
