import os
import zipfile
from datetime import datetime, timedelta

import pandas as pd
import wget

import utilities


class TweetsFiles:

    def __init__(self, cleaned_tweets_files_dir):

        self.cleaned_tweets_files_dir = cleaned_tweets_files_dir

        self.tweet_file_name_template = 'en_geo_{}'

        self.extracted_tweets_files_dir = 'extracted_tweets_files'
        self.filtered_tweets_files_dir = 'filtered_tweets_files'
        self.hydrated_tweets_files_dir = 'hydrated_tweets_files'
        self.downloaded_tweets_files_dir = 'downloaded_tweets_files'
        self.file_template = '{}.{}'

        self.tweets_file_base_url = 'https://crisisnlp.qcri.org/covid_data/en_geo_files'

        self.downloaded_tweets_file_type = 'zip'
        self.extracted_tweets_file_type = 'json'
        self.hydrated_tweets_file_type = 'json'
        self.filtered_tweets_file_type = 'txt'
        self.cleaned_tweets_file_type = 'csv'

        utilities.create_dir(self.downloaded_tweets_files_dir)
        utilities.create_dir(self.hydrated_tweets_files_dir)
        utilities.create_dir(self.cleaned_tweets_files_dir)

        utilities.create_dir(self.extracted_tweets_files_dir)
        utilities.create_dir(self.filtered_tweets_files_dir)

    def create_file_name(self, file_name, file_type):
        return self.file_template.format(file_name, file_type)

    @staticmethod
    def read_json_file_to_dataframe(file_path):
        if os.path.exists(file_path):
            print('File available. Reading...')
            with open(file_path) as f:
                df = pd.read_json(f, lines=True)
        else:
            print("File doesn't exists")
            df = pd.DataFrame()

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

    def cleanup(self):
        utilities.remove_dir(self.extracted_tweets_files_dir)
        utilities.remove_dir(self.filtered_tweets_files_dir)

    def process_tweets_date_range_files(self, start_date, end_date, date_format):
        summaries = []

        while start_date <= end_date:
            file_name = self.tweet_file_name_template.format(datetime.strftime(start_date, date_format))
            summaries.append(self.process_tweet_file(file_name))
            start_date = start_date + timedelta(days=1)

        return summaries

    def process_tweet_file(self, file_name):

        downloaded_tweet_file_name = self.create_file_name(file_name, self.downloaded_tweets_file_type)
        download_tweets_file_path = self.tweets_file_base_url + '/' + downloaded_tweet_file_name
        downloaded_tweets_file = os.path.join(self.downloaded_tweets_files_dir, downloaded_tweet_file_name)

        extracted_tweet_file_name = self.create_file_name(file_name, self.extracted_tweets_file_type)
        extracted_tweets_file = os.path.join(self.extracted_tweets_files_dir, extracted_tweet_file_name)

        cleaned_tweet_file_name = self.create_file_name(file_name,
                                                        self.cleaned_tweets_file_type)
        cleaned_tweets_file = os.path.join(self.cleaned_tweets_files_dir, cleaned_tweet_file_name)

        print('Looking for Cleaned File Path : {}'.format(cleaned_tweets_file))
        if os.path.exists(cleaned_tweets_file):
            print('Cleaned file available. Skipping process...')
        else:
            print('Cleaned file not available. Starting process...')

            self.download_tweet_file(download_tweets_file_path, downloaded_tweets_file)

            self.extract_tweet_file(downloaded_tweets_file, extracted_tweets_file)

            summary_row_dict = self.read_extracted_tweet_file(file_name, extracted_tweets_file, cleaned_tweets_file)

            utilities.delete_file(extracted_tweets_file)

            return summary_row_dict

    def read_extracted_tweet_file(self, file_name, extracted_tweets_file, cleaned_tweets_file):
        summary_row_dict = {}

        filtered_tweet_file_name = self.create_file_name(file_name,
                                                         self.filtered_tweets_file_type)
        filtered_tweets_file = os.path.join(self.filtered_tweets_files_dir, filtered_tweet_file_name)

        hydrated_tweet_file_name = self.create_file_name(file_name,
                                                         self.hydrated_tweets_file_type)
        hydrated_tweets_file = os.path.join(self.hydrated_tweets_files_dir, hydrated_tweet_file_name)

        tweets_df = self.read_json_file_to_dataframe(extracted_tweets_file)
        refined_tweets_df = self.populate_location_columns(tweets_df)
        refined_tweets_df = self.populate_tweet_location_column(refined_tweets_df)
        refined_tweets_df = self.populate_custom_fields(refined_tweets_df)
        refined_tweets_df = self.rename_raw_columns(refined_tweets_df)

        filtered_tweets_df = self.filter_india_specific_tweets(refined_tweets_df)
        self.create_filtered_tweet_ids_file(filtered_tweets_df, filtered_tweets_file)

        self.hydrate_tweets_from_file(filtered_tweets_file, hydrated_tweets_file)

        utilities.delete_file(filtered_tweets_file)

        merged_tweets_df = self.create_merged_dataframe(filtered_tweets_df, hydrated_tweets_file)

        summary_row_dict['file_name'] = file_name
        summary_row_dict['total_tweets'] = len(tweets_df.index)
        summary_row_dict['tweets_india'] = refined_tweets_df[
            'is_tweet_locations_inc_india'].value_counts().to_dict().get(1)
        summary_row_dict['tweets_outside_india'] = refined_tweets_df[
            'is_tweet_locations_inc_india'].value_counts().to_dict().get(0)
        summary_row_dict['user_india'] = refined_tweets_df['is_user_india_based'].value_counts().to_dict().get(1)
        summary_row_dict['user_outside_india'] = refined_tweets_df['is_user_india_based'].value_counts().to_dict().get(
            0)
        summary_row_dict['tweet_with_full_text'] = len(
            merged_tweets_df.loc[pd.isnull(merged_tweets_df['full_text']), :].index)
        summary_row_dict['tweet_without_full_text'] = len(
            merged_tweets_df.loc[~pd.isnull(merged_tweets_df['full_text']), :].index)

        cleaned_tweets_df = self.filter_dataframe(merged_tweets_df)
        self.create_cleaned_tweets_file(cleaned_tweets_df, cleaned_tweets_file)

        return summary_row_dict

    @staticmethod
    def populate_summary(merged_tweets_df):
        print(len(merged_tweets_df.index))
        print(merged_tweets_df['is_tweet_locations_inc_india'].value_counts())
        print(merged_tweets_df['is_user_india_based'].value_counts())
        print(len(merged_tweets_df.loc[pd.isnull(merged_tweets_df['full_text']), :].index))

    def download_tweet_file(self, download_tweets_file_path, downloaded_tweets_file):
        print('Download File Url Path : {}'.format(download_tweets_file_path))
        print('Looking for Downloaded File Path : {}'.format(downloaded_tweets_file))
        if not os.path.exists(downloaded_tweets_file):
            print('File not available. Downloading...')
            wget.download(download_tweets_file_path, out=self.downloaded_tweets_files_dir)
        else:
            print('File already exists')
        print()

    def extract_tweet_file(self, downloaded_tweets_file, extracted_tweets_file):
        print('Looking for Extracted File Path : {}'.format(extracted_tweets_file))
        if os.path.exists(downloaded_tweets_file) and not os.path.exists(extracted_tweets_file):
            print('File not available. Extracting...')
            with zipfile.ZipFile(downloaded_tweets_file, 'r') as zip_ref:
                zip_ref.extractall(self.extracted_tweets_files_dir)
        else:
            print('File already exists')
        print()

    def create_merged_dataframe(self, filtered_tweets_df, hydrated_tweets_file):
        print('Looking for Hydrated File Path : {}'.format(hydrated_tweets_file))
        if not filtered_tweets_df.empty and os.path.exists(hydrated_tweets_file):
            print('Hydrated File available. Creating merged dataframe...')
            hydrated_tweets_df = self.read_json_file_to_dataframe(hydrated_tweets_file)
            merged_tweets_df = filtered_tweets_df.merge(hydrated_tweets_df.rename(columns={'id': 'tweet_id'}),
                                                        how='left', on='tweet_id')
        else:
            print("Dataframe doesn't exist or File already exists")
            merged_tweets_df = pd.DataFrame()
        print()

        return merged_tweets_df

    @staticmethod
    def create_filtered_tweet_ids_file(filtered_tweets_df, filtered_tweets_file):
        print('Looking for Filtered File Path : {}'.format(filtered_tweets_file))
        if not (filtered_tweets_df.empty or os.path.exists(filtered_tweets_file)):
            print('File not available. Creating filtered file...')
            filtered_tweets_df.to_csv(filtered_tweets_file,
                                      columns=['tweet_id'], header=False, index=False)
        else:
            print("Dataframe doesn't exist or File already exists")
        print()

    @staticmethod
    def create_cleaned_tweets_file(cleaned_tweets_df, cleaned_tweets_file):
        print('Looking for Cleaned File Path : {}'.format(cleaned_tweets_file))
        if not (cleaned_tweets_df.empty or os.path.exists(cleaned_tweets_file)):
            print('File not available. Creating cleaned file...')
            cleaned_tweets_df.to_csv(cleaned_tweets_file, index=False)
        else:
            print("Dataframe doesn't exist or File already exists")
        print()

    @staticmethod
    def hydrate_tweets_from_file(filtered_tweets_file, hydrated_tweets_file):
        print('Looking for Hydrated File Path : {}'.format(hydrated_tweets_file))
        if not os.path.exists(hydrated_tweets_file):
            print('File not available. Hydrating...')
            os.system('twarc hydrate ' + filtered_tweets_file + '> ' + hydrated_tweets_file)
        else:
            print('File already exists')
        print()

    @staticmethod
    def filter_dataframe(tweets_df):
        cleaned_tweets_df = tweets_df.loc[~pd.isnull(tweets_df['full_text'])]
        cleaned_tweets_df = cleaned_tweets_df.loc[:, ['tweet_id', 'is_user_india_based',
                                                      'full_text'
                                                      ]]

        return cleaned_tweets_df
