import csv
import os
from datetime import datetime, timedelta

from tweets_files import TweetsFiles


class GeoCovid19Tweets:

    def __init__(self):
        self.cleaned_tweets_files_dir = 'cleaned_tweets_files'

        self.tweet = TweetsFiles(self.cleaned_tweets_files_dir)

        self.summaries = []

        self.summary_file_name = 'summary.csv'
        self.summary_fieldnames = ['file_name', 'total_tweets', 'india_specific_tweets', 'outside_india_tweets',
                                   'india_specific_tweets_with_full_text', 'india_specific_tweets_without_full_text',
                                   'india_full_text_tweet_india_users', 'india_full_text_tweet_outside_india_users', ]

    def process_date_range_files(self, tweets_start_date, tweets_end_date, date_format,
                                 download_only_mode, ):

        tweets_file_base_url_for_date_files = 'https://crisisnlp.qcri.org/covid_data/en_geo_files'
        tweets_date_file_name_template = 'en_geo_{}'

        start_date = datetime.strptime(tweets_start_date, date_format)
        end_date = datetime.strptime(tweets_end_date, date_format)

        while start_date <= end_date:
            file_name = tweets_date_file_name_template.format(datetime.strftime(start_date, date_format))
            summary_row_dict = self.tweet.process_tweet_file(tweets_file_base_url_for_date_files,
                                                             file_name, download_only_mode)
            start_date = start_date + timedelta(days=1)

            self.add_to_summary(summary_row_dict)

    def process_misc_update_file(self, download_only_mode):
        tweets_file_base_url_for_misc_file = 'https://crisisnlp.qcri.org/covid_data/misc_updates'

        misc_update_file = 'geo_place_update_11_6_2020'

        summary_row_dict = self.tweet.process_tweet_file(tweets_file_base_url_for_misc_file, misc_update_file,
                                                         download_only_mode)
        self.add_to_summary(summary_row_dict)

    def add_to_summary(self, summary_row_dict):
        if bool(summary_row_dict):
            self.summaries.append(summary_row_dict)

    def process_tweet_files(self, download_only_mode, start_date, end_date, date_format,
                            process_misc_update_file=False):

        if not os.path.exists(self.summary_file_name):
            with open(self.summary_file_name, 'a', newline='') as csvFile:
                writer = csv.DictWriter(csvFile, fieldnames=self.summary_fieldnames)
                writer.writeheader()

        self.process_date_range_files(start_date, end_date, date_format, download_only_mode)

        if process_misc_update_file:
            self.process_misc_update_file(download_only_mode)

        if len(self.summaries) > 0:
            with open(self.summary_file_name, 'a', newline='') as csvFile:
                writer = csv.DictWriter(csvFile, fieldnames=self.summary_fieldnames)
                for summary_row_dict in self.summaries:
                    writer.writerow(summary_row_dict)

        self.tweet.cleanup()

    def initiate_processing_tweet_files(self):
        download_only_mode = True

        tweets_start_date = '2020-02-01'
        tweets_end_date = '2020-02-02'
        date_format = '%Y-%m-%d'

        process_misc_update_file = False

        self.process_tweet_files(download_only_mode, tweets_start_date, tweets_end_date, date_format,
                                 process_misc_update_file)


if __name__ == '__main__':
    geo_tweets = GeoCovid19Tweets()
    geo_tweets.initiate_processing_tweet_files()
