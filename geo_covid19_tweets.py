import csv
import os
from datetime import datetime, timedelta

from tweets_files import TweetsFiles


class GeoCovid19Tweets:

    @staticmethod
    def process_tweet_files():
        cleaned_tweets_files_dir = 'cleaned_tweets_files'

        tweets_collection_start_date = '2020-02-01'
        tweets_collection_end_date = '2020-05-01'
        date_format = '%Y-%m-%d'

        summary_file_name = 'summary.csv'
        summary_fieldnames = ['file_name', 'total_tweets', 'india_specific_tweets', 'outside_india_tweets',
                              'india_specific_tweets_with_full_text', 'india_specific_tweets_without_full_text',
                              'india_full_text_tweet_india_users', 'india_full_text_tweet_outside_india_users', ]

        tweets_file_base_url_for_date_files = 'https://crisisnlp.qcri.org/covid_data/en_geo_files'
        tweets_file_base_url_for_misc_file = 'https://crisisnlp.qcri.org/covid_data/misc_updates'
        tweets_date_file_name_template = 'en_geo_{}'
        misc_update_file = 'geo_place_update_11_6_2020'
        download_only_mode = True

        if not os.path.exists(summary_file_name):
            with open(summary_file_name, 'a', newline='') as csvFile:
                writer = csv.DictWriter(csvFile, fieldnames=summary_fieldnames)
                writer.writeheader()

        start_date = datetime.strptime(tweets_collection_start_date, date_format)
        end_date = datetime.strptime(tweets_collection_end_date, date_format)

        tweet = TweetsFiles(cleaned_tweets_files_dir)

        summaries = []

        while start_date <= end_date:
            file_name = tweets_date_file_name_template.format(datetime.strftime(start_date, date_format))
            summary_row_dict = tweet.process_tweet_file(tweets_file_base_url_for_date_files,
                                                        file_name, download_only_mode)
            start_date = start_date + timedelta(days=1)
            if bool(summary_row_dict):
                summaries.append(summary_row_dict)

        summary_row_dict = tweet.process_tweet_file(tweets_file_base_url_for_misc_file, misc_update_file,
                                                    download_only_mode)

        if bool(summary_row_dict):
            summaries.append(summary_row_dict)

        if len(summaries) > 0:
            with open(summary_file_name, 'a', newline='') as csvFile:
                writer = csv.DictWriter(csvFile, fieldnames=summary_fieldnames)
                for summary_row_dict in summaries:
                    writer.writerow(summary_row_dict)

        tweet.cleanup()


if __name__ == '__main__':
    geo_tweets = GeoCovid19Tweets()
    geo_tweets.process_tweet_files()
