import csv
import os
from datetime import datetime

from tweets_files import TweetsFiles


def process_tweet_files():
    cleaned_tweets_files_dir = 'cleaned_tweets_files'

    tweets_collection_start_date = '2020-02-01'
    tweets_collection_end_date = '2020-02-02'
    date_format = '%Y-%m-%d'

    summary_file_name = 'summary.csv'
    summary_fieldnames = ['file_name', 'total_tweets', 'tweets_india', 'tweets_outside_india',
                          'user_india', 'user_outside_india', 'tweet_with_full_text',
                          'tweet_without_full_text']

    if not os.path.exists(summary_file_name):
        with open(summary_file_name, 'a', newline='') as csvFile:
            writer = csv.DictWriter(csvFile, fieldnames=summary_fieldnames)
            writer.writeheader()

    start_date = datetime.strptime(tweets_collection_start_date, date_format)
    end_date = datetime.strptime(tweets_collection_end_date, date_format)

    tweet = TweetsFiles(cleaned_tweets_files_dir)

    summaries = tweet.process_tweets_date_range_files(start_date, end_date, date_format)

    if len(summaries) > 0:
        with open(summary_file_name, 'a', newline='') as csvFile:
            writer = csv.DictWriter(csvFile, fieldnames=summary_fieldnames)
            for summary_row_dict in summaries:
                writer.writerow(summary_row_dict)

    tweet.cleanup()


if __name__ == '__main__':
    process_tweet_files()
