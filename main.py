from tweets_files import TweetsFiles


def process_tweet_files():
    downloaded_tweets_files_dir = 'downloaded_tweets_files'
    extracted_tweets_files_dir = 'extracted_tweets_files'
    filtered_tweets_files_dir = 'filtered_tweets_files'
    hydrated_tweets_files_dir = 'hydrated_tweets_files'
    merged_tweets_files_dir = 'merged_tweets_files'

    tweets_collection_start_date = '2020-02-01'
    tweets_collection_end_date = '2020-02-02'

    tweet = TweetsFiles(downloaded_tweets_files_dir, extracted_tweets_files_dir,
                        filtered_tweets_files_dir, hydrated_tweets_files_dir,
                        merged_tweets_files_dir,
                        tweets_collection_start_date,
                        tweets_collection_end_date)

    tweet.process_tweets_files()


if __name__ == '__main__':
    process_tweet_files()
