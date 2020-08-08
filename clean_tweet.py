import preprocessor as p


class CleanTweet:
    def __init__(self):
        p.set_options(p.OPT.RESERVED, p.OPT.MENTION, p.OPT.URL)

    @staticmethod
    def process_tweet(tweet):
        output = tweet.lower()
        output = p.tokenize(p.clean(output))

        return output
