import re

import preprocessor as p


class CleanTweet:
    def __init__(self):
        p.set_options(p.OPT.RESERVED, p.OPT.MENTION, p.OPT.URL, p.OPT.HASHTAG,
                      p.OPT.EMOJI, p.OPT.SMILEY)

    @staticmethod
    def process_tweet(tweet):
        output = tweet
        if output and isinstance(output, str):
            output = p.tokenize(p.clean(tweet))
            output = re.sub(r'[.,!?:;=*\-]', ' ', output)
            output = re.sub(r'\s+', ' ', output)
            output = output.lower().strip()
        return output
