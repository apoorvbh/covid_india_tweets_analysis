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
            output = output.lower().strip()
            output = ' '.join(re.sub("[.,!?:;-=*]", " ", output).split())
            output = re.sub("\\s+", "", output)
        return output
