#!/usr/bin/env python
"""
test.py - initial attempt at automating dn flows using luigi
"""

from collections import Counter
import hashlib
import json
import logging
import os
import time
from urllib.parse import urlparse

import luigi
import twarc

logging.getLogger().setLevel(logging.WARN)


def time_hash(digits=6):
    """Generate an arbitrary hash based on the current time for filenames."""
    hash = hashlib.sha1()
    hash.update(str(time.time()).encode())
    return hash.hexdigest()[:digits]


def localstrftime():
    return time.strftime('%Y-%m-%dT%H%M', time.localtime())


class Twarcy(object):

    _c_key = os.environ.get('CONSUMER_KEY')
    _c_secret = os.environ.get('CONSUMER_SECRET')
    _a_token = os.environ.get('ACCESS_TOKEN')
    _a_token_secret = os.environ.get('ACCESS_TOKEN_SECRET')
    _twarc = twarc.Twarc(consumer_key=_c_key,
                         consumer_secret=_c_secret,
                         access_token=_a_token,
                         access_token_secret=_a_token_secret)


class TestTask(luigi.Task):
    x = luigi.IntParameter()
    y = luigi.IntParameter()

    def run(self):
        print(self.x + self.y)


class FetchTweets(luigi.Task, Twarcy):
    date_minute = luigi.Parameter()
    term = luigi.Parameter()
    lang = luigi.Parameter(default='en')
    count = luigi.IntParameter(default=1000)

    def output(self):
        fname = 'data/%s-tweets.json' % self.date_minute
        logging.debug('FETCH: writing to local target %s' % fname)
        return luigi.LocalTarget(fname)

    def run(self):
        logging.debug('FETCH: fetching tweets')
        i = 0
        tweets = []
        logging.debug('FETCH: searching for "%s"' % self.term)
        for tweet in self._twarc.search(self.term, lang=self.lang):
            i += 1
            if i > self.count:
                logging.debug('FETCH: %s hits reached' % self.count)
                break
            tweets.append(tweet)

        with self.output().open('w') as fp_out:
            for tweet in tweets:
                fp_out.write(json.dumps(tweet) + '\n')
        logging.debug('FETCH: done writing file')


class CountHashtags(luigi.Task):
    date_minute = luigi.Parameter()
    term = luigi.Parameter()

    def requires(self):
        return FetchTweets(date_minute=self.date_minute, term=self.term)

    def output(self):
        fname = self.input().fn.replace('.json', '-hashtags.json')
        logging.debug('hashtag count filename is %s' % fname)
        return luigi.LocalTarget(fname)

    def run(self):
        c = Counter()
        fp_counts = self.output().open('w')
        for tweet_str in self.input().open('r'):
            tweet = json.loads(tweet_str)
            c.update([ht['text'].lower()
                      for ht in tweet['entities']['hashtags']])
        json.dump(c, fp_counts, sort_keys=True, indent=2)
        fp_counts.close()


class CountUrls(luigi.Task):
    date_minute = luigi.Parameter()
    term = luigi.Parameter()

    def requires(self):
        return FetchTweets(date_minute=self.date_minute, term=self.term)

    def output(self):
        fname = self.input().fn.replace('.json', '-urls.json')
        logging.debug('url count filename is %s' % fname)
        return luigi.LocalTarget(fname)

    def run(self):
        c = Counter()
        fp_counts = self.output().open('w')
        for tweet_str in self.input().open('r'):
            tweet = json.loads(tweet_str)
            c.update([url['expanded_url'].lower()
                      for url in tweet['entities']['urls']])
        json.dump(c, fp_counts, sort_keys=True, indent=2)
        fp_counts.close()


class CountDomains(luigi.Task):
    date_minute = luigi.Parameter()
    term = luigi.Parameter()

    def requires(self):
        return FetchTweets(date_minute=self.date_minute, term=self.term)

    def output(self):
        fname = self.input().fn.replace('.json', '-domains.json')
        logging.debug('domain count filename is %s' % fname)
        return luigi.LocalTarget(fname)

    def run(self):
        c = Counter()
        fp_counts = self.output().open('w')
        for tweet_str in self.input().open('r'):
            tweet = json.loads(tweet_str)
            c.update([urlparse(url['expanded_url']).netloc.lower()
                      for url in tweet['entities']['urls']])
        json.dump(c, fp_counts, sort_keys=True, indent=2)
        fp_counts.close()


class CountMentions(luigi.Task):
    date_minute = luigi.Parameter()
    term = luigi.Parameter()

    def requires(self):
        return FetchTweets(date_minute=self.date_minute, term=self.term)

    def output(self):
        fname = self.input().fn.replace('.json', '-mentions.json')
        logging.debug('mention count filename is %s' % fname)
        return luigi.LocalTarget(fname)

    def run(self):
        c = Counter()
        fp_counts = self.output().open('w')
        for tweet_str in self.input().open('r'):
            tweet = json.loads(tweet_str)
            c.update([m['screen_name'].lower()
                     for m in tweet['entities']['user_mentions']])
        json.dump(c, fp_counts, sort_keys=True, indent=2)
        fp_counts.close()


class RunFlow(luigi.Task):
    date_minute = luigi.Parameter(
            default=time.strftime(luigi.DateMinuteParameter.date_format))
    term = luigi.Parameter()

    def requires(self):
        return CountHashtags(date_minute=self.date_minute, term=self.term), \
            CountUrls(date_minute=self.date_minute, term=self.term), \
            CountDomains(date_minute=self.date_minute, term=self.term), \
            CountMentions(date_minute=self.date_minute, term=self.term)
