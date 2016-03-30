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
import requests
import twarc

logging.getLogger().setLevel(logging.DEBUG)


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
    name = luigi.Parameter()
    term = luigi.Parameter()
    lang = luigi.Parameter(default='en')
    count = luigi.IntParameter(default=1000)

    def output(self):
        fname = 'data/%s-tweets.json' % self.name
        return luigi.LocalTarget(fname)

    def run(self):
        i = 0
        tweets = []
        for tweet in self._twarc.search(self.term, lang=self.lang):
            i += 1
            if i > self.count:
                break
            tweets.append(tweet)

        with self.output().open('w') as fp_out:
            for tweet in tweets:
                fp_out.write(json.dumps(tweet) + '\n')


class CountHashtags(luigi.Task):
    name = luigi.Parameter()
    term = luigi.Parameter()

    def requires(self):
        return FetchTweets(name=self.name, term=self.term)

    def output(self):
        fname = self.input().fn.replace('.json', '-hashtags.json')
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
    name = luigi.Parameter()
    term = luigi.Parameter()

    def requires(self):
        return FetchTweets(name=self.name, term=self.term)

    def output(self):
        fname = self.input().fn.replace('.json', '-urls.json')
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
    name = luigi.Parameter()
    term = luigi.Parameter()

    def requires(self):
        return FetchTweets(name=self.name, term=self.term)

    def output(self):
        fname = self.input().fn.replace('.json', '-domains.json')
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
    name = luigi.Parameter()
    term = luigi.Parameter()

    def requires(self):
        return FetchTweets(name=self.name, term=self.term)

    def output(self):
        fname = self.input().fn.replace('.json', '-mentions.json')
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


class CountMedia(luigi.Task):
    name = luigi.Parameter()
    term = luigi.Parameter()

    def requires(self):
        return FetchTweets(name=self.name, term=self.term)

    def output(self):
        fname = self.input().fn.replace('.json', '-media.json')
        return luigi.LocalTarget(fname)

    def run(self):
        c = Counter()
        fp_counts = self.output().open('w')
        for tweet_str in self.input().open('r'):
            tweet = json.loads(tweet_str)
            c.update([m['media_url']
                     for m in tweet['entities'].get('media', [])
                     if m['type'] == 'photo'])
        json.dump(c, fp_counts, sort_keys=True, indent=2)
        fp_counts.close()


class FetchMedia(luigi.Task):
    name = luigi.Parameter()
    term = luigi.Parameter()
    media_files = []

    def requires(self):
        return CountMedia(name=self.name, term=self.term)

    def run(self):
        dirname = 'data/%s-media' % self.name
        os.makedirs(dirname, exist_ok=True)
        # lots of hits to same server, so let requests pool connections
        session = requests.Session()
        media = json.load(self.input().open('r'))

        for url, count in media.items():
            parsed_url = urlparse(url)
            fname = parsed_url.path.split('/')[-1]
            if len(fname) == 0:
                continue
            self.media_files.append('%s/%s' % (dirname, fname))
            r = session.get(url)
            if r.ok:
                with open('%s/%s' % (dirname, fname), 'wb') as media_file:
                    media_file.write(r.content)

    def output(self):
        # ensure only one successful fetch for each url
        # unless FetchTweets is called again with a new hash
        return [luigi.LocalTarget(fname) for fname in self.media_files]


class RunFlow(luigi.Task):
    name = time_hash()
    term = luigi.Parameter()
    # lang = luigi.Parameter(default='en')
    # count = luigi.IntParameter(default=1000)

    def requires(self):
        return CountHashtags(name=self.name, term=self.term), \
            CountUrls(name=self.name, term=self.term), \
            CountDomains(name=self.name, term=self.term), \
            CountMentions(name=self.name, term=self.term), \
            CountMedia(name=self.name, term=self.term), \
            FetchMedia(name=self.name, term=self.term)
