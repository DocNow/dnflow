#!/usr/bin/env python

"""
Store a filterstream in a series of JSON files.  These are created
as tempfiles and moved to the target dir when appending is finished
to accommodate Spark Streaming's requirements for watching folders.
See:

http://spark.apache.org/docs/latest/streaming-programming-guide.html#basic-sources

...for details.

FIXME: hard-coded to filter track for now.

Note/TODO:  on an up-to-date OSX machine with spark-2.0.0 installed
via homebrew, start this script with spark-submit, e.g.:

  spark-submit filter.py --interval 3 justice

This script will both start spark and a spark stream handler and
a twitter filter stream.  Tweets will be written to --dir every
--interval seconds.  The Spark stream handler will look for new files
written into that directory every SPARK_BATCH_INTERVAL seconds.
"""

import argparse
import json
import logging
import os
import shutil
import tempfile
import time

from flask import Flask
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import redis
from twarc import Twarc


logging.getLogger().setLevel(logging.INFO)

# Using Flask infrastructure for config to save code
app = Flask(__name__)
app.config.from_pyfile('dnflow.cfg')

redis_conn = redis.StrictRedis(
    host=app.config['REDIS_HOST'],
    port=app.config['REDIS_PORT'],
    charset='utf-8',
    decode_responses=True
)


def time_now_filename():
    return time.strftime('%Y%m%dT%H%M%S', time.gmtime())


def process_tweets(tweet_rdd):
    # Start by reading the JSON and caching into memory; will save
    # time if we do anything more than just count tweets
    tweet_json = tweet_rdd.map(lambda line: json.loads(line)) \
        .cache()
    hashtag_counts = tweet_json.flatMap(
        lambda tweet: [(ht['text'].lower(), 1)
                       for ht in tweet['entities']['hashtags']]) \
        .reduceByKey(lambda a, b: a + b) \
        .collect()
    pipe = redis_conn.pipeline()
    # Populate minute-based redis keys with counts; we'll aggregate by min
    set_name = 'stream:hashtag:%s' % time.strftime('%H%M', time.gmtime())
    for hashtag, count in hashtag_counts:
        pipe.zincrby(set_name, hashtag, count)
    pipe.execute()


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Filter Twitter stream to files.')
    parser.add_argument('--dir', dest='dir', default='data/filter',
                        help='target directory in which to store files')
    parser.add_argument('--interval', dest='interval', type=int, default=10,
                        help='interval, in seconds, for each file')
    parser.add_argument('track_terms', nargs='+', default=None,
                        help='terms to track')
    parser.add_argument('--consumer_key',
                        default=None, help="Twitter API consumer key")
    parser.add_argument('--consumer_secret',
                        default=None, help="Twitter API consumer secret")
    parser.add_argument('--access_token',
                        default=None, help="Twitter API access key")
    parser.add_argument('--access_token_secret',
                        default=None, help="Twitter API access token secret")
    parser.add_argument('--verbose', default=False, action='store_true',
                        help='verbose debugging output')
    args = parser.parse_args()

    try:
        os.makedirs(args.dir, exist_ok=True)
    except:
        parser.error('Unable to create dir: %s' % args.dir)

    if not args.track_terms:
        parser.error('Enter some terms to track')

    # Set up spark; pyspark defaults to python2.7 if we don't tell it
    # otherwise
    os.environ['PYSPARK_PYTHON'] = 'python3'
    sc = SparkContext()
    ssc = StreamingContext(sc, app.config['SPARK_BATCH_INTERVAL'])
    # startup overhead can take a little while
    time.sleep(15)

    # Spark's streaming will watch for new files in args.dir
    incoming_tweets = ssc.textFileStream(args.dir)
    # Send each new batch through process()
    incoming_tweets.foreachRDD(process_tweets)
    # Kick of directory watching
    ssc.start()
    # Stop Spark when the time comes (TODO: is this correct?)
    # ssc.awaitTermination()

    consumer_key = args.consumer_key or os.environ.get('CONSUMER_KEY')
    consumer_secret = args.consumer_secret or os.environ.get('CONSUMER_SECRET')
    access_token = args.access_token or os.environ.get('ACCESS_TOKEN')
    access_token_secret = args.access_token_secret \
        or os.environ.get('ACCESS_TOKEN_SECRET')

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    t = Twarc(consumer_key=consumer_key,
              consumer_secret=consumer_secret,
              access_token=access_token,
              access_token_secret=access_token_secret)

    time_start = time.time()
    file_counter = 0
    fname_base = '%s/%s' % (args.dir, time_now_filename())
    tweet_counter = 0
    fname = '%s-%03d.json' % (fname_base, file_counter)
    fp = tempfile.NamedTemporaryFile('w', buffering=True, delete=False)
    # FIXME: track is hard-coded
    for tweet in t.filter(track=args.track_terms):
        tweet_counter += 1
        fp.write(json.dumps(tweet) + '\n')
        if time.time() - time_start > args.interval:
            fp.close()
            shutil.move(fp.name, fname)
            time_start = time.time()
            tweet_counter = 0
            file_counter += 1
            fp = tempfile.NamedTemporaryFile('w', buffering=True, delete=False)
            fname = '%s-%04d.json' % (fname_base, file_counter)
