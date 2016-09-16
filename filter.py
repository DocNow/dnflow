#!/usr/bin/env python

"""
Store a filterstream in a series of JSON files.  These are created
as tempfiles and moved to the target dir when appending is finished
to accommodate Spark Streaming's requirements for watching folders.
See:

http://spark.apache.org/docs/latest/streaming-programming-guide.html#basic-sources

...for details.
"""

import argparse
import json
import logging
import os
import shutil
import tempfile
import time

from twarc import Twarc


logging.getLogger().setLevel(logging.INFO)


def time_now_filename():
    return time.strftime('%Y%m%dT%H%M%S', time.gmtime())


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
    for tweet in t.search(args.track_terms):
        tweet_counter += 1
        fp.write(json.dumps(tweet) + '\n')
        if time.time() - time_start > args.interval:
            fp.close()
            shutil.move(fp.name, fname)
            logging.debug('Wrote %s tweets to %s' % (tweet_counter, fname))
            time_start = time.time()
            tweet_counter = 0
            file_counter += 1
            fp = tempfile.NamedTemporaryFile('w', buffering=True, delete=False)
            fname = '%s-%03d.json' % (fname_base, file_counter)
