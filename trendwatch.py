#!/usr/bin/env python

import argparse
import logging
import os
import sqlite3
import time

from flask.config import Config

import twarc


# According to Twitter's trends/place docs:
#   https://dev.twitter.com/rest/reference/get/trends/place
# ...results are cached for five minutes, so we wait five minutes between
# calls.
DELAY_SECONDS = 60 * 5


config = Config(os.path.dirname(__file__))
config.from_pyfile('dnflow.cfg')

logging.getLogger().setLevel(logging.INFO)
logging.getLogger('').setLevel(logging.INFO)


def get_twarc():
    return twarc.Twarc(
        consumer_key=config['TWITTER_CONSUMER_KEY'],
        consumer_secret=config['TWITTER_CONSUMER_SECRET'],
        access_token=config['TWITTER_ACCESS_TOKEN'],
        access_token_secret=config['TWITTER_ACCESS_TOKEN_SECRET']
    )


def query(sql, args=(), one=False, json=False):
    conn = sqlite3.connect(config['DATABASE'])
    c = conn.cursor()
    c.execute(sql, args)
    rv = c.fetchall()
    conn.commit()
    conn.close()
    if json:
        return [{k: r[k] for k in r.keys()} for r in rv]
    return (dict(rv[0]) if rv else None) if one else rv


def fetch_locations():
    """Grab a fresh set of trend locations available, stash in db"""
    query('DELETE FROM trend_locations')
    t = get_twarc()
    trend_locations = t.trends_available()
    sql = """
        INSERT INTO trend_locations
            (woeid, country_code, url, name,
             country, type_name,
             type_code, parent_woeid)
        VALUES (?, ?, ?, ?,
            ?, ?,
            ?, ?)
        """
    for tl in trend_locations:
        args = [tl['woeid'], tl['countryCode'], tl['url'], tl['name'],
                tl['country'], tl['placeType']['name'],
                tl['placeType']['code'], tl['parentid']]
        query(sql=sql, args=args)


def fetch_trends(woeid):
    """Grab and stash the trends for the specific woeid"""
    t = get_twarc()
    trends = t.trends_place(woeid=woeid)[0]['trends']
    sql = """
        INSERT INTO trends
            (woeid, ts_fetch, query, tweet_volume,
             url, promoted_content, name)
        VALUES (?, CURRENT_TIMESTAMP, ?, ?,
            ?, ?, ?)
        """
    for trend in trends:
        args = [woeid, trend['query'], trend['tweet_volume'] or 0,
                trend['url'], trend['promoted_content'] or 0, trend['name']]
        query(sql=sql, args=args)


def main():
    parser = argparse.ArgumentParser(description='capture Twitter trends')
    parser.add_argument('woeid', nargs='+', type=int,
                        help='one or more WOEIDs to fetch trends for')
    args = parser.parse_args()

    # fetch all locations first
    fetch_locations()

    # now fetch current trends in a time-delayed loop
    while True:
        for woeid in args.woeid:
            fetch_trends(woeid)
        time.sleep(DELAY_SECONDS)


if __name__ == '__main__':
    main()
