import csv
import json
import sys
from urllib.parse import urlparse

from pyspark import SparkContext


def write_csv(fname, data, col1, col2):
    """csv-writing boilerplate"""
    fn = sys.argv[1].replace('tweets.json', fname)
    with open(fn, 'w') as fp:
        writer = csv.DictWriter(fp, delimiter=',', quoting=csv.QUOTE_MINIMAL,
                                fieldnames=[col1, col2])
        writer.writeheader()
        for val1, val2 in data:
            writer.writerow({col1: val1, col2: val2})


if __name__ == "__main__":
    sc = SparkContext()
    # load the tweets set into an RDD and cache it for speedy processing
    tweets = sc.textFile(sys.argv[1]) \
        .map(lambda line: json.loads(line)) \
        .cache()

    # hashtag counts
    hashtag_counts = tweets.flatMap(
        lambda tweet: [(ht['text'].lower(), 1)
                       for ht in tweet['entities']['hashtags']]) \
        .reduceByKey(lambda a, b: a + b) \
        .collect()
    write_csv('count-hashtags.csv', hashtag_counts, 'hashtag', 'count')

    # hashtag edgelist
    hashtag_edgelist = tweets.flatMap(
        lambda t: [(t['user']['screen_name'], ht['text'].lower())
                   for ht in t['entities']['hashtags']]) \
        .collect()
    write_csv('edgelist-hashtags.csv', hashtag_edgelist, 'user', 'hashtag')

    # urls
    url_counts = tweets.flatMap(
        lambda tweet: [(url['expanded_url'].lower(), 1)
                       for url in tweet['entities']['urls']]) \
        .reduceByKey(lambda a, b: a + b) \
        .collect()
    write_csv('count-urls.csv', url_counts, 'url', 'count')

    # domains
    domain_counts = tweets.flatMap(
        lambda tweet: [(urlparse(url['expanded_url']).netloc.lower(), 1)
                       for url in tweet['entities']['urls']]) \
        .reduceByKey(lambda a, b: a + b) \
        .collect()
    write_csv('count-domains.csv', domain_counts, 'domain', 'count')

    # mentions
    mention_counts = tweets.flatMap(
        lambda tweet: [(m['screen_name'].lower(), 1)
                       for m in tweet['entities']['user_mentions']]) \
        .reduceByKey(lambda a, b: a + b) \
        .collect()
    write_csv('count-mentions.csv', mention_counts, 'screen_name', 'count')

    # mention edgelist
    mention_edgelist = tweets.flatMap(
        lambda t: [(t['user']['screen_name'], mention['screen_name'])
                   for mention in t['entities']['user_mentions']]) \
        .collect()
    write_csv('edgelist-mentions.csv', mention_edgelist, 'from_user',
              'to_user')

    # photos
    photo_counts = tweets.flatMap(
        lambda tweet: [(m['media_url'], 1)
                       for m in tweet['entities'].get('media', [])
                       if m['type'] == 'photo']) \
        .reduceByKey(lambda a, b: a + b) \
        .collect()
    write_csv('count-photos.csv', photo_counts, 'url', 'count')

    # followers
    follower_counts = tweets.map(
        lambda tweet: (tweet['user']['screen_name'],
                       tweet['user']['followers_count'])) \
        .distinct() \
        .collect()
    write_csv('count-followers.csv', follower_counts, 'user', 'count')

    # follow ratio
    follow_ratios = tweets.map(
        lambda t: (t['user']['screen_name'],
                   (int(t['user']['followers_count']),
                    int(t['user']['friends_count'])))) \
        .filter(lambda f: f[1][1] > 0) \
        .map(lambda f: (f[0], "{0:.2f}".format(f[1][0] / float(f[1][1])))) \
        .distinct() \
        .collect()
    write_csv('follow-ratio.csv', follow_ratios, 'user', 'count')

    # write out this file to confirm the task is complete
    with open(sys.argv[2], 'w') as fp_stop_sign:
        fp_stop_sign.write('1')
