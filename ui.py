import logging
import sqlite3

from flask_oauthlib.client import OAuth
from flask import g, jsonify, request, redirect, session, flash, make_response
from flask import Flask, render_template, url_for, send_from_directory, abort
import pandas as pd
import redis
from rq import Queue
import numpy as np 
from sampler import sampler
from queue_tasks import run_flow

import json
import csv
import numpy as np
import json2csv
from numpy.random import shuffle

# configure application

app = Flask(__name__)
app.config.from_pyfile('dnflow.cfg')

redis_conn = redis.StrictRedis(
    host=app.config['REDIS_HOST'],
    port=app.config['REDIS_PORT'],
    charset='utf-8',
    decode_responses=True
)

q = Queue(connection=redis_conn)

logging.getLogger().setLevel(logging.DEBUG)


# twitter authentication


oauth = OAuth()
twitter = oauth.remote_app('twitter',
    base_url='https://api.twitter.com/1.1/',
    request_token_url='https://api.twitter.com/oauth/request_token',
    access_token_url='https://api.twitter.com/oauth/access_token',
    authorize_url='https://api.twitter.com/oauth/authenticate',
    access_token_method='GET',
    consumer_key=app.config['TWITTER_CONSUMER_KEY'],
    consumer_secret=app.config['TWITTER_CONSUMER_SECRET']
)


@app.route('/login')
def login():
    next = request.args.get('next') or request.referrer or None
    callback_url = 'http://' + app.config['HOSTNAME'] + url_for('oauth_authorized', next=next)
    return twitter.authorize(callback=callback_url)


@app.route('/logout')
def logout():
    del session['twitter_token']
    del session['twitter_user']
    return redirect('/')


@app.route('/oauth-authorized')
def oauth_authorized():
    next_url = request.args.get('next') or url_for('index')
    resp = twitter.authorized_response()
    if resp is None:
        flash(u'You denied the request to sign in.')
        return redirect(next_url)
    session['twitter_token'] = (
        resp['oauth_token'],
        resp['oauth_token_secret']
    )
    session['twitter_user'] = resp['screen_name']
    flash('You were signed in as %s' % resp['screen_name'])
    return redirect(next_url)


@twitter.tokengetter
def get_twitter_token(token=None):
    return session.get('twitter_token')


# webapp routes


@app.route('/static/<path:path>')
def send_static(path):
    return send_from_directory('/static', path)

@app.errorhandler(404)
def page_not_found(error):
    return 'This route does not exist {}'.format(request.url), 404


@app.before_request
def before_request():
    g.db = connect_db()
    g.db.row_factory = sqlite3.Row


def connect_db():
    return sqlite3.connect(app.config['DATABASE'])


def query(sql, args=(), one=False, json=False):
    c = g.db.execute(sql, args)
    rv = c.fetchall()
    c.close()
    if json:
        return [{k: r[k] for k in r.keys()} for r in rv]
    return (dict(rv[0]) if rv else None) if one else rv


@app.teardown_request
def teardown_request(exception):
    db = getattr(g, 'db', None)
    if db is not None:
        db.close()


@app.context_processor
def inject_user():
    return dict(twitter_user=session.get('twitter_user', None))


@app.context_processor
def inject_analytics():
    return dict(google_analytics=app.config.get('GOOGLE_ANALYTICS'))


@app.route('/', methods=['GET'])
def index():
    return render_template('index.html', title='dnflow prototype home')


@app.route('/searches/', methods=['POST'])
def add_search():
    text = request.form.get('text', None)
    user = session.get('twitter_user', None)
    if not user:
        response = jsonify({"error": "✋ please login first, thanks!"})
        response.status_code = 403
        return response
    try:
        count = request.form.get('count', None)
        count = int(count)
    except:
        count = 1000
    if text:
        sql = '''
            INSERT INTO searches (text, date_path, user)
            VALUES (?, ?, ?)
            '''
        query(sql, [request.form['text'], '', user])
        g.db.commit()
        r = query(sql='SELECT last_insert_rowid() AS job_id FROM searches',
                  one=True)
        job_id = r['job_id']
        job = q.enqueue_call(
            run_flow,
            args=(
                text,
                job_id,
                count,
                session['twitter_token'][0],
                session['twitter_token'][1]
            ),
            timeout=app.config['MAX_TIMEOUT']
        )
        logging.debug('job: %s' % job)
    return redirect(url_for('index'))


@app.route('/job/', methods=['PUT'])
def job():
    job_id = request.form.get('job_id', None)
    date_path = request.form.get('date_path', None)
    status = request.form.get('status', None)

    # A job is starting, we want the date_path
    if job_id and date_path:
        query('UPDATE searches SET date_path = ? WHERE id = ?',
              [date_path, job_id])
        logging.debug('update date_path=%s where id=%s' % (date_path, job_id))
        g.db.commit()
    # A job is in progress, we want the status
    if date_path and status:
        query('UPDATE searches SET status = ? WHERE date_path = ?',
              [status, date_path])
        logging.debug('update status=%s where date_path=%s' % (status,
                                                               date_path))
        g.db.commit()
    return redirect(url_for('index'))


@app.route('/summary/<date_path>/', methods=['GET'])
def summary(date_path):
    user = session.get('twitter_user', None)
    search = query('SELECT * FROM searches WHERE date_path = ?', [date_path],
                   one=True)

    if not search['published'] and user != search['user']:
        abort(401)

    return render_template('summary.html', title=search['text'], search=search)


@app.route('/summary/<date_path>/<path:file_name>', methods=['GET'])
def summary_static_proxy(date_path, file_name):
    user = session.get('twitter_user', None)
    search = query('SELECT * FROM searches WHERE date_path = ?', [date_path],
                   one=True)

    if not search['published'] and user != search['user']:
        abort(401)

    fname = '%s/%s' % (date_path, file_name)
    return send_from_directory(app.config['DATA_DIR'], fname)


@app.route('/summary/<int:search_id>/compare', methods=['GET'])
def summary_compare(search_id):
    search = query('SELECT * FROM searches WHERE id = ?', [search_id],
                   one=True)
    compare_ids = request.args.getlist('id')
    return render_template('summary_compare.html', search=search,
                           compare_ids=compare_ids)


@app.route('/summary/<date_path>/sample/', methods=['POST'])
def sample(date_path):
    try:
        sample_size = int(request.form.get('sample_size', None))
    except ValueError:
        return redirect(url_for('summary', date_path=date_path))
    #s = sampler(sample_size,date_path)
    #s.resample()
    with open('data/%s/summary.json' % date_path,'r') as summary_file:    
        summary = json.load(summary_file)
    num_tweets = summary['num_tweets']
    index = np.arange(num_tweets)
    shuffle(index)
    index = index[0:sample_size]
    counter = 0
    with open('data/%s/sample.csv' % date_path, 'w') as sample_file:
        writer = csv.writer(sample_file) 
        writer.writerow(json2csv.get_headings())
        with open('data/%s/tweets.json' % date_path,'r') as tweets_file:
            for line in tweets_file:
                tweet = json.loads(line)
                if counter in index:
                    writer.writerow(json2csv.get_row(tweet))
                counter += 1
    return redirect(url_for('summary', date_path=date_path))
        #print('data/%s/sample.csv' % date_path)
        #return app.send_static_file('data/%s/sample.csv' % date_path)
        #return send_from_directory('data/%s/' % date_path, 'sample.csv')


@app.route('/feed/')
def feed():
    searches = query(
        '''
        SELECT * FROM searches 
        WHERE published IS NOT NULL 
        ORDER BY id DESC
        ''', json=True)
    site_url = 'http://' + app.config['HOSTNAME']
    feed_url = site_url + '/feed/'
    def add_url(s):
        s['url'] = site_url + '/summary/' + s['date_path'] + '/'
        return s
    searches = map(_date_format, searches)
    searches = list(map(add_url, searches))
    resp = make_response(
        render_template(
            'feed.xml', 
            updated=searches[0]['created'],
            site_url=site_url,
            feed_url=feed_url,
            searches=searches
        )
    )
    resp.headers['Content-Type'] = 'application/atom+xml'
    return resp


@app.route('/robots.txt')
def robots():
    resp = make_response(render_template('robots.txt'))
    resp.headers['Content-Type'] = 'text/plain'
    return resp


# api routes for getting data

@app.route('/api/searches/', methods=['GET'])
def api_searches():
    user = session.get('twitter_user', None)
    q = '''
        SELECT * 
        FROM searches 
        WHERE user = ?
          OR published IS NOT NULL
        ORDER BY id DESC
        '''
    searches = query(q, [user], json=True)
    searches = {
        "user": user,
        "searches": list(map(_date_format, searches))
    }
    return jsonify(searches)


@app.route('/api/search/<int:search_id>', methods=["GET", "PUT", "DELETE"])
def search(search_id):
    search = query('SELECT * FROM searches WHERE id = ?', [search_id], one=True)
    if not search:
        abort(404)

    # they must own the search to modify it
    user = session.get('twitter_user', None)
    if request.method in ['PUT', 'DELETE'] and search['user'] != user:
        abort(401)

    if request.method == 'PUT':
        new_search = request.get_json()
        if new_search['published']:
            query("UPDATE searches SET published = CURRENT_TIMESTAMP WHERE id = ? AND published IS NULL", [search_id])
        elif not new_search['published']:
            query("UPDATE searches SET published = NULL WHERE id = ?",
                  [search_id])
        g.db.commit()
    elif request.method == 'DELETE':
        query("DELETE FROM searches WHERE id = ?", [search_id])
        g.db.commit()

    return jsonify(_date_format(search))


@app.route('/api/hashtags/<int:search_id>/', methods=['GET'])
def hashtags_multi(search_id):
    ids = [search_id]
    ids.extend(request.args.getlist('id'))
    in_clause = ','.join([str(i) for i in ids])
    searches = query("""
        SELECT id, date_path, text
        FROM searches WHERE id in (%s)
        """ % in_clause)
    summary = []
    search = searches[0]
    summary.append({'id': search['id'], 'date_path': search['date_path'],
                    'text': search['text'],
                    'colname': 'count_%s' % search['id']})
    d = pd.read_csv('data/%s/count-hashtags.csv' % search['date_path'])
    d = d.rename(columns={'count': 'count_%s' % search['id']})
    for search in searches[1:]:
        summary.append({'id': search['id'], 'date_path': search['date_path'],
                        'text': search['text'],
                        'colname': 'count_%s' % search['id']})
        e = pd.read_csv('data/%s/count-hashtags.csv' % search['date_path'])
        e = e.rename(columns={'count': 'count_%s' % search['id']})
        d = pd.merge(d, e, on='hashtag', how='outer').fillna(0)
    d.sort_values(by='count_%s' % search_id, inplace=True, ascending=False)
    result = {'summary': summary, 'hashtags': d.to_dict(orient='record')}
    return jsonify(result)


@app.route('/api/searches/<date_path>/hashtags/', methods=['GET'])
def hashtags(date_path):
    d = _count_entities(date_path, 'hashtags', 'hashtag')
    return jsonify(d)


@app.route('/api/searches/<date_path>/mentions/', methods=['GET'])
def mentions(date_path):
    d = _count_entities(date_path, 'mentions', 'screen_name')
    return jsonify(d)


def _count_entities(date_path, entity, attrname):
    try:
        # range query is 0-indexed
        num = int(request.args.get('num', 24)) - 1
    except:
        num = 24
    counts = redis_conn.zrevrange('count:%s:%s' % (entity, date_path), 0, num,
                                  True)
    return [{attrname: e, 'count': c} for e, c in counts]


def _date_format(row):
    for name in ['created', 'published']:
        t = row[name]
        if t:
            t = t.replace(' ', 'T')
            t += 'Z'
            row[name] = t
    return row


if __name__ == '__main__':
    app.run(debug=app.config['DEBUG'])
