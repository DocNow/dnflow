import logging
import sqlite3

import redis
from rq import Queue

from flask import Flask, render_template, url_for, send_from_directory
from flask import g, jsonify, request, redirect

from queue_tasks import run_flow


# FIXME: these should be in a separate config file
DEBUG = True
DATABASE = 'db.sqlite3'
SECRET_KEY = 'a super secret key'
STATIC_URL_PATH = '/static'
DATA_DIR = 'data'
MAX_TIMEOUT = 2 * 60 * 60  # two hours should be long enough

# FIXME: host hard-coded, and decode_responses shouldn't always be True
redis_conn = redis.StrictRedis(host='localhost', charset='utf-8',
                               decode_responses=True)
q = Queue(connection=redis_conn)

app = Flask(__name__, static_url_path=STATIC_URL_PATH)
app.config.from_object(__name__)

logging.getLogger().setLevel(logging.DEBUG)


def connect_db():
    return sqlite3.connect(app.config['DATABASE'])


@app.route('/static/<path:path>')
def send_static(path):
    return send_from_directory(STATIC_URL_PATH, path)


@app.before_request
def before_request():
    g.db = connect_db()
    g.db.row_factory = sqlite3.Row


def query(sql, args=(), one=False, json=False):
    c = g.db.execute(sql, args)
    rv = c.fetchall()
    c.close()
    if json:
        return [{k: r[k] for k in r.keys()} for r in rv]
    return (rv[0] if rv else None) if one else rv


@app.teardown_request
def teardown_request(exception):
    db = getattr(g, 'db', None)
    if db is not None:
        db.close()


@app.route('/', methods=['GET'])
def index():
    searches = query('SELECT * FROM searches ORDER BY id DESC')
    return render_template('index.html', searches=searches)


@app.route('/new', methods=['POST'])
def add_search():
    text = request.form.get('text', None)
    try:
        count = request.form.get('count', None)
        count = int(count)
    except:
        count = 1000
    if text:
        query('INSERT INTO searches (text, date_path) VALUES (?, ?)',
              [request.form['text'], ''])
        g.db.commit()
        r = query(sql='SELECT last_insert_rowid() AS job_id FROM searches',
                  one=True)
        job_id = r['job_id']
        job = q.enqueue_call(run_flow, args=(text, job_id, count),
                             timeout=app.config['MAX_TIMEOUT'])
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


@app.route('/api/searches/', methods=['GET'])
def api_searches():
    searches = query('SELECT * FROM searches ORDER BY id DESC', json=True)
    return jsonify(searches)


@app.route('/summary/<date_path>/', methods=['GET'])
def summary(date_path):
    return render_template('summary.html')


@app.route('/summary/<date_path>/<path:file_name>', methods=['GET'])
def summary_static_proxy(date_path, file_name):
    fname = '%s/%s' % (date_path, file_name)
    return send_from_directory(app.config['DATA_DIR'], fname)


def _count_entities(date_path, entity, attrname):
    try:
        # range query is 0-indexed
        num = int(request.args.get('num', 24)) - 1
    except:
        num = 24
    counts = redis_conn.zrevrange('count:%s:%s' % (entity, date_path), 0, num,
                                  True)
    return [{attrname: e, 'count': c} for e, c in counts]


@app.route('/q/<date_path>/count-hashtags/', methods=['GET'])
def q_count_hashtags(date_path):
    d = _count_entities(date_path, 'hashtags', 'hashtag')
    return jsonify(d)


@app.route('/q/<date_path>/count-mentions/', methods=['GET'])
def q_count_mentions(date_path):
    d = _count_entities(date_path, 'mentions', 'screen_name')
    return jsonify(d)


if __name__ == '__main__':
    app.run(debug=app.config['DEBUG'])
