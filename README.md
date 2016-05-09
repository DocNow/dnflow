# dnflow

An early experiment in automating a series of actions with Twitter
data for docnow.

Uses [Luigi](http://luigi.readthedocs.org/) for workflow automation.


## running it for yourself

The current `summarize.py` is set up to collect a handful of tweets
based on a search, then execute a series of counts against it.  This
will result in one data file (the source tweets) and several count
files (with the same name under `data/` but with extensions like
`-urls`, `-hashtags` added on.

Assuming you either have an activated virtualenv or similar sandbox,
install the requirements first:
```
% pip install -r requirements
```

Start the `luigid` central scheduler, best done in another terminal:
```
% luigid
```

To test the workflow, run the following to kick it off (substituting a
search term of interest):
```
% python -m luigi --module summarize RunFlow --term lahoreblast
```

It may take a moment to execute the search, which will require repeated
calls to the Twitter API.  As soon as it completes, you should have all
the mentioned files in your `data/` directory.  The naming scheme isn't
well thought out.  This is only a test.

While you're at it, take a look at the web ui for luigi's scheduler at:

    http://localhost:8082/

(Assuming you didn't change the port when you started luigid.)


## adding the flask UI

`ui.py` contains a simple web app that allows a search to be specified
through the web, queueing workflows to execute in the background, and
showing workflow process status and links to completed summaries as well.
Running the web UI takes a few more steps.

 * Install and run [Redis](http://redis.io/)

Redis can be run without configuration changes, best done in another
terminal:

```
% redis-server
```

 * Start a [Redis Queue](http://python-rq.org/) worker

RQ requires a running instance of Redis and one or more workers, also
best done in another terminal.  Within your dnflow virtual environment:

```
% rq worker
```

 * Create the flask UI backend

A simple SQLite3 database tracks the searches you will create and their
workflow status.  Within your dnflow virtual environment:

```
% sqlite3 db.sqlite3 < schema.sql
```

 * Start the [flask](http://flask.pocoo.org/) UI

The flask UI shows a list of existing searches, lets you add new ones,
and links to completed search summaries.  Again, within your dnflow
virtual environment, and probably in yet another terminal window:

```
% python ui.py
```


### The flow, for now

The luigi workflow is not automated; it needs to be invoked explicitly.
The web UI is the wrong place to invoke the workflow because the
workflow can run for a long time, yet the UI needs to remain
responsive.  For these reasons, the process is separated out with
the queue.

When a search is added, dnflow adds a job to the queue by defining
a Python subprocess to call the luigi workflow from the commandline.
RQ enqueues this task for later processing.  If one or more RQ
workers are available, the job is assigned and begins.  Because
dnflow's enqueueing of the job is very fast, it can return an updated
view promptly.

The luigi workflow takes as long as it needs, generating static files
in a distinct directory for each requested search.

Integration between the web UI and workflows occurs in the UI's
SQLite database, where search terms are stored with a job id.  When
the workflow is assigned to an RQ worker, that search record is
updated through an HTTP PUT to the web app at the URL `/job`, with
a reference to the job id and its output directory.  Each individual
task within the workflow further updates this same URL with additional
PUTs upon task start, success, or failure.  This is handled using
[Luigi's event
model](http://luigi.readthedocs.io/en/stable/api/luigi.event.html) and
HTTP callbacks/hooks to the UI keep the integration between the two
pieces of the environment simple.  During a workflow, the most recent
task status will be recorded in the database, and is available for
display in the UI.

With these pieces in place, several requests for new searches can
be added rapidly within the UI.  Each search will be run by the
next available RQ worker process, so if only one process is available,
they will execute in succession, but with more than one worker running,
multiple workflows can run in parallel.  The main limitation here
is the rate limit on Twitter's API.
