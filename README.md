# dnflow

An early experiment in automating a series of actions with Twitter
data for docnow.

Uses [Luigi](http://luigi.readthedocs.org/) for workflow automation.


## running it for yourself

The current `test.py` is set up to collect a handful of tweets based on
a search, then execute a series of counts against it.  This will result
in one data file (the source tweets) and several count files (with the
same name under `data/` but with extensions like `-urls`, `-hashtags`
added on.

Assuming you either have an activated virtualenv or similar sandbox,
install the requirements first:
```
% pip install -r requirements
```

Start the `luigid` central scheduler, best done in another terminal:
```
% luigid
```

Finally, run the following to kick it off (substituting a search term 
of interest):
```
% python -m luigi --module test RunFlow --term lahoreblast
```

It may take a moment to execute the search, which will require repeated
calls to the Twitter API.  As soon as it completes, you should have all
the mentioned files in your `data/` directory.  The naming scheme isn't
well thought out.  This is only a test.

While you're at it, take a look at the web ui for luigi's scheduler at:

    http://localhost:8082/

(Assuming you didn't change the port when you started luigid.)
