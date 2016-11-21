var Search = React.createClass({
  put: function(change) {
    $.ajax({
      type: 'PUT',
      url: '/api/search/' + this.props.id,
      data: JSON.stringify(change),
      dataType: 'json',
      contentType: 'application/json'
    });
  },
  unpublish: function() {
    this.put({id: this.props.id, published: false});
  },
  publish: function() {
    this.put({id: this.props.id, published: true});
  },
  remove: function() {
    $.ajax({
      type: 'DELETE',
      url: '/api/search/' + this.props.id,
      data: JSON.stringify({}),
      dataType: 'json',
      contentType: 'application/json'
    });
  }, 
  render: function() {
    var link = <a href={"/summary/" + this.props.date_path}>{this.props.text}</a>;
    if (! (this.props.status == "FINISHED: RunFlow")) {
      link = this.props.text;
    }

    if (this.props.canModify) {
      if (this.props.published) {
        var publishButton =
          <button
            onClick={ this.unpublish } 
            className="unpublish">Unpublish</button>;
      } else {
        var publishButton = 
          <button
            onClick={ this.publish }
            className="publish">Publish</button>;
      }
      var buttons =
        <td>
          { publishButton }
          <button 
            onClick={this.remove}
            className="delete">Delete</button>
        </td>
    }

    return (
      <tr className="search item">
        <td>{ formatDateTime(this.props.created) }</td>
        <td>{link}</td>
        <td><a href={"https://twitter.com/" + this.props.user}>{this.props.user}</a></td>
        <td>{ formatDateTime(this.props.published) }</td>
        <td>{this.props.status}</td>
        { buttons }
      </tr>
    );
  }
});

var SearchList = React.createClass({
  render: function() {
    var user = this.props.user;
    var includePublished = this.props.includePublished;
    var searches = this.props.searches.filter(function(search) {
      return (search.user == user) || (search.published && includePublished);
    });
    var searchNodes = searches.map(function(search) {
      return (
        <Search text={search.text}
          key={search.id}
          id={search.id}
          date_path={search.date_path}
          status={search.status}
          user={search.user}
          canModify={search.user == user} 
          created={search.created}
          published={search.published}>

        </Search>
      );
    });
    return (
      <table className="searchList">
        <thead>
          <tr>
            <th>Created</th>
            <th>Search</th>
            <th>Creator</th>
            <th>Published</th>
            <th>Job Status</th>
            <th className="actions">Actions</th>
          </tr>
        </thead>
        <tbody>
        {searchNodes}
        </tbody>
      </table>
    );
  }
});

var SearchForm = React.createClass({
  getInitialState: function() {
    return {text: '', count: '1000'};
  },
  handleTextChange: function(e) {
    this.setState({text: e.target.value});
  },
  handleCountChange: function(e) {
    this.setState({count: e.target.value});
  },
  handleSubmit: function(e) {
    e.preventDefault();
    var text = this.state.text.trim();
    var count = this.state.count.trim();
    if (!text || !count) {
      return;
    }
    this.props.onSearchSubmit({text: text, count: count});
    this.setState({text: '', count: '1000'});
  },
  render: function() {
    return (
      <form className="searchForm" onSubmit={this.handleSubmit}>
        search: <input type='text'
          value={this.state.text}
          onChange={this.handleTextChange} />
        &nbsp;
        num tweets: <input type='text'
          size="6"
          maxSize="6"
          value={this.state.count}
          onChange={this.handleCountChange} />
        &nbsp;
        <input type='submit' value='create dataset!' />
      </form>
    );
  }
});

var SearchBox = React.createClass({
  loadSearchesFromServer: function() {
    $.ajax({
      url: this.props.url,
      dataType: 'json',
      cache: false,
      success: function(data) {
        this.setState({searches: data.searches, user: data.user});
      }.bind(this),
      error: function(xhr, status, err) {
        console.error(this.props.url, status, err.toString());
      }.bind(this)
    });
  },
  handleIncludePublishedChange: function(e) {
    this.setState({includePublished: e.target.checked});
  },
  handleSearchSubmit: function(search) {
    $.ajax({
      // url: this.props.url,
      url: '/searches/',
      dataType: 'json',
      type: 'POST',
      data: search,
      success: function(data) {
        this.setState({searches: data.searches, user: data.user});
      }.bind(this),
      error: function(xhr, status, err) {
        if (xhr.responseJSON) {
          this.setState({error: xhr.responseJSON.error});
        }
      }.bind(this)
    });
  },
  getInitialState: function() {
    return {searches: [], user: null, includePublished: true};
  },
  componentDidMount: function() {
    this.loadSearchesFromServer();
    setInterval(this.loadSearchesFromServer, this.props.pollInterval);
  },
  render: function() {
    if (this.state.user) {
      var includePublished = 
          <div id="includePublished">
            include datasets published by others? 
            &nbsp;
            <input type='checkbox' 
              checked={this.state.includePublished}
              onChange={this.handleIncludePublishedChange} />
          </div>
    } else {
      var includePublished = null;
    }
    return (
      <div className="searchBox">
        <h3 style={{color: 'red'}}>{ this.state.error }</h3>
        <SearchForm onSearchSubmit={this.handleSearchSubmit} />
        <br />
        { includePublished }
        <SearchList 
          includePublished={this.state.includePublished}
          user={this.state.user}
          searches={this.state.searches} />
      </div>
    );
  }
});

function formatDateTime(t) {
    if (t) {
        return $.format.date(new Date(t), 'yyyy-MM-dd HH:mm:ss');
    } else {
        return null;
    }
}

ReactDOM.render(
  <SearchBox url="/api/searches/" pollInterval={2000} />,
  document.getElementById('searches')
);
