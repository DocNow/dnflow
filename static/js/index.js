var Search = React.createClass({
  render: function() {
    var link = <a href={"/summary/" + this.props.date_path}>{this.props.text}</a>;
    if (! (this.props.status == "FINISHED: RunFlow")) {
      link = this.props.text;
    }
    var t = $.format.date(new Date(this.props.created), 'yyyy-MM-dd HH:mm:ss');
    return (
      <tr className="search item">
        <td>{t}</td>
        <td>{link}</td>
        <td><a href={"https://twitter.com/" + this.props.twitter_user}>{this.props.twitter_user}</a></td>
        <td>{this.props.status}</td>
      </tr>
    );
  }
});

var SearchList = React.createClass({
  render: function() {
    var searchNodes = this.props.data.map(function(search) {
      return (
        <Search text={search.text}
          key={search.id}
          id={search.id}
          date_path={search.date_path}
          status={search.status}
          twitter_user={search.twitter_user}
          created={search.created}>

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
            <th>Status</th>
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
        terms: <input type='text'
          value={this.state.text}
          onChange={this.handleTextChange} />
        &nbsp;
        count: <input type='text'
          size="6"
          maxSize="6"
          value={this.state.count}
          onChange={this.handleCountChange} />
        &nbsp;
        <input type='submit' value='add' />
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
        this.setState({data: data});
      }.bind(this),
      error: function(xhr, status, err) {
        console.error(this.props.url, status, err.toString());
      }.bind(this)
    });
  },
  handleSearchSubmit: function(search) {
    $.ajax({
      // url: this.props.url,
      url: '/searches/',
      dataType: 'json',
      type: 'POST',
      data: search,
      success: function(data) {
        this.setState({data: data});
      }.bind(this),
      error: function(xhr, status, err) {
        if (xhr.responseJSON) {
          this.setState({error: xhr.responseJSON.error});
        }
      }.bind(this)
    });
  },
  getInitialState: function() {
    return {data: []};
  },
  componentDidMount: function() {
    this.loadSearchesFromServer();
    setInterval(this.loadSearchesFromServer, this.props.pollInterval);
  },
  render: function() {
    return (
      <div className="searchBox">
        <h3 style={{color: 'red'}}>{ this.state.error }</h3>
        <SearchForm onSearchSubmit={this.handleSearchSubmit} />
        <br />
        <SearchList data={this.state.data} />
      </div>
    );
  }
});


ReactDOM.render(
  <SearchBox url="/api/searches/" pollInterval={2000} />,
  document.getElementById('searches')
);
