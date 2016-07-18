import { connect } from 'react-redux'

import { toggleItem } from './actions'
import List from './List'

let url = "/api/list10/"

let ListFrame = React.createClass({
  getInitialState: function() {
    return {data: []};
  },
  loadListFromServer: function() {
    $.ajax({
      url: url,
      dataType: 'json',
      cache: false,
      success: function(data) {
        this.setState({data: data});
      }.bind(this),
      error: function(xhr, status, err) {
        console.error(url, status, err.toString());
      }.bind(this)
    });
  },
  componentDidMount: function() {
    this.loadListFromServer();
  },
  render: function() {
    return (
      <div className="listFrame">
        <h2>List {this.props.id}</h2>
        <List
          listItems={this.state.data}
          toggleItem={toggleItem}
          />
      </div>
    );
  }
});

export default ListFrame
