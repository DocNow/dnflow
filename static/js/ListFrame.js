import React, { Component, PropTypes } from 'react'
import { connect } from 'react-redux'

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
    const { store } = this.context;
    this.unsubscribe = store.subscribe(() => this.forceUpdate());
    this.loadListFromServer();
  },

  componentWillUnmount: function() {
    this.unsubscribe()
  },

  render: function() {
    return (
      <div className="listFrame">
        <h2>List</h2>
        <List
          items={this.state.data}
          />
      </div>
    );
  }
});

ListFrame.propTypes = {
  items: PropTypes.array.isRequired,
}

ListFrame.contextTypes = {
  store: React.PropTypes.object
};


export default ListFrame
