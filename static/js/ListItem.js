import React, { PropTypes } from 'react'
import { connect } from 'react-redux'

import { toggleItem } from './actions'

const mapStateToProps = (state, ownProps) => {
  return {id: ownProps.id, value: ownProps.value}
}
const mapDispatchToProps = (dispatch, ownProps) => {
  return {
    onClick: () => {
      dispatch(toggleItem(ownProps.id))
    }
  }
}
let ListItem = (props) => {
  return (
    <li className="listItem"
      onClick={props.onClick}
      >
      {props.id} : {props.value}
    </li>
  );
};
ListItem = connect(
  mapStateToProps,
  mapDispatchToProps
)(ListItem)

ListItem.propTypes = {
  id: PropTypes.number.isRequired,
  value: PropTypes.number.isRequired,
  onClick: PropTypes.func.isRequired
}
ListItem.contextTypes = {
  store: React.PropTypes.object
}

export default ListItem
