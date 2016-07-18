import React from 'react'
import { connect } from 'react-redux'

import ListFrame from './ListFrame'


const mapStateToProps = (state) => {
  return {
    data: state.data
  }
}

const mapDispatchToProps = (dispatch) => {
  return {
    onListItemClick: (id) => {
      dispatch(toggleItem(id))
    }
  }
}

const ListContainer = connect(
  mapStateToProps,
  mapDispatchToProps
)(ListFrame)

export default ListContainer
