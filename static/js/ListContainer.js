import React from 'react'
import { connect } from 'react-redux'

import ListFrame from './ListFrame'
import { toggleItem } from './actions'

const mapStateToProps = (state) => {
  return {
    items: state.items,
    highlights: state.highlights
  }
}

const mapDispatchToProps = (dispatch) => {
  return {
    onItemClick: (id) => {
      dispatch(toggleItem(id))
    }
  }
}

const ListContainer= connect(
  mapStateToProps,
  mapDispatchToProps
)(ListFrame)

export default ListContainer
