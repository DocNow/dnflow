import React, { PropTypes } from 'react'

import ListItem from './ListItem'
import { toggleItem } from './actions'

let List = (props, { store }) => (
  <ul>
    {props.items.map(item =>
      <ListItem key={item.id}
        id={item.id}
        value={item.value}
        onClick={() => store.dispatch(toggleItem(item.id))}
      />
    )}
  </ul>
);

List.propTypes = {
  items: PropTypes.arrayOf(PropTypes.shape({
    id: PropTypes.number.isRequired,
    value: PropTypes.number.isRequired
  }).isRequired).isRequired,
  // onClick: PropTypes.func.isRequired
}
List.contextTypes = {
  store: React.PropTypes.object
}

export default List
