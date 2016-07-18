import { PropTypes } from 'react'

import ListItem from './ListItem'

const List = ({ listItems, toggleItem }) => (
  <ul>
    {listItems.map(listItem =>
      <ListItem key={listItem.id}
        id={listItem.id}
        value={listItem.value}
        onClick={() => toggleItem(listItem.id)}
      />
    )}
  </ul>
);

List.propTypes = {
  listItems: PropTypes.arrayOf(PropTypes.shape({
    id: PropTypes.number.isRequired,
    value: PropTypes.number.isRequired
  }).isRequired).isRequired,
  toggleItem: PropTypes.func.isRequired
}

export default List
