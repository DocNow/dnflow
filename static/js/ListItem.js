import { PropTypes } from 'react'

import { toggleItem } from './actions'

let ListItem = ({ id, value, onClick }) => {
  return (
    <li className="listItem"
      onClick={onClick}
      >
      {id} : {value}
    </li>
  );
};

ListItem.propTypes = {
  onClick: PropTypes.func.isRequired,
  id: PropTypes.number.isRequired,
  value: PropTypes.number.isRequired
}

export default ListItem
