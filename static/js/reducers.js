import { combineReducers } from 'redux'

const items = (state=[], action) => {
    switch (action.type) {
      case 'TOGGLE_ITEM':
        return state
      default:
        return state
    }
}

const highlights = (state=[], action) => {
  switch (action.type) {
    case 'TOGGLE_ITEM':
      let newstate = [...state]
      if (newstate.includes(action.id)) {
        newstate = newstate.filter(x => x != action.id)
      } else {
        newstate.push(action.id)
      }
      return newstate
    default:
      return state
  }
}

export default combineReducers({
  items: items,
  highlights: highlights
})
