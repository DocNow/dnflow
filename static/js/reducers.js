import { combineReducers } from 'redux'

const toggleItemReducer = (state = [], action) => {
  console.log("action: " + action.type + " " + action.id)
  switch (action.type) {
    case 'TOGGLE_ITEM':
      var newstate = [...state];
      if (newstate.includes(action.id)) {
        console.log('newstate: ' + newstate)
        console.log('newstate includes ' + action.id)
          newstate = newstate.filter(x => x != action.id)
      } else {
        newstate.push(action.id)
      }
      console.log("state: " + newstate)
      return newstate
    default:
      return state
  }
}

export default combineReducers({
  toggleItemReducer
})
