import { combineReducers } from 'redux'

const toggleItemReducer = (state = [], action) => {
  switch (action.type) {
    case 'TOGGLE_ITEM':
      var state = [...state];
      if (state.includes(action.id)) {
        while(state.includes(action.id)) {
          state.pop(action.id)
        }
      } else {
        state.push(action.id)
      }
      console.log("state: " + state)
      return state
    default:
      return state
  }
}

const highlighter = combineReducers({
  toggleItemReducer
})

export default highlighter
