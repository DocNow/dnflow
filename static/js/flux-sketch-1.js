import React from 'react'
import { render } from 'react-dom'
import { Provider } from 'react-redux'
import { createStore } from 'redux'

import ListContainer from './ListContainer'
import highlighter from './reducers'

let store = createStore(highlighter)

render(
  <Provider store={store}>
    <ListContainer />
  </Provider>,
  document.getElementById('list1')
)
