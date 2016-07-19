import React from 'react'
import { render } from 'react-dom'
import { Provider } from 'react-redux'
import { createStore } from 'redux'

import ListContainer from './ListContainer'
import reducer from './reducers'

let store = createStore(
  reducer,
  window.devToolsExtension && window.devToolsExtension()
);

render(
  <Provider store={store}>
    <ListContainer />
  </Provider>,
  document.getElementById('list1')
)
