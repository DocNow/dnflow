export const TOGGLE_ITEM = 'TOGGLE_ITEM'

export const toggleItem = (id) => {
  return({ type: TOGGLE_ITEM, id: id })
}
