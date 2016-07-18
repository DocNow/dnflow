export const TOGGLE_ITEM = 'TOGGLE_ITEM'

export function toggleItem(index) {
  return({ type: TOGGLE_ITEM, index: index })
}
