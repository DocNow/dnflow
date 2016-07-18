export const TOGGLE_ITEM = 'TOGGLE_ITEM'

export function toggleItem(index) {
  console.log({ type: TOGGLE_ITEM, index: index })
  return({ type: TOGGLE_ITEM, index: index })
}
