package flexqueue

import "container/list"

// FlexList is a high performance ordered map that maintains constant amortized
// time O(1) for all read/write operations. Internally it uses a combination of
// a double linked list for item order and a map of strings for an index.
type FlexList struct {
	items   *list.List
	indices map[string]*list.Element
}

// NewFlexList factory func should always be used to instantiate a new FlexList
func NewFlexList() *FlexList {
	return &FlexList{
		items:   list.New(),
		indices: make(map[string]*list.Element),
	}
}

// ItemWrapper retains the relationship between the linked list and the index map
type ItemWrapper struct {
	index string
	item  interface{}
}

// PushFront will create the index and add the item to the front of the list. If
// the index already exists then the operation is ignored.
// Returns:
// * bool: true if a new item was inserted or false if it already existed
func (l *FlexList) PushFront(index string, item interface{}) bool {

	if _, ok := l.indices[index]; !ok {
		l.indices[index] = l.items.PushFront(&ItemWrapper{
			index: index,
			item:  item,
		})
		return true
	}

	return false
}

// PushBack will create the index and add the item to the back of the list. If
// the index already exists then the operation is ignored.
// Returns:
// * bool: true if a new item was inserted or false if it already existed
func (l *FlexList) PushBack(index string, item interface{}) bool {

	if _, ok := l.indices[index]; !ok {
		l.indices[index] = l.items.PushBack(&ItemWrapper{
			index: index,
			item:  item,
		})
		return true
	}

	return false
}

// PullFront will remove an item from the front of the list and return it.
// This is an alias for ReadFront() + Remove().
// Returns:
// * string: The index of the item
// * interface{}: The item
// * bool: true if an item was found and false for an empty list
func (l *FlexList) PullFront() (string, interface{}, bool) {

	if index, item, ok := l.ReadFront(); ok {
		_ = l.Remove(index)
		return index, item, ok
	}

	return "", nil, false
}

// PullBack will return an item from the back of the list and then remove it.
// This is an alias for ReadBack() + Remove().
// Returns:
// * string: The index of the item
// * interface{}: The item
// * bool: true if an item was found and false for an empty list
func (l *FlexList) PullBack() (string, interface{}, bool) {

	if index, item, ok := l.ReadBack(); ok {
		_ = l.Remove(index)
		return index, item, ok
	}

	return "", nil, false
}

// Pull will return an item from the list based on the index and then remove it.
// This is an alias for Read() + Remove()
// Returns:
// * interface{}: The item
// * bool: true if the item was found
func (l *FlexList) Pull(index string) (interface{}, bool) {

	if item, ok := l.Read(index); ok {
		_ = l.Remove(index)
		return item, ok
	}

	return nil, false
}

// ReadFront will return an item from the front of the list without removing it.
// Returns:
// * string: The index of the item
// * interface{}: The item
// * bool: true if an item was found and false for an empty list
func (l *FlexList) ReadFront() (string, interface{}, bool) {

	if item := l.items.Front(); item != nil {
		wrapper := item.Value.(*ItemWrapper)
		return wrapper.index, wrapper.item, true
	}

	return "", nil, false
}

// ReadBack will return an item from the back of the list without removing it.
// Returns:
// * string: The index of the item
// * interface{}: The item
// * bool: true if an item was found and false for an empty list
func (l *FlexList) ReadBack() (string, interface{}, bool) {

	if item := l.items.Back(); item != nil {
		wrapper := item.Value.(*ItemWrapper)
		return wrapper.index, wrapper.item, true
	}

	return "", nil, false
}

// Read will return an item from the list based on the index without removing it.
// Returns:
// * interface{}: The item
// * bool: true if the item was found
func (l *FlexList) Read(index string) (interface{}, bool) {

	if item, ok := l.indices[index]; ok {
		wrapper := item.Value.(*ItemWrapper)
		return wrapper.item, true
	}

	return nil, false
}

// Update will update an item in the list based on its index without changing the
// order. The operation is ignored if the item does not already exist.
// Returns:
// * bool: true if the item was updated and false if not found
func (l *FlexList) Update(index string, item interface{}) bool {

	if oldItem, ok := l.indices[index]; ok {
		l.indices[index] = l.items.InsertAfter(&ItemWrapper{
			index: index,
			item:  item,
		}, oldItem)
		_ = l.items.Remove(oldItem)
		return true
	}

	return false
}

// Remove will delete an item from the list based on the index.
// Returns:
// * bool: true if the item was removed and false if not found
func (l *FlexList) Remove(index string) bool {

	if item, ok := l.indices[index]; ok {
		_ = l.items.Remove(item)
		delete(l.indices, index)
		return true
	}

	return false
}

// Has will return true if the list contains the given index and
// false if it does not.
func (l *FlexList) Has(index string) bool {
	_, ok := l.indices[index]
	return ok
}

// Len will return the number of items in the list
func (l *FlexList) Len() int {
	return l.items.Len()
}
