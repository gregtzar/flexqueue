package flexqueue

import (
	"sync"
	"time"
)

const (
	NoMax = -1
)

// FlexQueue is a combined FIFO/LIFO single lane queue with all the features of
// FlexList but also supporting mutex thread safety, max queue length, message
// de-duplication and ttl/expiration.
type FlexQueue struct {
	sync.RWMutex                // Shared mutex for locking
	messages     FlexList       // An ordered map of messages
	ttl          map[string]TTL // A table of TTL controls keyed by digest
	max          int            // The max queue length
}

// TTL is an expiration control that applies to a single message.
type TTL struct {
	Expires  time.Time
	Callback func(digest string, message interface{})
}

// Expired will check the ttl expires time against now and return true if it
// is expired and false if not.
func (ttl *TTL) Expired() bool {
	return time.Now().After(ttl.Expires)
}

// NewTTL creates a new TTL control for the duration based on now
func NewTTL(ttl time.Duration, callback func(digest string, message interface{})) *TTL {
	return &TTL{
		Expires:  time.Now().Add(ttl),
		Callback: callback,
	}
}

// NewFlexQueue is a factory method for creating a new flex queue. It is
// important to use this method to properly initialize the internal structs.
func NewFlexQueue() *FlexQueue {
	return &FlexQueue{
		messages: *NewFlexList(),
		ttl:      make(map[string]TTL),
		max:      NoMax,
	}
}

func (q *FlexQueue) SetMax(max int) *FlexQueue {
	if max > NoMax {
		q.max = max
	}
	return q
}

// PushFront will add a new message to the front of the queue. It returns true
// if the message was added or if it already existed in the queue based on
// the digest value (automatic de-duping), and false if the message was
// not added because the queue was full. If de-dupe occurs then the message will
// not be updated.
func (q *FlexQueue) PushFront(digest string, message interface{}) bool {

	q.Lock()
	defer q.Unlock()

	return q.pushFB(true, digest, message)
}

// PushBack will add a new message to the end of the queue. It returns true
// if the message was added or if it already existed in the queue based on
// the digest value (automatic de-duping), and false if the message was
// not added because the queue was full. If de-dupe occurs then the message will
// not be updated.
func (q *FlexQueue) PushBack(digest string, message interface{}) bool {

	q.Lock()
	defer q.Unlock()

	return q.pushFB(false, digest, message)
}

// pushFB will push a message into the queue unless it is full
func (q *FlexQueue) pushFB(front bool, digest string, message interface{}) bool {

	// Job de-duplication: Just return true now if the digest already exists
	// in the message list. Its important to perform this check before the limit
	// check otherwise de-dupes could still return false if the queue is full.
	if q.messages.Has(digest) {
		return true
	}

	// Disallow the push if the queue is already full
	if q.max > NoMax && q.messages.Len() >= q.max {
		return false
	}

	var ok bool

	// The last thing we do is add the message to the list
	if front {
		ok = q.messages.PushFront(digest, message)
	} else {
		ok = q.messages.PushBack(digest, message)
	}

	return ok
}

// PushFrontTTL will add a new message to the front of the queue. It behaves
// identical to PushFront expect that it attaches a TTL and expiration callback
// to the message.
func (q *FlexQueue) PushFrontTTL(digest string, message interface{}, ttl time.Duration, callback func(digest string, message interface{})) bool {

	q.Lock()
	defer q.Unlock()

	return q.pushFBTTL(true, digest, message, ttl, callback)
}

// PushBackTTL will add a new message to the back of the queue. It behaves
// identical to PushBack expect that it attaches a TTL and expiration callback
// to the message.
func (q *FlexQueue) PushBackTTL(digest string, message interface{}, ttl time.Duration, callback func(digest string, message interface{})) bool {

	q.Lock()
	defer q.Unlock()

	return q.pushFBTTL(false, digest, message, ttl, callback)
}

// pushFBTTL will push a message into the queue like push, and also create
// a ttl table entry
func (q *FlexQueue) pushFBTTL(front bool, digest string, message interface{}, ttl time.Duration, callback func(digest string, message interface{})) bool {

	// Create the ttl control and abort now if the ttl is already expired
	ctrl := NewTTL(ttl, callback)
	if ctrl.Expired() {
		ctrl.Callback(digest, message)
		return false
	}

	// Pass through to the push operation
	if ok := q.pushFB(front, digest, message); ok {
		// If the push was successful then add the ctrl to the ttl table
		q.ttl[digest] = *ctrl
		return true
	}

	return false
}

// Pull will return the message with the given digest and remove it from the queue.
// Messages with an expired ttl are automatically removed.
// Returns:
// * interface{}: The message
// * bool: true if a message was found or false if not found or expired/removed
func (q *FlexQueue) Pull(digest string) (interface{}, bool) {

	q.Lock()
	defer q.Unlock()

	if q.pruneMessage(digest) {
		return nil, false
	}

	return q.messages.Pull(digest)
}

// PullFront will remove a message from the beginning of the queue and return a
// reference to it. Messages with an expired ttl are automatically removed.
// Returns:
// * string: The message digest
// * interface{}: The message
// * bool: true if a message was found or false if empty queue
func (q *FlexQueue) PullFront() (string, interface{}, bool) {

	q.Lock()
	defer q.Unlock()

	return q.pullFB(true)
}

// PullBack will remove a message from the end of the queue and return a
// reference to it. Messages with an expired ttl are automatically removed.
// Returns:
// * string: The message digest
// * interface{}: The message
// * bool: true if a message was found or false if empty queue
func (q *FlexQueue) PullBack() (string, interface{}, bool) {

	q.Lock()
	defer q.Unlock()

	return q.pullFB(false)
}

// pullFB is a recursive function that will continue to peel messages off
// the queue until it finds one that has not expired or the queue is empty
func (q *FlexQueue) pullFB(front bool) (string, interface{}, bool) {

	var (
		digest  string
		message interface{}
		ok      bool
	)

	if front {
		digest, message, ok = q.messages.PullFront()
	} else {
		digest, message, ok = q.messages.PullBack()
	}

	if !ok {
		return "", nil, false
	}

	if q.pruneMessage(digest) {
		return q.pullFB(front)
	}

	return digest, message, true
}

// Read will return the message with the given digest without removing it.
// Messages with an expired ttl are automatically removed.
// Returns:
// * interface{}: The message
// * bool: true if a message was found or false if not found or expired/removed
func (q *FlexQueue) Read(digest string) (interface{}, bool) {

	q.Lock()
	defer q.Unlock()

	if q.pruneMessage(digest) {
		return nil, false
	}

	return q.messages.Read(digest)
}

// ReadFront will return a message from the beginning of the queue without
// removing it. Messages with an expired ttl are automatically removed.
// Returns:
// * string: The message digest
// * interface{}: The message
// * bool: true if a message was found or false if empty queue
func (q *FlexQueue) ReadFront() (string, interface{}, bool) {

	q.Lock()
	defer q.Unlock()

	return q.readFB(true)
}

// ReadBack will return a message from the end of the queue without
// removing it. Messages with an expired ttl are automatically removed.
// Returns:
// * string: The message digest
// * interface{}: The message
// * bool: true if a message was found or false if empty queue
func (q *FlexQueue) ReadBack() (string, interface{}, bool) {

	q.Lock()
	defer q.Unlock()

	return q.readFB(false)
}

// readFB is a recursive function that will continue to readFB messages off
// the queue until it finds one that has not expired or the queue is empty
func (q *FlexQueue) readFB(front bool) (string, interface{}, bool) {

	var (
		digest  string
		message interface{}
		ok      bool
	)

	if front {
		digest, message, ok = q.messages.ReadFront()
	} else {
		digest, message, ok = q.messages.ReadBack()
	}

	if !ok {
		return "", nil, false
	}

	if q.pruneMessage(digest) {
		return q.readFB(front)
	}

	return digest, message, true
}

// Update will update a message already in the queue based on its digest
// without changing the order.
// Returns:
// * bool: true if the item was updated and false if not found
func (q *FlexQueue) Update(digest string, message interface{}) bool {

	q.Lock()
	defer q.Unlock()

	if q.pruneMessage(digest) {
		return false
	}

	return q.messages.Update(digest, message)
}

// ResetTTL will update the TTL for a message already in the queue with a new duration.
// The callback for the existing TTL will be kept in place.
// Returns:
// * bool: true if the item was updated and false if message not found or TTL not found on message
func (q *FlexQueue) ResetTTL(digest string, ttl time.Duration) bool {

	q.Lock()
	defer q.Unlock()

	// Do not allow reset if the ttl for the targetted message is already expired
	if q.pruneMessage(digest) {
		return false
	}

	// Grab the current ttl control for the message, if it has one
	oldCtrl, ok := q.ttl[digest]
	if !ok {
		return false
	}

	// Create the new ttl control using the old ttl callback,
	// and abort now if the new ttl is already expired
	msg, _ := q.messages.Read(digest)
	ctrl := NewTTL(ttl, oldCtrl.Callback)
	if ctrl.Expired() {
		ctrl.Callback(digest, msg)
		return false
	}

	// Replace the current ttl ctrl with the new one
	q.ttl[digest] = *ctrl

	return true
}

// Remove will delete the message from the queue. Returns true if the
// message was found and deleted or false if not found.
func (q *FlexQueue) Remove(digest string) bool {

	q.Lock()
	defer q.Unlock()

	if q.pruneMessage(digest) {
		return false
	}

	if q.messages.Remove(digest) {
		delete(q.ttl, digest)
		return true
	}

	return false
}

// Prune will scan all messages and remove any with an expired ttl. This
// function is meant to be used on an interval by the caller in the case that
// the automatic removal of expired messages by Pull, Read, or Has methods is
// insufficient. Returns true if any expired messages were found and removed.
func (q *FlexQueue) Prune() bool {

	q.Lock()
	defer q.Unlock()

	removed := false

	for digest, ttl := range q.ttl {
		if ttl.Expired() {
			msg, _ := q.messages.Read(digest)
			ttl.Callback(digest, msg)
			_ = q.messages.Remove(digest)
			delete(q.ttl, digest)
			removed = true
		}
	}

	return removed
}

// pruneMessage will test for the message ttl and if it exists and is expired then
// the ttl callback will be fired and the message will be removed from the queue.
// Returns true if the message message was expired, otherwise false.
func (q *FlexQueue) pruneMessage(digest string) bool {

	ttl, ok := q.ttl[digest]
	if ok && ttl.Expired() {
		msg, _ := q.messages.Read(digest)
		ttl.Callback(digest, msg)
		_ = q.messages.Remove(digest)
		delete(q.ttl, digest)
		return true
	}

	return false
}

// Has returns true if the message with the given digest is in the queue.
// Expired messages will be removed and this will return false.
func (q *FlexQueue) Has(digest string) bool {

	q.Lock()
	defer q.Unlock()

	if q.pruneMessage(digest) {
		return false
	}

	return q.messages.Has(digest)
}

// Len returns the number of messages currently in the queue
func (q *FlexQueue) Len() int {

	q.RLock()
	defer q.RUnlock()

	return q.messages.Len()
}

// Max returns the maximum number of messages the queue can hold. If there
// is no message limit then this will return -1.
func (q *FlexQueue) Max() int {
	if q.max > NoMax {
		return q.max
	}
	return NoMax
}

// IsFull returns true if the queue is full and false if its not
func (q *FlexQueue) IsFull() bool {

	q.RLock()
	defer q.RUnlock()

	return q.max > NoMax && q.messages.Len() >= q.max
}

// IsEmpty returns true if the queue is empty and false if its not
func (q *FlexQueue) IsEmpty() bool {

	q.RLock()
	defer q.RUnlock()

	return q.messages.Len() == 0
}
