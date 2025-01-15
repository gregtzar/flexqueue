package flexqueue_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/gregtzar/flexqueue"
)

type Message struct {
	Digest string
	TTL    time.Duration
}

func TestFlexQueueFIFO(t *testing.T) {
	type tcase struct {
		Messages []Message
		Reverse  bool
	}

	fn := func(tc tcase) func(t *testing.T) {
		return func(t *testing.T) {

			queue := flexqueue.NewFlexQueue()

			// perform and validate the pushes
			for i := range tc.Messages {
				if tc.Reverse {
					if ok := queue.PushFront(tc.Messages[i].Digest, &tc.Messages[i]); !ok {
						t.Errorf("expected push to be ok but got not ok")
					}
				} else {
					if ok := queue.PushBack(tc.Messages[i].Digest, &tc.Messages[i]); !ok {
						t.Errorf("expected push to be ok but got not ok")
					}
				}
				if !queue.Has(tc.Messages[i].Digest) {
					t.Errorf("expected message %v to exist", tc.Messages[i].Digest)
				}
			}

			// verify queue len
			if queue.Len() != len(tc.Messages) {
				t.Errorf("expected list len to be %v but got %v instead", len(tc.Messages), queue.Len())
			}

			// perform and validate the pulls
			for i := range tc.Messages {
				var (
					digest  string
					message interface{}
					ok      bool
				)
				if tc.Reverse {
					if digest, message, ok = queue.PullBack(); !ok {
						t.Errorf("expected pull to be ok but got not ok")
					}
				} else {
					if digest, message, ok = queue.PullFront(); !ok {
						t.Errorf("expected pull to be ok but got not ok")
					}
				}
				if digest != tc.Messages[i].Digest {
					t.Errorf("expected extracted digest to be %v but got %v instead", tc.Messages[i].Digest, digest)
				}
				if message.(*Message).Digest != tc.Messages[i].Digest {
					t.Errorf("expected extracted message to have digest %v but got %v instead", tc.Messages[i].Digest, message.(*Message).Digest)
				}
				if queue.Has(tc.Messages[i].Digest) {
					t.Errorf("expected message %v to not exist", tc.Messages[i].Digest)
				}
			}

			// verify queue len
			if queue.Len() != 0 {
				t.Errorf("expected list len to be %v but got %v instead", 0, queue.Len())
			}

			// pull from the empty queue
			_, _, ok := queue.PullFront()
			if ok {
				t.Errorf("expected pull from empty queue to be not ok but got ok")
			}
		}
	}

	tcases := map[string]tcase{
		"single": {
			Messages: []Message{
				Message{
					Digest: "A",
				},
			},
		},
		"multiple": {
			Messages: []Message{
				Message{
					Digest: "A",
				},
				Message{
					Digest: "B",
				},
				Message{
					Digest: "C",
				},
			},
		},
	}

	for k, v := range tcases {
		v.Reverse = false
		t.Run(k, fn(v))
		v.Reverse = true
		t.Run(fmt.Sprintf("%v reverse", k), fn(v))
	}
}

func TestFlexQueueLIFO(t *testing.T) {
	type tcase struct {
		Messages []Message
		Reverse  bool
	}

	fn := func(tc tcase) func(t *testing.T) {
		return func(t *testing.T) {

			queue := flexqueue.NewFlexQueue()

			// perform and validate the pushes
			for i := range tc.Messages {
				if tc.Reverse {
					if ok := queue.PushBack(tc.Messages[i].Digest, &tc.Messages[i]); !ok {
						t.Errorf("expected push to be ok but got not ok")
					}
				} else {
					if ok := queue.PushFront(tc.Messages[i].Digest, &tc.Messages[i]); !ok {
						t.Errorf("expected push to be ok but got not ok")
					}
				}
				if !queue.Has(tc.Messages[i].Digest) {
					t.Errorf("expected message %v to exist", tc.Messages[i].Digest)
				}
			}

			// verify queue len
			if queue.Len() != len(tc.Messages) {
				t.Errorf("expected list len to be %v but got %v instead", len(tc.Messages), queue.Len())
			}

			// perform and validate the pulls (reverse loop)
			for i := len(tc.Messages) - 1; i >= 0; i-- {
				var (
					digest  string
					message interface{}
					ok      bool
				)
				if tc.Reverse {
					if digest, message, ok = queue.PullBack(); !ok {
						t.Errorf("expected pull to be ok but got not ok")
					}
				} else {
					if digest, message, ok = queue.PullFront(); !ok {
						t.Errorf("expected pull to be ok but got not ok")
					}
				}
				if digest != tc.Messages[i].Digest {
					t.Errorf("expected extracted digest to be %v but got %v instead", tc.Messages[i].Digest, digest)
				}
				if message.(*Message).Digest != tc.Messages[i].Digest {
					t.Errorf("expected extracted message to have digest %v but got %v instead", tc.Messages[i].Digest, message.(*Message).Digest)
				}
				if queue.Has(tc.Messages[i].Digest) {
					t.Errorf("expected message %v to not exist", tc.Messages[i].Digest)
				}
			}

			// verify queue len
			if queue.Len() != 0 {
				t.Errorf("expected list len to be %v but got %v instead", 0, queue.Len())
			}

			// pull from the empty queue
			_, _, ok := queue.PullFront()
			if ok {
				t.Errorf("expected pull from empty queue to be not ok but got ok")
			}
		}
	}

	tcases := map[string]tcase{
		"single": {
			Messages: []Message{
				Message{
					Digest: "A",
				},
			},
		},
		"multiple": {
			Messages: []Message{
				Message{
					Digest: "A",
				},
				Message{
					Digest: "B",
				},
				Message{
					Digest: "C",
				},
			},
		},
	}

	for k, v := range tcases {
		v.Reverse = false
		t.Run(k, fn(v))
		v.Reverse = true
		t.Run(fmt.Sprintf("%v reverse", k), fn(v))
	}
}

func TestFlexQueueTTLPrune(t *testing.T) {

	type tcase struct {
		Messages    []Message
		WaitTime    time.Duration
		ExpectedLen int
	}

	fn := func(tc tcase) func(t *testing.T) {
		return func(t *testing.T) {

			queue := flexqueue.NewFlexQueue()

			cbCount := 0
			cbFunc := func(digest string, message interface{}) {
				cbCount++
			}

			// perform the pushes
			for i := range tc.Messages {
				if ok := queue.PushBackTTL(tc.Messages[i].Digest, &tc.Messages[i], tc.Messages[i].TTL, cbFunc); !ok {
					t.Errorf("expected push to be ok but got not ok")
				}
			}

			// wait for a while
			time.Sleep(tc.WaitTime)

			// prune the queue to remove expired messages
			expectExpired := (tc.ExpectedLen != len(tc.Messages))
			if ok := queue.Prune(); ok != expectExpired {
				t.Errorf("expected sweep to return %v but got %v", expectExpired, ok)
			}
			// verify queue len
			if queue.Len() != tc.ExpectedLen {
				t.Errorf("expected queue count to be %v but got %v", tc.ExpectedLen, queue.Len())
			}
			// verify callback executions
			if cbCount != len(tc.Messages)-tc.ExpectedLen {
				t.Errorf("expected callback count to be %v but got %v", len(tc.Messages)-tc.ExpectedLen, cbCount)
			}
		}
	}

	tcases := map[string]tcase{
		"expired none": {
			Messages: []Message{
				Message{
					Digest: "A",
					TTL:    time.Millisecond * 20,
				},
			},
			WaitTime:    time.Millisecond * 10,
			ExpectedLen: 1,
		},
		"expired all": {
			Messages: []Message{
				Message{
					Digest: "A",
					TTL:    time.Millisecond * 10,
				},
			},
			WaitTime:    time.Millisecond * 20,
			ExpectedLen: 0,
		},
		"expired some": {
			Messages: []Message{
				Message{
					Digest: "A",
					TTL:    time.Millisecond * 30,
				},
				Message{
					Digest: "B",
					TTL:    time.Millisecond * 10,
				},
				Message{
					Digest: "C",
					TTL:    time.Millisecond * 30,
				},
			},
			WaitTime:    time.Millisecond * 20,
			ExpectedLen: 2,
		},
	}

	for k, v := range tcases {
		t.Run(k, fn(v))
	}
}

func TestFlexQueueTTLPullFB(t *testing.T) {

	type tcase struct {
		Messages    []Message
		WaitTime    time.Duration
		ExpectedLen int
		Reverse     bool
	}

	fn := func(tc tcase) func(t *testing.T) {
		return func(t *testing.T) {

			queue := flexqueue.NewFlexQueue()

			cbCount := 0
			cbFunc := func(digest string, message interface{}) {
				cbCount++
			}

			// perform the pushes
			for i := range tc.Messages {
				if ok := queue.PushFrontTTL(tc.Messages[i].Digest, &tc.Messages[i], tc.Messages[i].TTL, cbFunc); !ok {
					t.Errorf("expected push to be ok but got not ok")
				}
			}

			// wait for a while
			time.Sleep(tc.WaitTime)

			// pull once for every message in the queue to remove expired messages
			successCount := 0
			for range tc.Messages {
				if tc.Reverse {
					if _, _, ok := queue.PullBack(); ok {
						successCount++
					}
				} else {
					if _, _, ok := queue.PullFront(); ok {
						successCount++
					}
				}
			}

			// verify the count of active messages
			if successCount != tc.ExpectedLen {
				t.Errorf("expected success count to be %v but got %v", tc.ExpectedLen, successCount)
			}

			// verify callback executions
			if cbCount != len(tc.Messages)-tc.ExpectedLen {
				t.Errorf("expected callback count to be %v but got %v", len(tc.Messages)-tc.ExpectedLen, cbCount)
			}
		}
	}

	tcases := map[string]tcase{
		"expired none": {
			Messages: []Message{
				Message{
					Digest: "A",
					TTL:    time.Millisecond * 20,
				},
			},
			WaitTime:    time.Millisecond * 10,
			ExpectedLen: 1,
		},
		"expired all": {
			Messages: []Message{
				Message{
					Digest: "A",
					TTL:    time.Millisecond * 10,
				},
			},
			WaitTime:    time.Millisecond * 20,
			ExpectedLen: 0,
		},
		"expired some": {
			Messages: []Message{
				Message{
					Digest: "A",
					TTL:    time.Millisecond * 30,
				},
				Message{
					Digest: "B",
					TTL:    time.Millisecond * 10,
				},
				Message{
					Digest: "C",
					TTL:    time.Millisecond * 30,
				},
			},
			WaitTime:    time.Millisecond * 20,
			ExpectedLen: 2,
		},
	}

	for k, v := range tcases {
		v.Reverse = false
		t.Run(k, fn(v))
		v.Reverse = true
		t.Run(fmt.Sprintf("%v reverse", k), fn(v))
	}
}

func TestFlexQueueTTLReadFB(t *testing.T) {

	type tcase struct {
		Messages          []Message
		WaitTime          time.Duration
		ExpectReadSuccess bool
		ExpectedLen       int
		Reverse           bool
	}

	fn := func(tc tcase) func(t *testing.T) {
		return func(t *testing.T) {

			queue := flexqueue.NewFlexQueue()

			cbCount := 0
			cbFunc := func(digest string, message interface{}) {
				cbCount++
			}

			// perform the pushes
			for i := range tc.Messages {
				if ok := queue.PushFrontTTL(tc.Messages[i].Digest, &tc.Messages[i], tc.Messages[i].TTL, cbFunc); !ok {
					t.Errorf("expected push to be ok but got not ok")
				}
			}

			// wait for a while
			time.Sleep(tc.WaitTime)

			// read from the queue one time only
			if tc.Reverse {
				if _, _, ok := queue.ReadBack(); ok != tc.ExpectReadSuccess {
					t.Errorf("expected read success %v but got %v", tc.ExpectReadSuccess, ok)
				}
			} else {
				if _, _, ok := queue.ReadFront(); ok != tc.ExpectReadSuccess {
					t.Errorf("expected read success %v but got %v", tc.ExpectReadSuccess, ok)
				}
			}

			// verify queue len
			if queue.Len() != tc.ExpectedLen {
				t.Errorf("expected queue count to be %v but got %v", tc.ExpectedLen, queue.Len())
			}

			// verify callback executions
			if cbCount != len(tc.Messages)-tc.ExpectedLen {
				t.Errorf("expected callback count to be %v but got %v", len(tc.Messages)-tc.ExpectedLen, cbCount)
			}
		}
	}

	tcases := map[string]tcase{
		"expired none": {
			Messages: []Message{
				Message{
					Digest: "A",
					TTL:    time.Millisecond * 20,
				},
				Message{
					Digest: "B",
					TTL:    time.Millisecond * 20,
				},
			},
			WaitTime:          time.Millisecond * 10,
			ExpectReadSuccess: true,
			ExpectedLen:       2,
		},
		"expired all": {
			Messages: []Message{
				Message{
					Digest: "A",
					TTL:    time.Millisecond * 10,
				},
				Message{
					Digest: "B",
					TTL:    time.Millisecond * 10,
				},
			},
			WaitTime:          time.Millisecond * 20,
			ExpectReadSuccess: false,
			ExpectedLen:       0,
		},
		"expired some": {
			Messages: []Message{
				Message{
					Digest: "A",
					TTL:    time.Millisecond * 10,
				},
				Message{
					Digest: "B",
					TTL:    time.Millisecond * 10,
				},
				Message{
					Digest: "C",
					TTL:    time.Millisecond * 30,
				},
				Message{
					Digest: "D",
					TTL:    time.Millisecond * 10,
				},
				Message{
					Digest: "E",
					TTL:    time.Millisecond * 10,
				},
			},
			WaitTime:          time.Millisecond * 20,
			ExpectReadSuccess: true,
			ExpectedLen:       3,
		},
	}

	for k, v := range tcases {
		v.Reverse = false
		t.Run(k, fn(v))
		v.Reverse = true
		t.Run(fmt.Sprintf("%v reverse", k), fn(v))
	}
}

func TestFlexQueueTTLPull(t *testing.T) {

	type tcase struct {
		Messages    []Message
		WaitTime    time.Duration
		ExpectedLen int
	}

	fn := func(tc tcase) func(t *testing.T) {
		return func(t *testing.T) {

			queue := flexqueue.NewFlexQueue()

			cbCount := 0
			cbFunc := func(digest string, message interface{}) {
				cbCount++
			}

			// perform the pushes
			for i := range tc.Messages {
				if ok := queue.PushFrontTTL(tc.Messages[i].Digest, &tc.Messages[i], tc.Messages[i].TTL, cbFunc); !ok {
					t.Errorf("expected push to be ok but got not ok")
				}
			}

			// wait for a while
			time.Sleep(tc.WaitTime)

			// read once for every message in the queue to remove expired messages
			successCount := 0
			for i := range tc.Messages {
				if _, ok := queue.Pull(tc.Messages[i].Digest); ok {
					successCount++
				}
			}

			// verify the count of active messages
			if successCount != tc.ExpectedLen {
				t.Errorf("expected success count to be %v but got %v", tc.ExpectedLen, successCount)
			}

			// verify callback executions
			if cbCount != len(tc.Messages)-tc.ExpectedLen {
				t.Errorf("expected callback count to be %v but got %v", len(tc.Messages)-tc.ExpectedLen, cbCount)
			}
		}
	}

	tcases := map[string]tcase{
		"expired none": {
			Messages: []Message{
				Message{
					Digest: "A",
					TTL:    time.Millisecond * 20,
				},
			},
			WaitTime:    time.Millisecond * 10,
			ExpectedLen: 1,
		},
		"expired all": {
			Messages: []Message{
				Message{
					Digest: "A",
					TTL:    time.Millisecond * 10,
				},
			},
			WaitTime:    time.Millisecond * 20,
			ExpectedLen: 0,
		},
		"expired some": {
			Messages: []Message{
				Message{
					Digest: "A",
					TTL:    time.Millisecond * 30,
				},
				Message{
					Digest: "B",
					TTL:    time.Millisecond * 10,
				},
				Message{
					Digest: "C",
					TTL:    time.Millisecond * 30,
				},
			},
			WaitTime:    time.Millisecond * 20,
			ExpectedLen: 2,
		},
	}

	for k, v := range tcases {
		t.Run(k, fn(v))
	}
}

func TestFlexQueueTTLReadUpdateHas(t *testing.T) {

	const (
		METHOD_READ   = "read"
		METHOD_UPDATE = "update"
		METHOD_HAS    = "has"
	)

	type tcase struct {
		Messages    []Message
		WaitTime    time.Duration
		ExpectedLen int
		Method      string
	}

	fn := func(tc tcase) func(t *testing.T) {
		return func(t *testing.T) {

			queue := flexqueue.NewFlexQueue()

			cbCount := 0
			cbFunc := func(digest string, message interface{}) {
				cbCount++
			}

			// perform the pushes
			for i := range tc.Messages {
				if ok := queue.PushFrontTTL(tc.Messages[i].Digest, &tc.Messages[i], tc.Messages[i].TTL, cbFunc); !ok {
					t.Errorf("expected push to be ok but got not ok")
				}
			}

			// wait for a while
			time.Sleep(tc.WaitTime)

			// has once for every message in the queue to remove expired messages
			successCount := 0
			for i := range tc.Messages {
				switch tc.Method {
				case METHOD_READ:
					if _, ok := queue.Read(tc.Messages[i].Digest); ok {
						successCount++
					}
				case METHOD_UPDATE:
					if ok := queue.Update(tc.Messages[i].Digest, &tc.Messages[i]); ok {
						successCount++
					}
				case METHOD_HAS:
					if ok := queue.Has(tc.Messages[i].Digest); ok {
						successCount++
					}
				default:
					t.Errorf("invalid method: %v", tc.Method)
				}
			}

			// verify the count of active messages
			if successCount != tc.ExpectedLen {
				t.Errorf("expected success count to be %v but got %v", tc.ExpectedLen, successCount)
			}

			// verify queue len
			if queue.Len() != tc.ExpectedLen {
				t.Errorf("expected queue count to be %v but got %v", tc.ExpectedLen, queue.Len())
			}

			// verify callback executions
			if cbCount != len(tc.Messages)-tc.ExpectedLen {
				t.Errorf("expected callback count to be %v but got %v", len(tc.Messages)-tc.ExpectedLen, cbCount)
			}
		}
	}

	tcases := map[string]tcase{
		"expired none": {
			Messages: []Message{
				Message{
					Digest: "A",
					TTL:    time.Millisecond * 20,
				},
			},
			WaitTime:    time.Millisecond * 10,
			ExpectedLen: 1,
		},
		"expired all": {
			Messages: []Message{
				Message{
					Digest: "A",
					TTL:    time.Millisecond * 10,
				},
			},
			WaitTime:    time.Millisecond * 20,
			ExpectedLen: 0,
		},
		"expired some": {
			Messages: []Message{
				Message{
					Digest: "A",
					TTL:    time.Millisecond * 30,
				},
				Message{
					Digest: "B",
					TTL:    time.Millisecond * 10,
				},
				Message{
					Digest: "C",
					TTL:    time.Millisecond * 30,
				},
			},
			WaitTime:    time.Millisecond * 20,
			ExpectedLen: 2,
		},
	}

	for k, v := range tcases {
		v.Method = METHOD_READ
		t.Run(fmt.Sprintf("%v %v", k, v.Method), fn(v))
		v.Method = METHOD_UPDATE
		t.Run(fmt.Sprintf("%v %v", k, v.Method), fn(v))
		v.Method = METHOD_HAS
		t.Run(fmt.Sprintf("%v %v", k, v.Method), fn(v))
	}
}

func TestFlexQueueResetTTL(t *testing.T) {

	type tcase struct {
		Message            Message
		WaitTime           time.Duration
		ExpectResetSuccess bool
		ExpectReadSuccess  bool
	}

	fn := func(tc tcase) func(t *testing.T) {
		return func(t *testing.T) {

			queue := flexqueue.NewFlexQueue()

			cbFunc := func(digest string, message interface{}) {}

			// push message to queue
			if ok := queue.PushFrontTTL(tc.Message.Digest, &tc.Message, tc.Message.TTL, cbFunc); !ok {
				t.Errorf("expected push to be ok but got not ok")
			}

			// wait for a while
			time.Sleep(tc.WaitTime)

			ok := queue.ResetTTL(tc.Message.Digest, tc.Message.TTL)
			if (tc.ExpectResetSuccess && !ok) || (!tc.ExpectResetSuccess && ok) {
				t.Errorf("expected reset success %v but got %v", tc.ExpectResetSuccess, ok)
			}

			// wait again
			time.Sleep(tc.WaitTime)

			_, _, ok = queue.ReadFront()
			if (tc.ExpectReadSuccess && !ok) || (!tc.ExpectReadSuccess && ok) {
				t.Errorf("expected read success %v but got %v", tc.ExpectReadSuccess, ok)
			}
		}
	}

	tcases := map[string]tcase{
		"failed reset due to expiration": {
			Message: Message{
				Digest: "A",
				TTL:    time.Millisecond * 20,
			},
			WaitTime:           time.Millisecond * 25,
			ExpectResetSuccess: false,
			ExpectReadSuccess:  false,
		},
		"successful reset and wait": {
			Message: Message{
				Digest: "A",
				TTL:    time.Millisecond * 20,
			},
			WaitTime:           time.Millisecond * 15,
			ExpectResetSuccess: true,
			ExpectReadSuccess:  true,
		},
	}

	for k, v := range tcases {
		t.Run(k, fn(v))
	}
}

func TestFlexQueueDigestMatching(t *testing.T) {

	msgFoo := Message{
		Digest: "foo",
	}

	msgBar := Message{
		Digest: "bar",
	}

	msgFooBar := Message{
		Digest: "foo_bar",
	}

	queue := flexqueue.NewFlexQueue()

	// perform the foo insertion
	if ok := queue.PushFront(msgFoo.Digest, msgFoo); !ok {
		t.Errorf("expected successful push but got failed")
	}

	// verify queue len
	if queue.Len() != 1 {
		t.Errorf("expected queue len to be %v but got %v instead", 1, queue.Len())
	}

	// perform the bar insertion
	if ok := queue.PushFront(msgBar.Digest, msgBar); !ok {
		t.Errorf("expected successful push but got failed")
	}

	// verify queue len
	if queue.Len() != 2 {
		t.Errorf("expected queue len to be %v but got %v instead", 2, queue.Len())
	}

	// perform the foo_bar insertion
	if ok := queue.PushFront(msgFooBar.Digest, msgFooBar); !ok {
		t.Errorf("expected successful push but got failed")
	}

	// verify queue len
	if queue.Len() != 3 {
		t.Errorf("expected queue len to be %v but got %v instead", 2, queue.Len())
	}

	// perform a foo_bar read
	fb, ok := queue.Read(msgFooBar.Digest)
	if !ok {
		t.Errorf("expected successful read but failed")
	}

	fbMsg, ok := fb.(Message)
	if !ok {
		t.Errorf("failed to type cast read message")
	}

	if fbMsg.Digest != msgFooBar.Digest {
		t.Errorf("expected digest to be %v but got %v instead", msgFooBar.Digest, fbMsg.Digest)
	}
}

func TestFlexQueueTTLCallback(t *testing.T) {

	messages := []Message{
		Message{
			Digest: "A",
			TTL:    time.Millisecond * 10,
		},
		Message{
			Digest: "B",
			TTL:    time.Millisecond * 10,
		},
	}

	queue := flexqueue.NewFlexQueue()

	cbDigests := []string{}
	cbMessages := []Message{}

	cbFunc := func(digest string, message interface{}) {
		cbDigests = append(cbDigests, digest)
		cbMessages = append(cbMessages, *message.(*Message))
	}

	// perform the pushes
	for i := range messages {
		if ok := queue.PushBackTTL(messages[i].Digest, &messages[i], messages[i].TTL, cbFunc); !ok {
			t.Errorf("expected push to be ok but got not ok")
		}
	}

	// wait for a while
	time.Sleep(time.Millisecond * 20)

	// perform the Has checks to force prune each one in expected order
	for i := range messages {
		if ok := queue.Has(messages[i].Digest); ok {
			t.Errorf("expected has to be false but got true")
		}
	}

	if queue.Len() != 0 {
		t.Errorf("expected queue len to be %v but got %v instead", 0, queue.Len())
	}

	// analyze the callback records
	for i := range messages {
		if cbDigests[i] != messages[i].Digest {
			t.Errorf("expected callback digest record to exist for message digest %v", messages[i].Digest)
		}
		if cbMessages[i].Digest != messages[i].Digest {
			t.Errorf("expected callback message record to exist for message digest %v", messages[i].Digest)
		}
	}
}

func TestFlexQueueDedupe(t *testing.T) {

	type tcase struct {
		Messages    []Message
		DupeMessage Message
		MaxLen      int
		Reverse     bool
	}

	fn := func(tc tcase) func(t *testing.T) {
		return func(t *testing.T) {

			queue := flexqueue.NewFlexQueue().SetMax(tc.MaxLen)

			// perform the pushes
			for i := range tc.Messages {
				if tc.Reverse {
					if ok := queue.PushFront(tc.Messages[i].Digest, &tc.Messages[i]); !ok {
						t.Errorf("expected push to be ok but got not ok")
					}
				} else {
					if ok := queue.PushBack(tc.Messages[i].Digest, &tc.Messages[i]); !ok {
						t.Errorf("expected push to be ok but got not ok")
					}
				}
			}

			// verify message count
			if queue.Len() != len(tc.Messages) {
				t.Errorf("expected queue count to be %v but got %v", len(tc.Messages), queue.Len())
			}

			// push the duplicate message
			if tc.Reverse {
				if ok := queue.PushFront(tc.DupeMessage.Digest, &tc.DupeMessage); !ok {
					t.Errorf("expected push success to be true but got %v", ok)
				}
			} else {
				if ok := queue.PushBack(tc.DupeMessage.Digest, &tc.DupeMessage); !ok {
					t.Errorf("expected push success to be true but got %v", ok)
				}
			}

			// verify message count is unchanged
			if queue.Len() != len(tc.Messages) {
				t.Errorf("expected queue count to be %v but got %v", len(tc.Messages), queue.Len())
			}
		}
	}

	tcases := map[string]tcase{
		"single dedupe open queue": {
			Messages: []Message{
				Message{
					Digest: "A",
				},
			},
			DupeMessage: Message{
				Digest: "A",
			},
			MaxLen: flexqueue.NoMax,
		},
		"single dedupe closed queue": {
			Messages: []Message{
				Message{
					Digest: "A",
				},
			},
			DupeMessage: Message{
				Digest: "A",
			},
			MaxLen: 1,
		},
		"multiple dedupe open queue": {
			Messages: []Message{
				Message{
					Digest: "A",
				},
				Message{
					Digest: "B",
				},
				Message{
					Digest: "C",
				},
			},
			DupeMessage: Message{
				Digest: "B",
			},
			MaxLen: flexqueue.NoMax,
		},
		"multiple dedupe closed queue": {
			Messages: []Message{
				Message{
					Digest: "A",
				},
				Message{
					Digest: "B",
				},
				Message{
					Digest: "C",
				},
			},
			DupeMessage: Message{
				Digest: "B",
			},
			MaxLen: 3,
		},
	}

	for k, v := range tcases {
		v.Reverse = false
		t.Run(k, fn(v))
		v.Reverse = true
		t.Run(fmt.Sprintf("%v reverse", k), fn(v))
	}
}

func TestFlexQueueRemove(t *testing.T) {

	type tcase struct {
		Messages            []Message
		RemoveMessage       Message
		ExpectRemoveSuccess bool
		ExpectedLen         int
	}

	fn := func(tc tcase) func(t *testing.T) {
		return func(t *testing.T) {

			queue := flexqueue.NewFlexQueue()

			// perform the pushes
			for i := range tc.Messages {
				if ok := queue.PushFront(tc.Messages[i].Digest, &tc.Messages[i]); !ok {
					t.Errorf("expected push to be ok but got not ok")
				}
			}

			// perform the remove
			if ok := queue.Remove(tc.RemoveMessage.Digest); ok != tc.ExpectRemoveSuccess {
				t.Errorf("expected remove success to be %v but got %v", tc.ExpectRemoveSuccess, ok)
			}

			// verify queue len
			if queue.Len() != tc.ExpectedLen {
				t.Errorf("expected queue len to be %v but got %v", tc.ExpectedLen, queue.Len())
			}
		}
	}

	tcases := map[string]tcase{
		"single remove success": {
			Messages: []Message{
				Message{
					Digest: "A",
				},
			},
			RemoveMessage: Message{
				Digest: "A",
			},
			ExpectRemoveSuccess: true,
			ExpectedLen:         0,
		},
		"multi remove success": {
			Messages: []Message{
				Message{
					Digest: "A",
				},
				Message{
					Digest: "B",
				},
				Message{
					Digest: "C",
				},
			},
			RemoveMessage: Message{
				Digest: "B",
			},
			ExpectRemoveSuccess: true,
			ExpectedLen:         2,
		},
		"single remove fail": {
			Messages: []Message{
				Message{
					Digest: "A",
				},
			},
			RemoveMessage: Message{
				Digest: "B",
			},
			ExpectRemoveSuccess: false,
			ExpectedLen:         1,
		},
	}

	for k, v := range tcases {
		t.Run(k, fn(v))
	}
}

func TestFlexQueueMaxLenEmptyFull(t *testing.T) {

	messages := []Message{
		Message{
			Digest: "A",
		},
		Message{
			Digest: "B",
		},
	}

	queue := flexqueue.NewFlexQueue().SetMax(2)

	if queue.Max() != 2 {
		t.Errorf("expected queue max to be %v but got %v instead", 2, queue.Max())
	}

	if queue.Len() != 0 {
		t.Errorf("expected queue len to be %v but got %v instead", 0, queue.Len())
	}

	if !queue.IsEmpty() {
		t.Errorf("expected queue empty to be %v but got %v instead", true, queue.IsEmpty())
	}

	if queue.IsFull() {
		t.Errorf("expected queue full to be %v but got %v instead", false, queue.IsFull())
	}

	if ok := queue.PushBack(messages[0].Digest, &messages[0]); !ok {
		t.Errorf("expected successful push but got failed")
	}

	if queue.Len() != 1 {
		t.Errorf("expected queue len to be %v but got %v instead", 1, queue.Len())
	}

	if queue.IsEmpty() {
		t.Errorf("expected queue empty to be %v but got %v instead", false, queue.IsEmpty())
	}

	if queue.IsFull() {
		t.Errorf("expected queue full to be %v but got %v instead", false, queue.IsFull())
	}

	if ok := queue.PushBack(messages[1].Digest, &messages[1]); !ok {
		t.Errorf("expected successful push but got failed")
	}

	if queue.Len() != 2 {
		t.Errorf("expected queue len to be %v but got %v instead", 2, queue.Len())
	}

	if queue.IsEmpty() {
		t.Errorf("expected queue empty to be %v but got %v instead", false, queue.IsEmpty())
	}

	if !queue.IsFull() {
		t.Errorf("expected queue full to be %v but got %v instead", true, queue.IsFull())
	}
}
