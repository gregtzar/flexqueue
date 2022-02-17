# flexqueue

The `flexqueue` package provides some hybrid data structures which are intended to cover the most common use cases for highly performant single lane message queues and ordered lists. No single use case is likely to use all of the provided features or methods, but integral to the design is that no arbitrary limitations were imposed.

# Documentation

* [Go Package API Docs](https://pkg.go.dev/github.com/gregtzar/flexqueue)

# Design Considerations

No external dependencies, this package only uses internal go libraries.

Care was taken to implement all methods with a constant 0(1) time signature. This means that performance will be effected linearly as the collection size grows, no exponential degradation.

Care was taken to be as non-intrusive and non-prescriptive as possible. These data structures therefore do not spawn additional go routines or dictate any item interface. All features are optional and no major performance penalty is incurred by opting out of a feature.

Because these are hybrid data structures which blend several different concepts, I've maintained a coherent and consistent naming convention across everything. Depending on your existing bias of naming conventions, you may be required to adapt the language you are used to. 

# FlexList

`FlexList` is a high performance ordered map. Internally it uses a combination of a double linked list via `container/list` for item order and a map of strings for an index.

# FlexQueue

`FlexQueue` is a combined FIFO/LIFO single lane queue. `FlexQueue` is essentially a wrapper for `FlexList` but provides the following additional queue concepts:

* Thread safety via mutex
* Max queue length
* Message de-duplication
* Message TTL/expiration with callback

## FIFI/LIFO

* For a *FIFO* (first-in first-out) queue use `PushBack` for insertions and `PullFront` for extractions.
* For a *LIFO* (last-in first-out) queue use `PushFront` for insertions and `PullFront` for extractions.
* To leave a message in a *FIFO* queue while it is being processed use `ReadFront` and `Remove` rather than `PullFront`.

## De-Duplication

* To utilize message de-duplication provide a `digest` value based on a hash of message contents. You implement the digest algorithm.
* To avoid message de-duplication provide a unique `digest` value for every message.

## TTL

* TTL is optional, and the configuration is handled on each message insertion with a `time.Duration` and a callback function.
* A queue may contain a mix of messages with and without a TTL.
* TTL uses `time.Duration` to guarantee the expiration accuracy regardless of server time zone settings.
* If you messages use expiration dates then you should map them to a `time.Duration` at the time of insertion.
* All read/write functions which access a message in the queue will transparently perform a TTL analysis and if the message is expired it will be automatically removed from the queue and the access method will behave as if the message had not existed. The only exceptions to this are the `Len`, `Empty` and `Full` methods which do not perform TTL analysis and can therefore count expired messages. We did this to keep these counting methods performant. If you want to take the performance hit for better accuracy then call `Prune` first.