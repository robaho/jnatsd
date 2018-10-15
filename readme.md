**About**

Java port of nats-io server gnatsd. See [nats-io gnatsd](github.com/nats-io/gnatsd)

The original is written is Go.

**ToDo**

Need to support "router mode".

Add TLS connections.

Need to implement server and client options.

Use binary search for matching subscriptions.

Purge subscription cache based on "old than X" method.

Port many of the test cases from gnatsd to jnatsd.