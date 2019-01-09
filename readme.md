**About**

Java port of nats-io server gnatsd. See [nats-io gnatsd](github.com/nats-io/gnatsd)

The original is written is Go.

Use tag 1.0, master is flux with move to async IO - SSL channels are not supported. The code has gone through several iterations in order to increase performance. The latest uses asynchronous IO for better CPU utilitization with multiple clients, but performing SSL with async channels is not trivial.

**Recent Changes**

TLS connections are now fully supported.

Most simple options, like 'verbose' work. 

**ToDo**

Need to support "router mode".

Need to support authorization using client certificates.

Use binary search for matching subscriptions.

Purge subscription cache based on "old than X" method.

Port many of the test cases from gnatsd to jnatsd.
