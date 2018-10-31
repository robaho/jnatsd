**About**

Java port of nats-io server gnatsd. See [nats-io gnatsd](github.com/nats-io/gnatsd)

The original is written is Go.

**Recent Changes**

TLS connections are now fully supported.

Most simple options, like 'verbose' work. 

**ToDo**

Need to support "router mode".

Need to support authorization using client certificates.

Use binary search for matching subscriptions.

Purge subscription cache based on "old than X" method.

Port many of the test cases from gnatsd to jnatsd.