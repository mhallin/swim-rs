SWIM.rs
=======

A rust implementation of the `Scalable Weakly-consistent Infection-style Process Group Membership
Protocol`_, a building block for distributed systems which require weakly-consistent knowledge about
members participating in a cluster.

The library is in an experimental state where it only provides the basic support for membership
information with an API that probably will change.

----

Trying It Out
-------------

A binary called ``swimmer`` is included as a demo application. It takes three or four arguments: a
folder to keep state data, a cluster name, a listen address, and an optional seed node to connect
to. The demo application is not intended for actual usage, but its source is a minimal example on
how use the library.

To try it out, run the following:

.. code-block:: sh

   # In one terminal window
   mkdir _data1
   cargo run _data1 cluster1 127.0.0.1:2552

   # In another terminal window
   mkdir _data2
   cargo run _data2 cluster1 127.0.0.1:2562 127.0.0.1:2552


Try starting a third or fourth instance in new data folders, or terminating some of the running
ones. You should see the member events ticking in and how the cluster is kept in sync across all
nodes.

The data folder contains a ``host_key`` file to identify the node across restarts.

Implementation Notes
--------------------

There are some shortcomings in the current implementation:

* It uses rustc-serialize's JSON encoder/decoder for communication, which is a bit too verbose for
  small UDP messages.
* It does not does do random periodic full state sync like Hashicorp's memberlist_.
* It does not support node metadata.

And lastly, it's not in any way tested on any larger clusters.

.. _Scalable Weakly-consistent Infection-style Process Group Membership Protocol: http://www.cs.cornell.edu/%7Easdas/research/dsn02-swim.pdf
.. _memberlist: https://github.com/hashicorp/memberlist
