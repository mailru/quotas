<a href="http://tarantool.org">
   <img src="https://avatars2.githubusercontent.com/u/2344919?v=2&s=250"
align="right">
</a>

# Quota manager for Tarantool 1.6+

A quota is the maximum number of requests per second that
an identified action should occur in a cluster. For example:
"Guest users can do up to 3 inserts per second to space X."

Tarantool's quota manager is useful in any busy cluster where
there are rules for cooperating. For example, it is in use by
a telecoms organization where the idea is that too many
near-simultaneous operations would slow the system down - so
this is like advising a crowd of runners to leave space between
each other.

## Table of contents

* [Installation](#installation)
* [Initialization: init()](#initialization-init)
* [Usage: get\_lease()](#usage-get_lease)
* [How it works, from the user's point of view](#how-it-works-from-the-users-point-of-view)
* [How it works, inside the code](#how-it-works-inside-the-code)

## Installation

Pre-requisites:

* Tarantool version 1.6 or later
* `connpool` module (see https://github.com/tarantool/connpool).

To bring in the `quotas` module (for this example we assume
that the usual path includes directory ~/tarantool_sandbox), say:

```
git clone https://github.com/mailru/quotas.git
cp ~/quotas/src/lease.lua ~/tarantool_sandbox
```

## Initialization: init()

```
init (conn_pool, neighbor_count, max_lease)
```

Parameters:

* `conn_pool` - identifies a pool in a cluster.
  In order for `lease.init()` to be effective, it is first
  necessary to start a cluster and identify a pool within
  it. The nodes in a connection pool can be the same as all
  the nodes in a Tarantool cluster, but this is not strictly necessary.

* `neighbor_count` (integer) - states how many neighbors a node has.
  This is used for initial communication.
  The quota manager will pick a random set of nodes to
  send to, when it needs to inform other nodes in the
  cluster that a change is taking place.
  Eventually those other nodes will propagate the
  information to other nodes in the cluster, so
  soon the whole cluster will have the information.

  Minimum value = 1.

  Recommended value = at least 2.

  Maximum value = indefinite (the quota manager uses `neighbor_count` or
  the number of nodes in the cluster, whichever is smaller).

* `max_lease` (integer) - states the "batch size", officially known as
  `max_lease`. The reason for batching is that we don't
  want too many messages to clutter the cluster.
  If the quota manager on node#1 receives a request to lease
  (the `get_lease` function), it does nothing until it has received
  `batch-size` requests - the reply to the user is 'true'.

  Minimum value = 1.

  Recommended value = something greater than 1.

Example:

```
lease = require('lease')
lease.init(connpoolobject, 3, 1)
```

## Usage: get_lease()

```
get_lease(user, domain, op, quota)
```

Parameters:

* `user`, `domain`, `op` (strings) - may have any arbitrary values which
  the organization agrees on. The names suggest the most common sort of values
  - a user name such as 'guest', an area of applicability such as a space
  name, and an operation such as 'insert'. However, the
  quota manager uses these values only to form a unique
  identifier of what has a requests-per-second `quota`.
  Provided that is possible, the parameters `user`, `domain` and `op` can
  have any arbitrary values.

* `quota` (integer) - we will always refer to this parameter as
  the requests-per-second `quota`, states how often a leased action may occur
  within a second.

  The general idea is: the first three parameters
  identify the action. The fourth parameter states
  how many times the action may occur in a second.

Return value: 'true' or 'false'

The general idea is: the return value will be 'true' if:

* In the last second, the action happened less than or equal to
  requests-per-second `quota` times, or

* The `max_lease` batch size is greater than 1, and there have been fewer
  than batch size actions.

Therefore, a return value of 'false' means:
this action has been done too many times on the cluster,
it might be better to delay or cancel instead of performing it now.

Example:

```
lease.get_lease('guest','localhost','inserts',5)
```

Meaning: return 'true' if the action `guest user on localhost inserts`
is considered to be within the requests-per-second `quota`,
which is approximately 5.

## How it works, from the user's point of view

The typical program will contain, in pseudocode:

```
If (get_lease() returns true)
  perform the intended action
Else
  If (action is necessary)
    Repeat get_lease until it returns true,
    or until action cannot be delayed any longer
  Else
    Abort action
```

## How it works, inside the code

[Internally](https://github.com/mailru/quotas/blob/master/src/lease.lua),
a potential action is called a "resource".

The `quotas` module is intended to count resources over the last second
in a cluster. The `quotas` module itself does not limit or throttle
resource usage, but programs on each node may do so given what `get_lease()`
returns.

On each node, the quota manager maintains a local view of
a table, the Resource Usage Table, which contains a record
of resource usage for the latest 1 second. The table has one row
for each resource, and each row has a 10-element array, and
each element in the array has the number of times that the resource
has been used in a one-tenth-of-a-second period. Each array is a
FIFO stack - when a new tenth-of-a-second period begins,
the `quotas` module removes the oldest element and adds a new element.
The table also contains the time of the most recent update.

For example, suppose there are two resources:
`guest::_space1::insert` and `admin::_space2:select`.
The Resource Usage Table might look like this:

```
Time = 11;00:45.25
"guest::_space1::insert", {3,2,1,1,3,0,0,0,0,0}
"admin::_space2::select", {85,2,3,1,3,0,0,0,0,0}
```

Meaning: approximately 1.0 seconds ago, between time = 11:00:44.20
and time 11:00:44.30 (a figure that we get by subtracting 1 from 11:00:45.25
and rounding down and rounding up), there were 3 `guest::space1::select` and
85 `admin::_space2:select` resource uses, either due to `get_lease()` requests
on the local node or due to update information received from other nodes.
Approximately 0.9 seconds ago there were 2
`guest::space1::select` and 2 `admin::_space2:select` uses.
And so on, for each element in each array.

Now, if a `get_lease('guest','_space1','insert',11)`
request occurs, the return value will be 'true' because 3+2+1+1+3 = 10, so
the requests-per-second `quota` value (11) would not be exceeded.
However, for `get_lease('admin','_space2','select',11)`
the return value will be 'false' because 85+2+3+1+3 > 11.

One tenth of a second later, the resource usage table
will look like this:

```
Time = 11:00:45.35
"guest::_space1::insert", {2,3,1,3,0,0,0,0,0,1}
"admin::_space2::select", {2,3,1,3,0,0,0,0,0,0}
```

This is a change, so the local node might now send out
the information about its local Resource Usage Table
to other nodes in the cluster. Specifically: if the
second parameter of `init()` - `neighbor_count` - was 7,
and the number of nodes in the cluster is 55, the local node
will choose at random 7 other nodes in the cluster and send the
update information to them. These other nodes are called
the "neighbors". In turn, the neighbors will propagate
the changes until all nodes in the cluster have the updates.
This is a typical use of the
[gossip protocol](https://en.wikipedia.org/wiki/Gossip_protocol).

There are three reasons that the requests-per-second `quota`
is only approximate:

(1) time slices are one-tenth-second and therefore the meaning of
    "one second ago" is really
    "between 1.1 and 0.9 seconds ago";

(2) propagation of new information to all nodes is not instantaneous;

(3) it is not known what the `max_lease` batch size is on other nodes.

A quota's action identification - the combination of the first three arguments
to `get_lease()` - is meaningful only for users. Internally the quota manager
concatenates these arguments into a single string which is used as a key for
looking up actions in the Resource Usage Table, and for sending messages to
other nodes. The application decides what actual actions are performed for each
action identification.

The quota manager does not store the request-per-second `quota` value - it is
an argument in every `get_lease()` call. Thus the quota can be different for
different invocations and for different nodes. The quota manager's job is only
to compare the passed requests-per-second `quota` value with the number of times
that the action has happened, by adding up the integers in the array which is
associated with that action's row in the Resource Usage Table.

