# Redis-Raft Jepsen Test

This is a test suite, written using the [Jepsen distributed systems testing
library](https://jepsen.io), for [Redis](https://github.com/RedisLabs/redis).
It provides a single workload (`jepsen.redis.append`) based on list append,
implemented using `LRANGE` and `RPUSH`, which uses
[Elle](https://github.com/jepsen-io/elle) to find transactional anomalies up to
strict serializability.

This was originally written for redis-raft, which seems to be unmaintained.
I've rewritten it to run a single Redis instance; later I'd like to add Redis
Cluster.

## Prerequisites

You'll need a [Jepsen cluster](https://github.com/jepsen-io/jepsen#setting-up-a-jepsen-environment) running Debian 13.

## Usage

To get started, try

```
lein run test -n n1
```

To see all options, try

```
lein run test --help
```

## License

Copyright © Jepsen, LLC

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
