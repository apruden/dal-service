# Transport credentials and operator CLI

`dal run` requires a node `security` section. The production binary fails startup
when the trust policy, key files, endpoint pins, or libzmq CURVE support are
missing. Node-to-node control and bulk sockets use CURVE; each outbound address
must have an out-of-band key pin.

## Trust policy

Install the same cluster-scoped policy on every node and operator host. Keep the
policy and all key files owned by the service or operator account and mode
`0600`. Key files contain the 40-character Z85 representation of the key. The
policy contains public keys only:

```json
{
  "cluster_id": "0x1234",
  "generation": 1,
  "principals": [
    {
      "user_id": "node:1",
      "curve_public_key": "<node-1-public-key-z85>",
      "permissions": ["peer_rpc"]
    },
    {
      "user_id": "app:orders",
      "curve_public_key": "<orders-public-key-z85>",
      "permissions": ["client_read", "client_write", "search"]
    },
    {
      "user_id": "ops:production",
      "curve_public_key": "<operator-public-key-z85>",
      "permissions": ["operator"]
    }
  ]
}
```

Use a unique CURVE key for each principal. Every `node:<id>` must have
`peer_rpc`; operators need `operator`; application permissions are granted
individually. A key with no permission is rejected. The generation must be
nonzero and cluster IDs must match the node and descriptor.
Create the generation-marker directory before starting the service. Keep its
marker and lock files outside the database directory and every backup/restore
set; protect that directory from deletion by the service account or restore
automation. Startup atomically advances the marker and rejects an older policy.

## Node config

Add this object to each node config. Paths may be absolute or relative to that
JSON file. Include pins for every endpoint the node can contact, including both
control and bulk endpoints for descriptor members and control endpoints used as
seeds. The principal name in each pin must match the node that owns the address.

```json
{
  "cluster_id": "0x1234",
  "node_id": 1,
  "control_addr": "tcp://10.0.0.11:7001",
  "bulk_addr": "tcp://10.0.0.11:7002",
  "data_dir": "/var/lib/dal/node-1",
  "seeds": ["tcp://10.0.0.12:7001"],
  "security": {
    "trust_policy_file": "trust-policy.json",
    "trust_generation_file": "/var/lib/dal-security/node-1-generation",
    "curve_public_key_file": "node-1-public.key",
    "curve_secret_key_file": "node-1-secret.key",
    "server_key_pins": {
      "tcp://10.0.0.11:7001": "node:1",
      "tcp://10.0.0.11:7002": "node:1",
      "tcp://10.0.0.12:7001": "node:2",
      "tcp://10.0.0.12:7002": "node:2"
    }
  }
}
```

The local node key must map to its configured `node:<id>` principal. The node
loader adds its own control and bulk pins from that key. Every other bootstrap
descriptor endpoint must be explicitly pinned and bound to the corresponding
node principal. New peer addresses require an out-of-band pin update before the
node starts dialing them; directory data never establishes trust.

## Operator auth file

Use a separate operator key rather than copying a node secret to an operator
workstation. The file paths follow the same relative-path rule. Pin every
control and bulk endpoint in the cluster descriptor to its `node:<id>`.

```json
{
  "cluster_id": "0x1234",
  "trust_policy_file": "trust-policy.json",
  "trust_generation_file": "/var/lib/dal-security/operator-generation",
  "user_id": "ops:production",
  "curve_public_key_file": "operator-public.key",
  "curve_secret_key_file": "operator-secret.key",
  "server_key_pins": {
    "tcp://10.0.0.11:7001": "node:1",
    "tcp://10.0.0.11:7002": "node:1",
    "tcp://10.0.0.12:7001": "node:2",
    "tcp://10.0.0.12:7002": "node:2"
  }
}
```

Run nodes with `dal run --config node.json --cluster init.json`. Operator
commands take `--auth operator.json`, for example:

```sh
dal status --auth operator.json --cluster init.json
dal leave --auth operator.json --cluster init.json --node 3
```

`dal join` takes both the joining node config and the operator auth file. All
explicit `--seed` addresses must already have pins. Administrative RPCs require
the operator permission at the receiving node. Client reads, writes, and search
are checked against their respective permissions. A non-loopback `http_addr` is
rejected; remote diagnostics require a separately authenticated TLS proxy.
