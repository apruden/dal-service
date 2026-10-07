# Linux systemd deployment

`dal.service` is a single-node example for Linux. Install the release binary at
`/usr/local/bin/dal`, create the dedicated `dal` user and group, and provision
`/etc/dal/node.json`, `/etc/dal/cluster.json`, the trust policy, and CURVE keys
before enabling the unit. The node and cluster files should be readable by the
service account; private keys and trust-generation markers must remain mode
`0600` and owned by that account.

Create the data and security-marker directories before startup. Adjust
`ReadWritePaths` and the config paths for each node. Give the unit write access
only to that node's data directory and its external trust-generation marker
directory. Do not place the marker inside the data directory or backup set.

The unit runs `dal check-config` before `dal run`. The check is read-only and
requires a libzmq build with CURVE support. `TimeoutStopSec=75s` leaves 15
seconds beyond the default 60-second application shutdown timeout. Keep peer
units independent: `After=network-online.target` does not order peers behind
one another or wait for client readiness.

Install and start the service with:

```sh
sudo install -D -m 0644 deploy/systemd/dal.service /etc/systemd/system/dal.service
sudo systemctl daemon-reload
sudo systemctl enable --now dal.service
sudo systemctl status dal.service
sudo journalctl -u dal.service
```

For multiple nodes on one host, create one unit per node with unique config,
data, and marker paths. Production qualification still requires independent
failure domains, the recovery and maintenance procedures, and the capacity
profile; this unit alone does not provide those guarantees.
