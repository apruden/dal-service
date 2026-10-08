# DAL overview dashboard

Import `dal-overview.json` in Grafana and map its Prometheus input to the data
source scraping each node's `/metrics` endpoint. The dashboard uses the Classic
JSON model, fixed `instance`, `partition`, histogram bucket, and outcome labels,
and a six-hour default time range.

The panels show node-local signals. Leader acknowledgement age and Raft term
changes can indicate instability, but they do not establish cluster-wide quorum
or client availability. Continue to use external valid read/write canaries for
the availability gate in the production profile. A blank filesystem gauge may
mean capacity sampling failed; check `dal_storage_filesystem_sample_ok` and the
`DALDiskCapacitySampleFailed` alert. Backup age and maintenance state panels
belong here once those metrics exist.
