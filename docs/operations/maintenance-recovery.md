# Recovering a missing maintenance participant

This procedure applies after a maintenance operation has been installed. The
operation remains fenced until every frozen participant has recovered and an
operator submits an exact release request. There is no participant-removal or
force-release command.

1. Read the authoritative record:

   ```text
   dal maintenance-status --auth operator.json --cluster init.json
   ```

   Record the operation ID, generation, stage, participant list, and any node IDs
   awaiting acknowledgement. Run this again after a leader change; the command
   reads through the current meta leader.

2. For a participant missing in `Preparing`, restart that node with its existing
   durable directory, configured node ID, cluster ID, and CURVE identity. It
   must install the maintenance fence and drain accepted writes before its
   preparation acknowledgement appears. Do not infer completion from the
   process being reachable.

3. For a participant missing in `Verifying`, restart it with its existing
   durable state and the trust policy intended for the whole cluster. The
   runtime must pass bootstrap and every hosted voting group's recovery gate,
   then submit a new acknowledgement containing its current process
   incarnation, frozen topology digest, and policy digest. A new process
   replaces its old proof; a stale proof cannot satisfy the release heartbeat
   check. Keep the operation fenced while any participant is absent.

4. If all participants are acknowledged but the verification policy is
   `pending`, compare the per-participant policy digests in the status output.
   Correct the outlying node's policy and restart it so its new process can
   replace the old digest. The release leader's policy must match the common
   participant digest.

5. Once every participant is recovered and the policy digests agree, submit
   the exact operation ID and generation shown by `maintenance-status`:

   ```text
   dal maintenance-release --auth operator.json --cluster init.json \
     --operation 0x... --generation N
   ```

   The current meta leader waits for a new heartbeat from each recorded
   process, checks its own trust policy, and submits the release. A rejected
   request leaves maintenance fenced. A client timeout has an unknown outcome:
   the release may have committed before its reply was lost. Re-read status
   before retrying the same operation ID and generation.
   Increase `--wait-seconds` if the configured heartbeat interval makes the
   default 60-second client timeout too short.

If a participant's durable state has been permanently lost, this version has
no supported way to reconstruct that participant under the frozen topology.
Keep the operation fenced. Recovery from a verified backup is part of the
backup and restore phase; loss without a recoverable artifact has no safe
release procedure in the current build.
