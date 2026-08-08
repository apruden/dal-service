use std::cmp::Ordering;
use std::future::Future;
use std::pin::Pin;

use futures::stream::{FuturesUnordered, StreamExt};

use crate::error::{Error, Result};
use crate::search::{
    GenerationSelection, PartitionFailure, ScoringMode, SearchIndexGeneration, SearchReply,
    SearchRequest,
};

/// Total descending score order: scored hits before unscored, and NaN handled
/// via `total_cmp` so the distributed merge stays deterministic (I12).
fn score_descending(left: &Option<f32>, right: &Option<f32>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => right.total_cmp(left),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

pub trait ShardSearchSource: Send + Sync {
    fn search(
        &self,
        partition: u16,
        request: SearchRequest,
    ) -> Pin<Box<dyn Future<Output = Result<SearchReply>> + Send + '_>>;
}

/// Bounded scatter/gather merge over every configured partition.
pub struct SearchCoordinator<S> {
    partitions: u16,
    shards: S,
}

impl<S: ShardSearchSource> SearchCoordinator<S> {
    pub fn new(partitions: u16, shards: S) -> Result<Self> {
        if partitions == 0 {
            return Err(Error::Search(
                "search coordinator requires partitions".into(),
            ));
        }
        Ok(Self { partitions, shards })
    }

    pub async fn search(
        &self,
        generation: &SearchIndexGeneration,
        request: &SearchRequest,
    ) -> Result<SearchReply> {
        generation.validate_identity()?;
        if !matches!(request.scoring, ScoringMode::LocalBm25) {
            return Err(Error::Search(
                "GlobalBm25 is unavailable until two-phase global statistics are supplied".into(),
            ));
        }
        if request.deadline_ms == 0 || request.deadline_ms > 60_000 {
            return Err(Error::Search("deadline_ms must be in 1..=60000".into()));
        }
        let per_shard = request
            .offset
            .checked_add(request.limit)
            .ok_or_else(|| Error::Search("search page overflow".into()))?;
        let mut shard_request = request.clone();
        shard_request.generation = GenerationSelection::Exact(generation.id);
        shard_request.offset = 0;
        shard_request.limit = per_shard;

        let mut pending = FuturesUnordered::new();
        for partition in 0..self.partitions {
            let request = shard_request.clone();
            pending.push(async move { (partition, self.shards.search(partition, request).await) });
        }
        let mut replies = Vec::with_capacity(self.partitions as usize);
        let mut failures = Vec::new();
        let mut completed = vec![false; self.partitions as usize];
        let deadline = std::time::Duration::from_millis(request.deadline_ms.max(1) as u64);
        let fanout = tokio::time::timeout(deadline, async {
            while let Some((partition, reply)) = pending.next().await {
                completed[partition as usize] = true;
                match reply {
                    Ok(reply)
                        if reply.generation == generation.id
                            && reply.definition_hash == generation.definition_hash =>
                    {
                        replies.push(reply)
                    }
                    Ok(_) => failures.push(PartitionFailure {
                        partition,
                        reason: "shard index identity mismatch".into(),
                    }),
                    Err(error) => failures.push(PartitionFailure {
                        partition,
                        reason: error.to_string(),
                    }),
                }
            }
        })
        .await;
        if fanout.is_err() {
            for partition in 0..self.partitions {
                if !completed[partition as usize] {
                    failures.push(PartitionFailure {
                        partition,
                        reason: "search deadline exceeded".into(),
                    });
                }
            }
        }
        failures.sort_by_key(|failure| failure.partition);
        if !failures.is_empty() && !request.allow_partial {
            return Err(Error::Search(format!(
                "{} of {} partitions failed: {:?}",
                failures.len(),
                self.partitions,
                failures
            )));
        }
        let total_hits = replies.iter().map(|reply| reply.total_hits).sum();
        let mut hits = replies
            .into_iter()
            .flat_map(|reply| reply.hits)
            .collect::<Vec<_>>();
        hits.sort_by(|left, right| {
            score_descending(&left.score, &right.score)
                .then_with(|| left.partition.cmp(&right.partition))
                .then_with(|| left.key.cmp(&right.key))
        });
        hits = hits
            .into_iter()
            .skip(request.offset as usize)
            .take(request.limit as usize)
            .collect();
        Ok(SearchReply {
            generation: generation.id,
            definition_hash: generation.definition_hash,
            hits,
            total_hits,
            partial: !failures.is_empty(),
            failed_partitions: failures,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn score_order_is_total_even_with_nan_and_none() {
        let values = [
            Some(2.0_f32),
            Some(f32::NAN),
            None,
            Some(1.0),
            Some(f32::NAN),
            None,
            Some(2.0),
        ];
        let mut sorted = values.to_vec();
        // A non-total comparator panics here on Rust >= 1.81; determinism is
        // asserted by comparing against an independently sorted copy.
        sorted.sort_by(score_descending);
        let mut again = values.to_vec();
        again.reverse();
        again.sort_by(score_descending);
        assert_eq!(
            sorted
                .iter()
                .map(|s| s.map(f32::to_bits))
                .collect::<Vec<_>>(),
            again
                .iter()
                .map(|s| s.map(f32::to_bits))
                .collect::<Vec<_>>()
        );
        // Unscored hits sort after every scored hit.
        assert!(sorted.iter().rev().take(2).all(Option::is_none));
        // Plain scores stay descending.
        let plain: Vec<f32> = sorted
            .iter()
            .flatten()
            .copied()
            .filter(|s| !s.is_nan())
            .collect();
        assert_eq!(plain, vec![2.0, 2.0, 1.0]);
    }
}
