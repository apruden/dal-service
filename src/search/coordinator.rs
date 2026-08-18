use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::{self, StreamExt};

use crate::error::{Error, Result};
use crate::search::{
    GenerationSelection, PartitionFailure, ScoringMode, SearchIndexGeneration, SearchReply,
    SearchRequest, SearchSessionId, ShardExecuteRequest, ShardPrepareRequest, ShardPrepared,
    ShardStatistics,
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
    /// One-shot partition-locally scored search.
    fn search(
        &self,
        partition: u16,
        request: Arc<SearchRequest>,
    ) -> Pin<Box<dyn Future<Output = Result<SearchReply>> + Send + '_>>;

    /// Phase one of global scoring: fence the shard leader, pin a searcher,
    /// and return the statistics measured against it.
    fn prepare(
        &self,
        partition: u16,
        request: Arc<ShardPrepareRequest>,
    ) -> Pin<Box<dyn Future<Output = Result<ShardPrepared>> + Send + '_>>;

    /// Phase two: score the pinned searcher with cluster-wide statistics.
    fn execute(
        &self,
        partition: u16,
        request: Arc<ShardExecuteRequest>,
    ) -> Pin<Box<dyn Future<Output = Result<SearchReply>> + Send + '_>>;

    /// Abandon a prepared session without scoring it. Best effort: shards
    /// expire sessions on their own, so a failed release only delays reclaim.
    fn release(
        &self,
        partition: u16,
        session_id: SearchSessionId,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

impl ShardSearchSource for Box<dyn ShardSearchSource> {
    fn search(
        &self,
        partition: u16,
        request: Arc<SearchRequest>,
    ) -> Pin<Box<dyn Future<Output = Result<SearchReply>> + Send + '_>> {
        (**self).search(partition, request)
    }

    fn prepare(
        &self,
        partition: u16,
        request: Arc<ShardPrepareRequest>,
    ) -> Pin<Box<dyn Future<Output = Result<ShardPrepared>> + Send + '_>> {
        (**self).prepare(partition, request)
    }

    fn execute(
        &self,
        partition: u16,
        request: Arc<ShardExecuteRequest>,
    ) -> Pin<Box<dyn Future<Output = Result<SearchReply>> + Send + '_>> {
        (**self).execute(partition, request)
    }

    fn release(
        &self,
        partition: u16,
        session_id: SearchSessionId,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        (**self).release(partition, session_id)
    }
}

/// Bounded scatter/gather merge over every configured partition.
pub struct SearchCoordinator<S> {
    partitions: u16,
    shards: S,
}

/// What one partition contributed to a fan-out phase.
enum Contribution<T> {
    Value(u16, T),
    Failed(PartitionFailure),
}

struct ReplyAccumulator {
    window: usize,
    total_hits: u64,
    hits: Vec<crate::search::SearchHit>,
}

impl ReplyAccumulator {
    fn new(window: u32) -> Self {
        Self {
            window: window as usize,
            total_hits: 0,
            hits: Vec::new(),
        }
    }

    /// Keep only the globally best requested window as shard replies arrive.
    /// Memory is therefore bounded by one shard reply plus one result window,
    /// never `partitions * window`.
    fn add(&mut self, reply: SearchReply) {
        self.total_hits = self.total_hits.saturating_add(reply.total_hits);
        self.hits.extend(reply.hits);
        self.hits.sort_by(hit_order);
        self.hits.truncate(self.window);
    }
}

fn hit_order(left: &crate::search::SearchHit, right: &crate::search::SearchHit) -> Ordering {
    score_descending(&left.score, &right.score)
        .then_with(|| left.partition.cmp(&right.partition))
        .then_with(|| left.key.cmp(&right.key))
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
        if request.deadline_ms == 0 || request.deadline_ms > 60_000 {
            return Err(Error::Search("deadline_ms must be in 1..=60000".into()));
        }
        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(request.deadline_ms as u64);
        self.search_until(generation, request, deadline).await
    }

    /// Execute under an end-to-end deadline that may have started before the
    /// coordinator was constructed (for example, before catalog resolution).
    pub async fn search_until(
        &self,
        generation: &SearchIndexGeneration,
        request: &SearchRequest,
        deadline: tokio::time::Instant,
    ) -> Result<SearchReply> {
        generation.validate_identity()?;
        if request.deadline_ms == 0 || request.deadline_ms > 60_000 {
            return Err(Error::Search("deadline_ms must be in 1..=60000".into()));
        }
        if !request.sort.is_empty() {
            return Err(Error::Search("field sorting is not implemented".into()));
        }
        request.query.validate()?;
        if request.limit > crate::search::SEARCH_MAX_LIMIT
            || request
                .offset
                .checked_add(request.limit)
                .is_none_or(|window| window > crate::search::SEARCH_MAX_WINDOW)
        {
            return Err(Error::Search(format!(
                "search page exceeds limit {} or result window {}",
                crate::search::SEARCH_MAX_LIMIT,
                crate::search::SEARCH_MAX_WINDOW
            )));
        }
        // Every shard must offer at least the whole requested page, because the
        // global ordering can draw all of it from any single partition.
        let window = request.offset + request.limit;

        let (replies, failures) = match request.scoring {
            ScoringMode::LocalBm25 => {
                self.fan_out_one_shot(generation, request, window, deadline)
                    .await
            }
            ScoringMode::GlobalBm25 => {
                self.fan_out_two_phase(generation, request, window, deadline)
                    .await?
            }
        };
        self.merge(generation, request, replies, failures)
    }

    /// Partition-local scoring: a single round trip per shard.
    async fn fan_out_one_shot(
        &self,
        generation: &SearchIndexGeneration,
        request: &SearchRequest,
        window: u32,
        deadline: tokio::time::Instant,
    ) -> (ReplyAccumulator, Vec<PartitionFailure>) {
        let mut shard_request = request.clone();
        shard_request.generation = GenerationSelection::Exact(generation.id);
        shard_request.offset = 0;
        shard_request.limit = window;

        let shard_request = Arc::new(shard_request);
        self.fan_out(
            deadline,
            |partition| self.shards.search(partition, shard_request.clone()),
            (ReplyAccumulator::new(window), Vec::new()),
            |(replies, failures), outcome| match outcome {
                Contribution::Value(partition, reply) => {
                    match check_identity(generation, partition, &reply) {
                        Ok(()) => replies.add(reply),
                        Err(failure) => failures.push(failure),
                    }
                }
                Contribution::Failed(failure) => failures.push(failure),
            },
        )
        .await
    }

    /// Exact distributed BM25 (design §9.2): collect per-shard statistics from
    /// pinned searchers, sum them, then score every shard against the same
    /// cluster-wide values so the merged ranking is meaningful.
    async fn fan_out_two_phase(
        &self,
        generation: &SearchIndexGeneration,
        request: &SearchRequest,
        window: u32,
        deadline: tokio::time::Instant,
    ) -> Result<(ReplyAccumulator, Vec<PartitionFailure>)> {
        let prepare = Arc::new(ShardPrepareRequest {
            index: request.index.clone(),
            generation: generation.id,
            definition_hash: generation.definition_hash,
            query: request.query.clone(),
            consistency: request.consistency,
            window,
        });
        let (sessions, mut rejected_sessions, mut failures, global) = self
            .fan_out(
                deadline,
                |partition| self.shards.prepare(partition, prepare.clone()),
                (
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    ShardStatistics::default(),
                ),
                |(sessions, rejected_sessions, failures, global), outcome| match outcome {
                    Contribution::Value(partition, prepared) => {
                        if let Err(error) = prepared.statistics.validate() {
                            rejected_sessions.push((partition, prepared.session_id));
                            failures.push(PartitionFailure {
                                partition,
                                reason: bounded_failure_reason(error.to_string()),
                            });
                            return;
                        }
                        global.merge(&prepared.statistics);
                        sessions.push((partition, prepared.session_id));
                    }
                    Contribution::Failed(failure) => failures.push(failure),
                },
            )
            .await;

        // Fail before phase two rather than after: a partition missing from a
        // strict search must not have its statistics folded into scores the
        // client never receives hits for.
        if !failures.is_empty() && !request.allow_partial {
            // Release valid and rejected preparations in one bounded pass so a
            // slow rejected-session release cannot consume the whole deadline
            // before healthy sessions are even scheduled.
            rejected_sessions.extend(sessions);
            self.release_all(&rejected_sessions, deadline).await;
            return Err(shard_failure(self.partitions, &mut failures));
        }
        // In partial mode only malformed preparations are abandoned; healthy
        // sessions continue to phase two.
        self.release_all(&rejected_sessions, deadline).await;

        let global = Arc::new(global);
        let (replies, failures) = self
            .fan_out_over(
                sessions,
                deadline,
                |(partition, session_id)| {
                    let request = Arc::new(ShardExecuteRequest {
                        session_id,
                        statistics: global.clone(),
                    });
                    self.shards.execute(partition, request)
                },
                (ReplyAccumulator::new(window), failures),
                |(replies, failures), outcome| match outcome {
                    Contribution::Value(partition, reply) => {
                        match check_identity(generation, partition, &reply) {
                            Ok(()) => replies.add(reply),
                            Err(failure) => failures.push(failure),
                        }
                    }
                    Contribution::Failed(failure) => failures.push(failure),
                },
            )
            .await;
        Ok((replies, failures))
    }

    /// Hand back sessions this query will never execute, so a strict failure
    /// does not leave a pinned searcher on every healthy shard until it times
    /// out. Bounded by the request's own deadline.
    async fn release_all(
        &self,
        sessions: &[(u16, SearchSessionId)],
        deadline: tokio::time::Instant,
    ) {
        let mut pending = stream::iter(sessions.iter().copied())
            .map(|(partition, session_id)| self.shards.release(partition, session_id))
            .buffer_unordered(crate::search::SEARCH_MAX_FAN_OUT_CONCURRENCY);
        let drain = async { while pending.next().await.is_some() {} };
        let _ = tokio::time::timeout_at(deadline, drain).await;
    }

    async fn fan_out<T, F, Fut, A, C>(
        &self,
        deadline: tokio::time::Instant,
        call: F,
        state: A,
        consume: C,
    ) -> A
    where
        F: Fn(u16) -> Fut,
        Fut: Future<Output = Result<T>>,
        C: FnMut(&mut A, Contribution<T>),
    {
        self.fan_out_over(
            (0..self.partitions).collect(),
            deadline,
            call,
            state,
            consume,
        )
        .await
    }

    /// Run one phase against a set of partitions under a shared deadline. A
    /// partition that neither answered nor failed before the deadline is
    /// reported as a timeout, never silently dropped (I11).
    async fn fan_out_over<I, T, F, Fut, A, C>(
        &self,
        items: Vec<I>,
        deadline: tokio::time::Instant,
        call: F,
        mut state: A,
        mut consume: C,
    ) -> A
    where
        I: PartitionOf + Copy,
        F: Fn(I) -> Fut,
        Fut: Future<Output = Result<T>>,
        C: FnMut(&mut A, Contribution<T>),
    {
        let mut outstanding: BTreeSet<u16> = items.iter().map(|item| item.partition()).collect();
        let mut pending = stream::iter(items)
            .map(|item| {
                let partition = item.partition();
                let future = call(item);
                async move { (partition, future.await) }
            })
            .buffer_unordered(crate::search::SEARCH_MAX_FAN_OUT_CONCURRENCY);
        let collect = async {
            while let Some((partition, result)) = pending.next().await {
                outstanding.remove(&partition);
                consume(
                    &mut state,
                    match result {
                        Ok(value) => Contribution::Value(partition, value),
                        Err(error) => Contribution::Failed(PartitionFailure {
                            partition,
                            reason: bounded_failure_reason(error.to_string()),
                        }),
                    },
                );
            }
        };
        if tokio::time::timeout_at(deadline, collect).await.is_err() {
            for partition in outstanding {
                consume(
                    &mut state,
                    Contribution::Failed(PartitionFailure {
                        partition,
                        reason: "search deadline exceeded".into(),
                    }),
                );
            }
        }
        state
    }

    fn merge(
        &self,
        generation: &SearchIndexGeneration,
        request: &SearchRequest,
        replies: ReplyAccumulator,
        mut failures: Vec<PartitionFailure>,
    ) -> Result<SearchReply> {
        failures.sort_by_key(|failure| failure.partition);
        if !failures.is_empty() && !request.allow_partial {
            return Err(shard_failure(self.partitions, &mut failures));
        }
        let hits = replies
            .hits
            .into_iter()
            .skip(request.offset as usize)
            .take(request.limit as usize)
            .collect();
        let reply = SearchReply {
            generation: generation.id,
            definition_hash: generation.definition_hash,
            hits,
            total_hits: replies.total_hits,
            partial: !failures.is_empty(),
            failed_partitions: failures,
        };
        reply.validate_wire_size()?;
        Ok(reply)
    }
}

/// Lets one fan-out helper serve both a bare partition list and the
/// (partition, session) pairs phase two works from.
trait PartitionOf {
    fn partition(&self) -> u16;
}

impl PartitionOf for u16 {
    fn partition(&self) -> u16 {
        *self
    }
}

impl PartitionOf for (u16, SearchSessionId) {
    fn partition(&self) -> u16 {
        self.0
    }
}

fn check_identity(
    generation: &SearchIndexGeneration,
    partition: u16,
    reply: &SearchReply,
) -> std::result::Result<(), PartitionFailure> {
    if reply.generation == generation.id && reply.definition_hash == generation.definition_hash {
        return Ok(());
    }
    Err(PartitionFailure {
        partition,
        reason: "shard index identity mismatch".into(),
    })
}

fn bounded_failure_reason(mut reason: String) -> String {
    let limit = crate::search::SEARCH_MAX_FAILURE_REASON_BYTES;
    if reason.len() <= limit {
        return reason;
    }
    let mut end = limit.saturating_sub(3);
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    reason.truncate(end);
    reason.push_str("...");
    reason
}

fn shard_failure(partitions: u16, failures: &mut Vec<PartitionFailure>) -> Error {
    failures.sort_by_key(|failure| failure.partition);
    Error::Search(format!(
        "{} of {} partitions failed: {:?}",
        failures.len(),
        partitions,
        failures
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::{
        SearchConsistency, SearchHit, SearchIndexDefinition, SearchQuery, SortField,
    };
    use std::collections::HashSet;
    use std::sync::Mutex;

    /// Records what each phase was asked, so a test can assert that every
    /// shard was scored against one identical set of global statistics.
    struct MockShards {
        failing: HashSet<u16>,
        invalid_statistics: HashSet<u16>,
        definition_hash: [u8; 32],
        executed: Mutex<Vec<(u16, ShardStatistics)>>,
        released: Mutex<Vec<(u16, SearchSessionId)>>,
    }

    impl MockShards {
        fn new(failing: &[u16]) -> Self {
            Self {
                failing: failing.iter().copied().collect(),
                invalid_statistics: HashSet::new(),
                definition_hash: generation().definition_hash,
                executed: Mutex::new(Vec::new()),
                released: Mutex::new(Vec::new()),
            }
        }

        fn with_invalid_statistics(mut self, partitions: &[u16]) -> Self {
            self.invalid_statistics = partitions.iter().copied().collect();
            self
        }
    }

    impl ShardSearchSource for MockShards {
        fn search(
            &self,
            _partition: u16,
            _request: Arc<SearchRequest>,
        ) -> Pin<Box<dyn Future<Output = Result<SearchReply>> + Send + '_>> {
            Box::pin(async { Err(Error::Search("one-shot path unused".into())) })
        }

        fn prepare(
            &self,
            partition: u16,
            _request: Arc<ShardPrepareRequest>,
        ) -> Pin<Box<dyn Future<Output = Result<ShardPrepared>> + Send + '_>> {
            Box::pin(async move {
                if self.failing.contains(&partition) {
                    return Err(Error::Search("shard is down".into()));
                }
                let term = if self.invalid_statistics.contains(&partition) {
                    vec![0; crate::search::SEARCH_MAX_TERM_KEY_BYTES + 1]
                } else {
                    b"term".to_vec()
                };
                Ok(ShardPrepared {
                    session_id: SearchSessionId {
                        incarnation: 7,
                        sequence: 100 + partition as u64,
                    },
                    checkpoint_index: 7,
                    // Each shard contributes a distinct slice of the corpus.
                    statistics: ShardStatistics {
                        total_docs: 10 * (partition as u64 + 1),
                        field_tokens: vec![("title".into(), partition as u64 + 1)],
                        term_doc_freq: vec![(term, partition as u64 + 1)],
                    },
                })
            })
        }

        fn execute(
            &self,
            partition: u16,
            request: Arc<ShardExecuteRequest>,
        ) -> Pin<Box<dyn Future<Output = Result<SearchReply>> + Send + '_>> {
            Box::pin(async move {
                assert_eq!(request.session_id.incarnation, 7);
                assert_eq!(request.session_id.sequence, 100 + partition as u64);
                self.executed
                    .lock()
                    .unwrap()
                    .push((partition, (*request.statistics).clone()));
                Ok(SearchReply {
                    generation: 5,
                    definition_hash: self.definition_hash,
                    hits: vec![SearchHit {
                        partition,
                        key: vec![partition as u8],
                        version: 1,
                        score: Some(1.0 + partition as f32),
                        sort_values: Vec::new(),
                        stored_fields: Vec::new(),
                    }],
                    total_hits: 1,
                    partial: false,
                    failed_partitions: Vec::new(),
                })
            })
        }

        fn release(
            &self,
            partition: u16,
            session_id: SearchSessionId,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(async move {
                self.released.lock().unwrap().push((partition, session_id));
            })
        }
    }

    fn generation() -> SearchIndexGeneration {
        let definition = SearchIndexDefinition {
            document_type: "article".into(),
            fields: vec![crate::search::SearchField {
                name: "title".into(),
                source_path: vec![crate::search::PathSegment::Key("title".into())],
                kind: crate::search::FieldKind::Text {
                    tokenizer: "default".into(),
                    positions: true,
                },
                required: true,
                multi_valued: false,
                indexed: true,
                stored: true,
                fast: false,
            }],
            default_search_fields: vec!["title".into()],
        };
        SearchIndexGeneration::new(5, definition).unwrap()
    }

    fn request(allow_partial: bool) -> SearchRequest {
        SearchRequest {
            index: "articles".into(),
            generation: GenerationSelection::Active,
            query: SearchQuery::Text {
                query: "term".into(),
                fields: vec![],
            },
            limit: 10,
            offset: 0,
            sort: vec![],
            scoring: ScoringMode::GlobalBm25,
            consistency: SearchConsistency::Strict,
            allow_partial,
            deadline_ms: 5_000,
        }
    }

    #[tokio::test]
    async fn every_shard_is_scored_against_the_same_summed_statistics() {
        let coordinator = SearchCoordinator::new(3, MockShards::new(&[])).unwrap();
        let reply = coordinator
            .search(&generation(), &request(false))
            .await
            .unwrap();
        assert_eq!(reply.hits.len(), 3);
        assert!(!reply.partial);

        let executed = coordinator.shards.executed.lock().unwrap();
        assert_eq!(executed.len(), 3);
        let expected = ShardStatistics {
            total_docs: 10 + 20 + 30,
            field_tokens: vec![("title".into(), 1 + 2 + 3)],
            term_doc_freq: vec![(b"term".to_vec(), 1 + 2 + 3)],
        };
        for (_, statistics) in executed.iter() {
            assert_eq!(*statistics, expected);
        }
    }

    #[tokio::test]
    async fn global_search_rejects_unimplemented_field_sorting() {
        let coordinator = SearchCoordinator::new(3, MockShards::new(&[])).unwrap();
        let mut request = request(false);
        request.sort.push(SortField {
            field: "title".into(),
            descending: false,
        });

        let error = coordinator
            .search(&generation(), &request)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("sorting is not implemented"));
        assert!(coordinator.shards.executed.lock().unwrap().is_empty());
    }

    /// I11: a strict search must fail before phase two, so a partition's
    /// statistics never shape scores in a response that omits its hits.
    #[tokio::test]
    async fn a_strict_search_fails_closed_before_executing_any_shard() {
        let coordinator = SearchCoordinator::new(3, MockShards::new(&[1])).unwrap();
        let error = coordinator
            .search(&generation(), &request(false))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("1 of 3 partitions failed"));
        assert!(coordinator.shards.executed.lock().unwrap().is_empty());
        // The two shards that did prepare must get their searchers back
        // instead of pinning segment files until the session expires.
        let mut released = coordinator.shards.released.lock().unwrap().clone();
        released.sort_unstable();
        assert_eq!(
            released,
            vec![
                (
                    0,
                    SearchSessionId {
                        incarnation: 7,
                        sequence: 100,
                    },
                ),
                (
                    2,
                    SearchSessionId {
                        incarnation: 7,
                        sequence: 102,
                    },
                ),
            ]
        );
    }

    #[tokio::test]
    async fn a_partial_search_names_every_omitted_partition() {
        let coordinator = SearchCoordinator::new(3, MockShards::new(&[2])).unwrap();
        let reply = coordinator
            .search(&generation(), &request(true))
            .await
            .unwrap();
        assert!(reply.partial);
        assert_eq!(reply.failed_partitions.len(), 1);
        assert_eq!(reply.failed_partitions[0].partition, 2);
        assert_eq!(reply.hits.len(), 2);
    }

    #[tokio::test]
    async fn invalid_prepared_statistics_release_their_session() {
        let shards = MockShards::new(&[]).with_invalid_statistics(&[1]);
        let coordinator = SearchCoordinator::new(3, shards).unwrap();
        let reply = coordinator
            .search(&generation(), &request(true))
            .await
            .unwrap();
        assert_eq!(reply.failed_partitions.len(), 1);
        assert_eq!(reply.failed_partitions[0].partition, 1);
        assert_eq!(
            coordinator.shards.released.lock().unwrap().as_slice(),
            &[(
                (1),
                SearchSessionId {
                    incarnation: 7,
                    sequence: 101,
                }
            )]
        );
    }

    #[tokio::test]
    async fn fan_out_never_polls_more_than_the_configured_bound() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let coordinator = SearchCoordinator::new(200, MockShards::new(&[])).unwrap();
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let completed = coordinator
            .fan_out(
                tokio::time::Instant::now() + Duration::from_secs(5),
                |_partition| {
                    let active = active.clone();
                    let maximum = maximum.clone();
                    async move {
                        let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                        maximum.fetch_max(now, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(2)).await;
                        active.fetch_sub(1, Ordering::SeqCst);
                        Ok::<_, Error>(())
                    }
                },
                0usize,
                |completed, _| *completed += 1,
            )
            .await;
        assert_eq!(completed, 200);
        assert!(maximum.load(Ordering::SeqCst) <= crate::search::SEARCH_MAX_FAN_OUT_CONCURRENCY);
    }

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
