//! Transport-neutral client side of retained durable handlers.

use std::{future::Future, sync::Arc};

use myko_federation::{
    AuthorityUnavailable, LiveCollection, LiveCollectionHandle, LiveCollectionState,
    LiveCollectionWriter, LiveSubscription, LiveSubscriptionHandle, LiveSubscriptionState,
    LiveSubscriptionWriter, LogPosition, NodeId, ReconnectPolicy, ScopeId, SubscriptionLiveness,
    live_collection, live_subscription,
};
use myko_wire::{ErasedHandlerState, ErasedViewDelta, HandlerRequest, HandlerStreamRevision};
use serde::de::DeserializeOwned;

use super::MykoClient;
use crate::{
    query::QueryParams,
    report::{ReportOutputType, ReportParams},
    view::ViewParams,
};

type ApplyDelta<T, C> = fn(
    &mut LiveSubscriptionState<T, C>,
    &mut Option<Vec<String>>,
    ErasedViewDelta,
) -> Result<(), HandlerClientError>;
type DecodedHandlerState<T, C> = (LiveSubscriptionState<T, C>, Option<Vec<String>>);
type KeyedRows<T> = Vec<(Arc<str>, Arc<T>)>;

/// Failure while opening or following a durable application handler.
#[derive(Debug, thiserror::Error)]
pub enum HandlerClientError {
    #[error("this Myko client has no durable handler connector")]
    MissingConnector,
    #[error("durable handler transport failed: {0}")]
    Transport(String),
    #[error("durable handler protocol failed: {0}")]
    Protocol(String),
    #[error("authority unavailable: {0}")]
    AuthorityUnavailable(AuthorityUnavailable),
    #[error("durable handler value decoding failed: {0}")]
    Decode(#[from] serde_json::Error),
}

impl HandlerClientError {
    const fn is_recoverable(&self) -> bool {
        matches!(self, Self::Transport(_) | Self::AuthorityUnavailable(_))
    }
}

/// One transport-neutral handler frame after connection authorization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandlerFrame {
    Resynchronizing {
        reason: String,
    },
    State {
        revision: HandlerStreamRevision,
        state: ErasedHandlerState,
    },
    ViewDelta {
        revision: HandlerStreamRevision,
        delta: ErasedViewDelta,
    },
}

/// An authorized, ordered durable-handler connection.
#[async_trait::async_trait]
pub trait HandlerConnection: Send {
    /// Receive the next handler frame.
    async fn recv(&mut self) -> Result<HandlerFrame, HandlerClientError>;
}

/// Connector implemented by local, Iroh, or embedded node transports.
#[async_trait::async_trait]
pub trait HandlerConnector: Send + Sync {
    /// Resolve the node against which handler routing methods are evaluated.
    async fn target_node(&self) -> Result<NodeId, HandlerClientError>;

    /// Open one authorized handler stream and return its initial snapshot.
    async fn connect(
        &self,
        request: HandlerRequest,
    ) -> Result<(HandlerFrame, Box<dyn HandlerConnection>), HandlerClientError>;

    /// Clone this connector with a different routed destination.
    fn at(&self, destination: NodeId) -> Arc<dyn HandlerConnector>;

    /// Return the retry policy used by reactive watches.
    fn reconnect_policy(&self) -> ReconnectPolicy;
}

/// Current-then-live typed durable handler stream owned by [`MykoClient`].
pub struct NodeHandlerSubscription<T, C = LogPosition>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    connector: Arc<dyn HandlerConnector>,
    request: HandlerRequest,
    connection: Box<dyn HandlerConnection>,
    revision: HandlerStreamRevision,
    current: LiveSubscriptionState<T, C>,
    row_keys: Option<Vec<String>>,
    keyed: bool,
    apply_delta: ApplyDelta<T, C>,
}

impl<T, C> NodeHandlerSubscription<T, C>
where
    T: hyphae::CellValue + DeserializeOwned,
    C: hyphae::CellValue + DeserializeOwned,
{
    async fn connect(
        connector: Arc<dyn HandlerConnector>,
        request: HandlerRequest,
        keyed: bool,
        apply_delta: ApplyDelta<T, C>,
    ) -> Result<Self, HandlerClientError> {
        let (frame, connection) = connector.connect(request.clone()).await?;
        let HandlerFrame::State { revision, state } = frame else {
            return Err(HandlerClientError::Protocol(
                "handler stream did not begin with a state snapshot".to_owned(),
            ));
        };
        if revision.sequence != 0 {
            return Err(HandlerClientError::Protocol(format!(
                "handler stream began at sequence {} instead of zero",
                revision.sequence
            )));
        }
        let (current, row_keys) = decode_handler_state(state)?;
        Ok(Self {
            connector,
            request,
            connection,
            revision,
            current,
            row_keys,
            keyed,
            apply_delta,
        })
    }

    /// Return the newest coherent value, cursor, and liveness revision.
    #[must_use]
    pub const fn current(&self) -> &LiveSubscriptionState<T, C> {
        &self.current
    }

    /// Return the authoritative keys paired with a collection snapshot.
    #[must_use]
    pub fn row_keys(&self) -> Option<&[String]> {
        self.row_keys.as_deref()
    }

    fn validate_revision(
        &self,
        revision: HandlerStreamRevision,
        is_state: bool,
    ) -> Result<(), HandlerClientError> {
        let is_fresh_epoch =
            is_state && revision.epoch != self.revision.epoch && revision.sequence == 0;
        let expected = self.revision.sequence.saturating_add(1);
        if !is_fresh_epoch
            && (revision.epoch != self.revision.epoch || revision.sequence != expected)
        {
            return Err(HandlerClientError::Protocol(format!(
                "handler revision gap: expected {}:{expected}, received {}:{}",
                self.revision.epoch, revision.epoch, revision.sequence
            )));
        }
        Ok(())
    }

    /// Wait for the next ordered handler revision.
    ///
    /// # Errors
    ///
    /// Returns an error on terminal transport loss, a sequence gap, or invalid typed data.
    pub async fn recv(&mut self) -> Result<LiveSubscriptionState<T, C>, HandlerClientError> {
        match self.connection.recv().await? {
            HandlerFrame::Resynchronizing { reason } => {
                self.current.liveness = SubscriptionLiveness::Resynchronizing { reason };
                return Ok(self.current.clone());
            }
            HandlerFrame::State { revision, state } => {
                self.validate_revision(revision, true)?;
                let (current, row_keys) = decode_handler_state(state)?;
                self.current = current;
                self.row_keys = row_keys;
                self.revision = revision;
            }
            HandlerFrame::ViewDelta { revision, delta } if self.keyed => {
                self.validate_revision(revision, false)?;
                (self.apply_delta)(&mut self.current, &mut self.row_keys, delta)?;
                self.revision = revision;
            }
            HandlerFrame::ViewDelta { .. } => {
                return Err(HandlerClientError::Protocol(
                    "scalar handler received a keyed view delta".to_owned(),
                ));
            }
        }
        Ok(self.current.clone())
    }

    fn reconnect(&self) -> impl Future<Output = Result<Self, HandlerClientError>> + Send + 'static {
        let connector = Arc::clone(&self.connector);
        let request = self.request.clone();
        let keyed = self.keyed;
        let apply_delta = self.apply_delta;
        async move { Self::connect(connector, request, keyed, apply_delta).await }
    }
}

fn reject_view_delta<T, C>(
    _current: &mut LiveSubscriptionState<T, C>,
    _row_keys: &mut Option<Vec<String>>,
    _delta: ErasedViewDelta,
) -> Result<(), HandlerClientError> {
    Err(HandlerClientError::Protocol(
        "scalar handler received a keyed view delta".to_owned(),
    ))
}

fn decode_handler_state<T, C>(
    state: ErasedHandlerState,
) -> Result<DecodedHandlerState<T, C>, HandlerClientError>
where
    T: DeserializeOwned,
    C: DeserializeOwned,
{
    Ok((
        LiveSubscriptionState {
            value: state.value.map(serde_json::from_value).transpose()?,
            through: state.through.map(serde_json::from_value).transpose()?,
            liveness: state.liveness,
        },
        state.row_keys,
    ))
}

fn apply_view_delta<T, C>(
    current: &mut LiveSubscriptionState<Vec<T>, C>,
    row_keys: &mut Option<Vec<String>>,
    delta: ErasedViewDelta,
) -> Result<(), HandlerClientError>
where
    T: hyphae::CellValue + DeserializeOwned,
    C: hyphae::CellValue + DeserializeOwned,
{
    let items = current.value.take().unwrap_or_default();
    let keys = row_keys.take().unwrap_or_default();
    if keys.len() != items.len() {
        return Err(HandlerClientError::Protocol(
            "handler snapshot row keys do not match its values".to_owned(),
        ));
    }
    let previous_order = keys.clone();
    let mut rows = keys
        .into_iter()
        .zip(items)
        .collect::<std::collections::BTreeMap<_, _>>();
    for key in delta.deletes {
        rows.remove(&key);
    }
    for encoded in delta.upserts {
        rows.insert(encoded.key, serde_json::from_value(encoded.value)?);
    }
    let order = delta.order.unwrap_or(previous_order);
    let mut values = Vec::with_capacity(order.len());
    for key in &order {
        values.push(rows.remove(key).ok_or_else(|| {
            HandlerClientError::Protocol(format!("handler delta omitted ordered row {key:?}"))
        })?);
    }
    if !rows.is_empty() {
        return Err(HandlerClientError::Protocol(
            "handler delta left rows outside its authoritative order".to_owned(),
        ));
    }
    current.value = Some(values);
    current.through = delta.through.map(serde_json::from_value).transpose()?;
    current.liveness = delta.liveness;
    *row_keys = Some(order);
    Ok(())
}

/// Runtime owner for a durable handler's reactive scalar or snapshot value.
pub struct ReactiveHandlerSubscription<T, C = LogPosition>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    live: LiveSubscription<T, C>,
    writer: LiveSubscriptionWriter<T, C>,
    task: tokio::task::JoinHandle<()>,
}

impl<T, C> LiveSubscriptionHandle<T, C> for ReactiveHandlerSubscription<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    fn live_subscription(&self) -> &LiveSubscription<T, C> {
        &self.live
    }
}

impl<T, C> Drop for ReactiveHandlerSubscription<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    fn drop(&mut self) {
        self.writer.invalidate("subscription owner dropped");
        self.task.abort();
    }
}

/// Runtime owner for a durable handler's identity-preserving reactive view.
pub struct ReactiveViewSubscription<T, C = LogPosition>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    live: LiveCollection<T, C>,
    writer: LiveCollectionWriter<T, C>,
    task: tokio::task::JoinHandle<()>,
}

impl<T, C> LiveCollectionHandle<T, C> for ReactiveViewSubscription<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    fn live_collection(&self) -> &LiveCollection<T, C> {
        &self.live
    }
}

impl<T, C> Drop for ReactiveViewSubscription<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    fn drop(&mut self) {
        self.writer.invalidate("subscription owner dropped");
        self.task.abort();
    }
}

fn drive_handler<T, C>(
    mut subscription: NodeHandlerSubscription<T, C>,
) -> ReactiveHandlerSubscription<T, C>
where
    T: hyphae::CellValue + DeserializeOwned,
    C: hyphae::CellValue + DeserializeOwned,
{
    let (writer, live) = live_subscription(subscription.current.clone());
    let task_writer = writer.clone();
    let task = tokio::spawn(async move {
        loop {
            match subscription.recv().await {
                Ok(state) => {
                    task_writer.replace(state);
                    continue;
                }
                Err(error) if error.is_recoverable() => {
                    task_writer.resynchronizing(error.to_string());
                }
                Err(error) => {
                    task_writer.invalidate(error.to_string());
                    return;
                }
            }
            let mut delay = subscription.connector.reconnect_policy().initial_delay();
            loop {
                tokio::time::sleep(delay).await;
                match subscription.reconnect().await {
                    Ok(next) => {
                        task_writer.replace(next.current.clone());
                        subscription = next;
                        break;
                    }
                    Err(error) if error.is_recoverable() => {
                        task_writer.resynchronizing(error.to_string());
                        delay = subscription.connector.reconnect_policy().next_delay(delay);
                    }
                    Err(error) => {
                        task_writer.invalidate(error.to_string());
                        return;
                    }
                }
            }
        }
    });
    ReactiveHandlerSubscription { live, writer, task }
}

fn keyed_rows<T, C>(
    subscription: &NodeHandlerSubscription<Vec<T>, C>,
) -> Result<KeyedRows<T>, HandlerClientError>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    let items = subscription.current.value.as_deref().unwrap_or_default();
    let keys = subscription.row_keys.as_deref().unwrap_or_default();
    if keys.len() != items.len() {
        return Err(HandlerClientError::Protocol(
            "handler snapshot row keys do not match its values".to_owned(),
        ));
    }
    Ok(keys
        .iter()
        .zip(items)
        .map(|(key, item)| (Arc::from(key.as_str()), Arc::new(item.clone())))
        .collect())
}

fn drive_view<T, C>(
    mut subscription: NodeHandlerSubscription<Vec<T>, C>,
) -> Result<ReactiveViewSubscription<T, C>, HandlerClientError>
where
    T: hyphae::CellValue + DeserializeOwned,
    C: hyphae::CellValue + DeserializeOwned,
{
    let rows = keyed_rows(&subscription)?;
    let (writer, live) = live_collection(
        rows,
        LiveCollectionState {
            through: subscription.current.through.clone(),
            liveness: subscription.current.liveness.clone(),
        },
    );
    let task_writer = writer.clone();
    let task = tokio::spawn(async move {
        loop {
            match subscription.recv().await {
                Ok(_)
                    if let SubscriptionLiveness::Resynchronizing { reason } =
                        &subscription.current.liveness =>
                {
                    task_writer.resynchronizing(reason.clone());
                }
                Ok(_)
                    if let SubscriptionLiveness::Invalid { reason } =
                        &subscription.current.liveness =>
                {
                    task_writer.invalidate(reason.clone());
                    return;
                }
                Ok(_) => match keyed_rows(&subscription) {
                    Ok(rows) => {
                        if let Err(error) =
                            task_writer.reconcile(rows, subscription.current.through.clone())
                        {
                            task_writer.invalidate(error.to_string());
                            return;
                        }
                    }
                    Err(error) => {
                        task_writer.invalidate(error.to_string());
                        return;
                    }
                },
                Err(error) if error.is_recoverable() => {
                    task_writer.resynchronizing(error.to_string());
                    let mut delay = subscription.connector.reconnect_policy().initial_delay();
                    loop {
                        tokio::time::sleep(delay).await;
                        match subscription.reconnect().await {
                            Ok(next) => {
                                subscription = next;
                                match keyed_rows(&subscription) {
                                    Ok(rows) => {
                                        if let Err(error) = task_writer
                                            .reconcile(rows, subscription.current.through.clone())
                                        {
                                            task_writer.invalidate(error.to_string());
                                            return;
                                        }
                                    }
                                    Err(error) => {
                                        task_writer.invalidate(error.to_string());
                                        return;
                                    }
                                }
                                break;
                            }
                            Err(error) if error.is_recoverable() => {
                                task_writer.resynchronizing(error.to_string());
                                delay = subscription.connector.reconnect_policy().next_delay(delay);
                            }
                            Err(error) => {
                                task_writer.invalidate(error.to_string());
                                return;
                            }
                        }
                    }
                }
                Err(error) => {
                    task_writer.invalidate(error.to_string());
                    return;
                }
            }
        }
    });
    Ok(ReactiveViewSubscription { live, writer, task })
}

impl MykoClient {
    /// Create the retained application client over a durable node connector.
    #[must_use]
    pub fn with_handler_connector(connector: Arc<dyn HandlerConnector>) -> Self {
        let mut client = Self::with_options(super::MykoClientOptions {
            auto_reconnect: false,
            peer_failover: false,
            app_ping: false,
        });
        client.handler_connector = Some(connector);
        client
    }

    /// Route subsequent durable handler watches through another node.
    #[must_use]
    pub fn at(mut self, destination: NodeId) -> Self {
        self.handler_connector = self
            .handler_connector
            .as_ref()
            .map(|connector| connector.at(destination));
        self
    }

    fn handler_connector(&self) -> Result<Arc<dyn HandlerConnector>, HandlerClientError> {
        self.handler_connector
            .clone()
            .ok_or(HandlerClientError::MissingConnector)
    }

    /// Open a typed durable query in one scope, optionally filtered by event origin.
    ///
    /// `None` reads the logical scope across origins. The connector selects the
    /// serving node independently; an origin filter does not route the request.
    ///
    /// # Errors
    ///
    /// Returns an error when the connector, protocol, or typed payload is invalid.
    pub async fn follow_query<Q>(
        &self,
        source_node: Option<NodeId>,
        scope_id: ScopeId,
        query: &Q,
    ) -> Result<NodeHandlerSubscription<Vec<Q::Item>>, HandlerClientError>
    where
        Q: QueryParams,
        Q::Item: hyphae::CellValue + DeserializeOwned,
    {
        NodeHandlerSubscription::connect(
            self.handler_connector()?,
            HandlerRequest {
                kind: myko_federation::HandlerKind::Query,
                handler_id: Q::query_id_static().to_string(),
                source_node,
                scope_id: Some(scope_id),
                params: serde_json::to_value(query)?,
            },
            true,
            apply_view_delta::<Q::Item, LogPosition>,
        )
        .await
    }

    /// Open a typed durable report.
    ///
    /// # Errors
    ///
    /// Returns an error when the connector, protocol, or typed payload is invalid.
    pub async fn follow_report<R>(
        &self,
        report: &R,
    ) -> Result<NodeHandlerSubscription<<R as ReportOutputType>::Output>, HandlerClientError>
    where
        R: ReportParams,
        <R as ReportOutputType>::Output: hyphae::CellValue,
    {
        let connector = self.handler_connector()?;
        let target = connector.target_node().await?;
        NodeHandlerSubscription::connect(
            connector,
            HandlerRequest {
                kind: myko_federation::HandlerKind::Report,
                handler_id: R::report_id_static().to_owned(),
                source_node: report.source_node(target),
                scope_id: report.scope_id(target),
                params: serde_json::to_value(report)?,
            },
            false,
            reject_view_delta::<<R as ReportOutputType>::Output, LogPosition>,
        )
        .await
    }

    /// Open a typed durable view.
    ///
    /// # Errors
    ///
    /// Returns an error when the connector, protocol, or typed payload is invalid.
    pub async fn follow_view<V>(
        &self,
        view: &V,
    ) -> Result<NodeHandlerSubscription<Vec<V::Item>>, HandlerClientError>
    where
        V: ViewParams,
        V::Item: DeserializeOwned,
    {
        let connector = self.handler_connector()?;
        let target = connector.target_node().await?;
        NodeHandlerSubscription::connect(
            connector,
            HandlerRequest {
                kind: myko_federation::HandlerKind::View,
                handler_id: V::view_id_static().to_string(),
                source_node: view.source_node(target),
                scope_id: view.scope_id(target),
                params: serde_json::to_value(view)?,
            },
            true,
            apply_view_delta::<V::Item, LogPosition>,
        )
        .await
    }

    /// Open a reconnecting reactive durable query with an optional origin filter.
    ///
    /// # Errors
    ///
    /// Returns an error when the initial subscription cannot be established.
    pub async fn follow_query_reactive<Q>(
        &self,
        source_node: Option<NodeId>,
        scope_id: ScopeId,
        query: &Q,
    ) -> Result<ReactiveViewSubscription<Q::Item>, HandlerClientError>
    where
        Q: QueryParams,
        Q::Item: hyphae::CellValue + DeserializeOwned,
    {
        drive_view(self.follow_query(source_node, scope_id, query).await?)
    }

    /// Open a reconnecting reactive durable report.
    ///
    /// # Errors
    ///
    /// Returns an error when the initial subscription cannot be established.
    pub async fn follow_report_reactive<R>(
        &self,
        report: &R,
    ) -> Result<ReactiveHandlerSubscription<<R as ReportOutputType>::Output>, HandlerClientError>
    where
        R: ReportParams,
        <R as ReportOutputType>::Output: hyphae::CellValue,
    {
        Ok(drive_handler(self.follow_report(report).await?))
    }

    /// Open a reconnecting identity-preserving durable view.
    ///
    /// # Errors
    ///
    /// Returns an error when the initial subscription cannot be established.
    pub async fn follow_view_reactive<V>(
        &self,
        view: &V,
    ) -> Result<ReactiveViewSubscription<V::Item>, HandlerClientError>
    where
        V: ViewParams,
        V::Item: DeserializeOwned,
    {
        drive_view(self.follow_view(view).await?)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod tests {
    use std::{collections::VecDeque, sync::Mutex};

    use myko_federation::{HandlerKind, SubscriptionLiveness};
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct TestRow {
        value: u64,
    }

    struct QueueConnection {
        frames: VecDeque<HandlerFrame>,
    }

    #[async_trait::async_trait]
    impl HandlerConnection for QueueConnection {
        async fn recv(&mut self) -> Result<HandlerFrame, HandlerClientError> {
            self.frames.pop_front().ok_or_else(|| {
                HandlerClientError::Transport("test handler stream ended".to_owned())
            })
        }
    }

    struct TestConnector {
        initial: HandlerFrame,
        frames: Mutex<Option<VecDeque<HandlerFrame>>>,
    }

    #[async_trait::async_trait]
    impl HandlerConnector for TestConnector {
        async fn target_node(&self) -> Result<NodeId, HandlerClientError> {
            Ok(NodeId::new())
        }

        async fn connect(
            &self,
            _request: HandlerRequest,
        ) -> Result<(HandlerFrame, Box<dyn HandlerConnection>), HandlerClientError> {
            let frames = self
                .frames
                .lock()
                .map_err(|_| HandlerClientError::Protocol("test lock poisoned".to_owned()))?
                .take()
                .unwrap_or_default();
            Ok((self.initial.clone(), Box::new(QueueConnection { frames })))
        }

        fn at(&self, _destination: NodeId) -> Arc<dyn HandlerConnector> {
            panic!("test connector does not route")
        }

        fn reconnect_policy(&self) -> ReconnectPolicy {
            ReconnectPolicy::default()
        }
    }

    fn request() -> HandlerRequest {
        HandlerRequest {
            kind: HandlerKind::View,
            handler_id: "test_rows".to_owned(),
            source_node: None,
            scope_id: None,
            params: serde_json::json!({}),
        }
    }

    #[tokio::test]
    async fn keyed_handler_applies_delta_without_reordering_existing_rows() {
        let connector: Arc<dyn HandlerConnector> = Arc::new(TestConnector {
            initial: HandlerFrame::State {
                revision: HandlerStreamRevision {
                    epoch: 7,
                    sequence: 0,
                },
                state: ErasedHandlerState {
                    value: Some(serde_json::json!([
                        {"value": 1},
                        {"value": 2}
                    ])),
                    through: None,
                    liveness: SubscriptionLiveness::Current,
                    row_keys: Some(vec!["b".to_owned(), "a".to_owned()]),
                },
            },
            frames: Mutex::new(Some(VecDeque::from([HandlerFrame::ViewDelta {
                revision: HandlerStreamRevision {
                    epoch: 7,
                    sequence: 1,
                },
                delta: ErasedViewDelta {
                    upserts: vec![myko_wire::ErasedKeyedValue {
                        key: "a".to_owned(),
                        value: serde_json::json!({"value": 3}),
                    }],
                    deletes: Vec::new(),
                    order: None,
                    through: None,
                    liveness: SubscriptionLiveness::Current,
                },
            }]))),
        });
        let mut subscription = NodeHandlerSubscription::connect(
            connector,
            request(),
            true,
            apply_view_delta::<TestRow, LogPosition>,
        )
        .await
        .expect("open keyed handler");

        let state = subscription.recv().await.expect("apply keyed delta");

        assert_eq!(
            subscription.row_keys(),
            Some(["b".to_owned(), "a".to_owned()].as_slice())
        );
        assert_eq!(
            state.value,
            Some(vec![TestRow { value: 1 }, TestRow { value: 3 }])
        );
    }

    #[tokio::test]
    async fn handler_rejects_revision_gaps_before_mutating_state() {
        let initial_state = ErasedHandlerState {
            value: Some(serde_json::json!({"value": 1})),
            through: None,
            liveness: SubscriptionLiveness::Current,
            row_keys: None,
        };
        let connector: Arc<dyn HandlerConnector> = Arc::new(TestConnector {
            initial: HandlerFrame::State {
                revision: HandlerStreamRevision {
                    epoch: 2,
                    sequence: 0,
                },
                state: initial_state,
            },
            frames: Mutex::new(Some(VecDeque::from([HandlerFrame::State {
                revision: HandlerStreamRevision {
                    epoch: 2,
                    sequence: 2,
                },
                state: ErasedHandlerState {
                    value: Some(serde_json::json!({"value": 9})),
                    through: None,
                    liveness: SubscriptionLiveness::Current,
                    row_keys: None,
                },
            }]))),
        });
        let mut subscription = NodeHandlerSubscription::connect(
            connector,
            request(),
            false,
            reject_view_delta::<TestRow, LogPosition>,
        )
        .await
        .expect("open scalar handler");

        let error = subscription.recv().await.expect_err("reject sequence gap");

        assert!(error.to_string().contains("expected 2:1, received 2:2"));
        assert_eq!(subscription.current().value, Some(TestRow { value: 1 }));
    }

    #[tokio::test]
    async fn handler_accepts_a_fresh_epoch_state_as_resynchronization() {
        let connector: Arc<dyn HandlerConnector> = Arc::new(TestConnector {
            initial: HandlerFrame::State {
                revision: HandlerStreamRevision {
                    epoch: 2,
                    sequence: 0,
                },
                state: ErasedHandlerState {
                    value: Some(serde_json::json!({"value": 1})),
                    through: None,
                    liveness: SubscriptionLiveness::Current,
                    row_keys: None,
                },
            },
            frames: Mutex::new(Some(VecDeque::from([HandlerFrame::State {
                revision: HandlerStreamRevision {
                    epoch: 3,
                    sequence: 0,
                },
                state: ErasedHandlerState {
                    value: Some(serde_json::json!({"value": 9})),
                    through: None,
                    liveness: SubscriptionLiveness::Current,
                    row_keys: None,
                },
            }]))),
        });
        let mut subscription = NodeHandlerSubscription::connect(
            connector,
            request(),
            false,
            reject_view_delta::<TestRow, LogPosition>,
        )
        .await
        .expect("open scalar handler");

        let state = subscription.recv().await.expect("resynchronize handler");

        assert_eq!(state.value, Some(TestRow { value: 9 }));
        assert_eq!(subscription.revision.epoch, 3);
        assert_eq!(subscription.revision.sequence, 0);
    }
}
