//! Transport-neutral client side of retained durable handlers.

use std::{future::Future, sync::Arc};

use myko_federation::{
    AuthorityUnavailable, AuthorizationBlock, LiveCollection, LiveCollectionHandle,
    LiveCollectionState, LiveCollectionWriter, LiveSubscription, LiveSubscriptionHandle,
    LiveSubscriptionState, LiveSubscriptionWriter, LogPosition, NodeId, ReconnectPolicy, ScopeId,
    SubscriptionInterruption, SubscriptionLiveness, live_collection, live_subscription,
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
    #[error("durable handler authorization decision: {}", .0.public_message())]
    Authorization(Box<myko_federation::AuthorizationDecision>),
    #[error("authority unavailable: {0}")]
    AuthorityUnavailable(AuthorityUnavailable),
    #[error("durable handler value decoding failed: {0}")]
    Decode(#[from] serde_json::Error),
}

impl HandlerClientError {
    fn interruption(&self) -> Option<SubscriptionInterruption> {
        match self {
            Self::Transport(_) | Self::AuthorityUnavailable(_) => {
                Some(SubscriptionInterruption::Resynchronizing {
                    reason: self.to_string(),
                })
            }
            Self::Authorization(decision) => AuthorizationBlock::from_decision(*decision.clone())
                .map(|block| SubscriptionInterruption::AuthorizationBlocked { block }),
            Self::MissingConnector | Self::Protocol(_) | Self::Decode(_) => None,
        }
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
    /// Returns typed authorization failures, transport loss, sequence gaps, or invalid data.
    /// A denial or challenge clears the retained value before returning the error.
    pub async fn recv(&mut self) -> Result<LiveSubscriptionState<T, C>, HandlerClientError> {
        let frame = match self.connection.recv().await {
            Ok(frame) => frame,
            Err(error) => {
                if let Some(SubscriptionInterruption::AuthorizationBlocked { block }) =
                    error.interruption()
                {
                    self.current = LiveSubscriptionState {
                        value: None,
                        through: None,
                        liveness: SubscriptionLiveness::AuthorizationBlocked { block },
                    };
                    self.row_keys = self.keyed.then(Vec::new);
                }
                return Err(error);
            }
        };
        match frame {
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
    if matches!(
        state.liveness,
        SubscriptionLiveness::AuthorizationBlocked { .. }
    ) {
        return Ok((
            LiveSubscriptionState {
                value: None,
                through: None,
                liveness: state.liveness,
            },
            state.row_keys.map(|_| Vec::new()),
        ));
    }
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
    if matches!(
        delta.liveness,
        SubscriptionLiveness::AuthorizationBlocked { .. }
    ) {
        *current = LiveSubscriptionState {
            value: None,
            through: None,
            liveness: delta.liveness,
        };
        *row_keys = Some(Vec::new());
        return Ok(());
    }
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

fn publish_handler_state<T, C>(
    writer: &LiveSubscriptionWriter<T, C>,
    state: LiveSubscriptionState<T, C>,
) where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    match &state.liveness {
        SubscriptionLiveness::Connecting => {
            writer.resynchronizing("handler is establishing its snapshot");
        }
        SubscriptionLiveness::Resynchronizing { reason } => {
            writer.resynchronizing(reason.clone());
        }
        SubscriptionLiveness::AuthorizationBlocked { block } => {
            writer.interrupt(SubscriptionInterruption::AuthorizationBlocked {
                block: block.clone(),
            });
        }
        SubscriptionLiveness::Invalid { reason } => writer.invalidate(reason.clone()),
        SubscriptionLiveness::Current => {
            writer.replace(state);
        }
    }
}

struct PreparedHandlerOpen<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    connector: Arc<dyn HandlerConnector>,
    request: HandlerRequest,
    keyed: bool,
    apply_delta: ApplyDelta<T, C>,
}

impl<T, C> PreparedHandlerOpen<T, C>
where
    T: hyphae::CellValue + DeserializeOwned,
    C: hyphae::CellValue + DeserializeOwned,
{
    async fn connect(&self) -> Result<NodeHandlerSubscription<T, C>, HandlerClientError> {
        NodeHandlerSubscription::connect(
            Arc::clone(&self.connector),
            self.request.clone(),
            self.keyed,
            self.apply_delta,
        )
        .await
    }
}

async fn retry_initial<Output, Opening>(
    open: impl Fn() -> Opening,
    policy: ReconnectPolicy,
    on_retry: impl Fn(SubscriptionInterruption),
) -> Result<Output, HandlerClientError>
where
    Opening: Future<Output = Result<Output, HandlerClientError>>,
{
    let mut delay = policy.initial_delay();
    loop {
        match open().await {
            Ok(output) => return Ok(output),
            Err(error) => {
                let Some(interruption) = error.interruption() else {
                    return Err(error);
                };
                on_retry(interruption);
                tokio::time::sleep(delay).await;
                delay = policy.next_delay(delay);
            }
        }
    }
}

fn drive_handler<T, C, Preparing>(
    prepare: impl Fn() -> Preparing + Send + 'static,
    policy: ReconnectPolicy,
) -> ReactiveHandlerSubscription<T, C>
where
    T: hyphae::CellValue + DeserializeOwned,
    C: hyphae::CellValue + DeserializeOwned,
    Preparing: Future<Output = Result<PreparedHandlerOpen<T, C>, HandlerClientError>> + Send,
{
    let (writer, live) = live_subscription(LiveSubscriptionState {
        value: None,
        through: None,
        liveness: SubscriptionLiveness::Connecting,
    });
    let task_writer = writer.clone();
    let task = tokio::spawn(async move {
        let prepared = match retry_initial(prepare, policy, |interruption| {
            task_writer.interrupt(interruption);
        })
        .await
        {
            Ok(prepared) => prepared,
            Err(error) => {
                task_writer.invalidate(error.to_string());
                return;
            }
        };
        let mut subscription = match retry_initial(
            || prepared.connect(),
            policy,
            |interruption| {
                task_writer.interrupt(interruption);
            },
        )
        .await
        {
            Ok(subscription) => subscription,
            Err(error) => {
                task_writer.invalidate(error.to_string());
                return;
            }
        };
        drop(prepared);
        publish_handler_state(&task_writer, subscription.current.clone());
        loop {
            match subscription.recv().await {
                Ok(state) => {
                    publish_handler_state(&task_writer, state);
                    continue;
                }
                Err(error) => {
                    let Some(interruption) = error.interruption() else {
                        task_writer.invalidate(error.to_string());
                        return;
                    };
                    task_writer.interrupt(interruption);
                }
            }
            let mut delay = subscription.connector.reconnect_policy().initial_delay();
            loop {
                tokio::time::sleep(delay).await;
                match subscription.reconnect().await {
                    Ok(next) => {
                        publish_handler_state(&task_writer, next.current.clone());
                        subscription = next;
                        break;
                    }
                    Err(error) => {
                        let Some(interruption) = error.interruption() else {
                            task_writer.invalidate(error.to_string());
                            return;
                        };
                        task_writer.interrupt(interruption);
                        delay = subscription.connector.reconnect_policy().next_delay(delay);
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

fn publish_view_state<T, C>(
    writer: &LiveCollectionWriter<T, C>,
    subscription: &NodeHandlerSubscription<Vec<T>, C>,
) -> Result<(), String>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    match &subscription.current.liveness {
        SubscriptionLiveness::Current => writer
            .reconcile(
                keyed_rows(subscription).map_err(|error| error.to_string())?,
                subscription.current.through.clone(),
            )
            .map_err(|error| error.to_string())?,
        SubscriptionLiveness::Connecting => {
            writer.resynchronizing("handler is establishing its snapshot");
        }
        SubscriptionLiveness::Resynchronizing { reason } => {
            writer.resynchronizing(reason.clone());
        }
        SubscriptionLiveness::Invalid { reason } => return Err(reason.clone()),
        SubscriptionLiveness::AuthorizationBlocked { block } => {
            writer.interrupt(SubscriptionInterruption::AuthorizationBlocked {
                block: block.clone(),
            });
        }
    }
    Ok(())
}

fn drive_view<T, C, Preparing>(
    prepare: impl Fn() -> Preparing + Send + 'static,
    policy: ReconnectPolicy,
) -> ReactiveViewSubscription<T, C>
where
    T: hyphae::CellValue + DeserializeOwned,
    C: hyphae::CellValue + DeserializeOwned,
    Preparing: Future<Output = Result<PreparedHandlerOpen<Vec<T>, C>, HandlerClientError>> + Send,
{
    let (writer, live) = live_collection(
        Vec::new(),
        LiveCollectionState {
            through: None,
            liveness: SubscriptionLiveness::Connecting,
        },
    );
    let task_writer = writer.clone();
    let task = tokio::spawn(async move {
        let prepared = match retry_initial(prepare, policy, |interruption| {
            task_writer.interrupt(interruption);
        })
        .await
        {
            Ok(prepared) => prepared,
            Err(error) => {
                task_writer.invalidate(error.to_string());
                return;
            }
        };
        let mut subscription = match retry_initial(
            || prepared.connect(),
            policy,
            |interruption| {
                task_writer.interrupt(interruption);
            },
        )
        .await
        {
            Ok(subscription) => subscription,
            Err(error) => {
                task_writer.invalidate(error.to_string());
                return;
            }
        };
        drop(prepared);
        if let Err(reason) = publish_view_state(&task_writer, &subscription) {
            task_writer.invalidate(reason);
            return;
        }
        loop {
            match subscription.recv().await {
                Ok(_) => {
                    if let Err(reason) = publish_view_state(&task_writer, &subscription) {
                        task_writer.invalidate(reason);
                        return;
                    }
                }
                Err(error) => {
                    let Some(interruption) = error.interruption() else {
                        task_writer.invalidate(error.to_string());
                        return;
                    };
                    task_writer.interrupt(interruption);
                    let mut delay = subscription.connector.reconnect_policy().initial_delay();
                    loop {
                        tokio::time::sleep(delay).await;
                        match subscription.reconnect().await {
                            Ok(next) => {
                                subscription = next;
                                if let Err(reason) = publish_view_state(&task_writer, &subscription)
                                {
                                    task_writer.invalidate(reason);
                                    return;
                                }
                                break;
                            }
                            Err(error) => {
                                let Some(interruption) = error.interruption() else {
                                    task_writer.invalidate(error.to_string());
                                    return;
                                };
                                task_writer.interrupt(interruption);
                                delay = subscription.connector.reconnect_policy().next_delay(delay);
                            }
                        }
                    }
                }
            }
        }
    });
    ReactiveViewSubscription { live, writer, task }
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

    fn prepare_query<Q>(
        &self,
        source_node: Option<NodeId>,
        scope_id: ScopeId,
        query: &Q,
    ) -> Result<PreparedHandlerOpen<Vec<Q::Item>, LogPosition>, HandlerClientError>
    where
        Q: QueryParams,
        Q::Item: hyphae::CellValue + DeserializeOwned,
    {
        Ok(PreparedHandlerOpen {
            connector: self.handler_connector()?,
            request: HandlerRequest {
                kind: myko_federation::HandlerKind::Query,
                service_id: <Q as crate::query::QueryIdStatic>::SERVICE_ID
                    .map(|service| myko_federation::ServiceId::new(service.as_str())),
                handler_id: Q::query_id_static().to_string(),
                source_node,
                scope_id: Some(scope_id),
                params: serde_json::to_value(query)?,
            },
            keyed: true,
            apply_delta: apply_view_delta::<Q::Item, LogPosition>,
        })
    }

    async fn prepare_report<R>(
        &self,
        report: &R,
    ) -> Result<PreparedHandlerOpen<<R as ReportOutputType>::Output, LogPosition>, HandlerClientError>
    where
        R: ReportParams,
        <R as ReportOutputType>::Output: hyphae::CellValue,
    {
        let connector = self.handler_connector()?;
        let target = connector.target_node().await?;
        Ok(PreparedHandlerOpen {
            connector,
            request: HandlerRequest {
                kind: myko_federation::HandlerKind::Report,
                service_id: <R as crate::report::ReportIdStatic>::SERVICE_ID
                    .map(|service| myko_federation::ServiceId::new(service.as_str())),
                handler_id: R::report_id_static().to_owned(),
                source_node: report.source_node(target),
                scope_id: report.scope_id(target),
                params: serde_json::to_value(report)?,
            },
            keyed: false,
            apply_delta: reject_view_delta::<<R as ReportOutputType>::Output, LogPosition>,
        })
    }

    async fn prepare_view<V>(
        &self,
        view: &V,
    ) -> Result<PreparedHandlerOpen<Vec<V::Item>, LogPosition>, HandlerClientError>
    where
        V: ViewParams,
        V::Item: DeserializeOwned,
    {
        let connector = self.handler_connector()?;
        let target = connector.target_node().await?;
        Ok(PreparedHandlerOpen {
            connector,
            request: HandlerRequest {
                kind: myko_federation::HandlerKind::View,
                service_id: <V as crate::view::ViewIdStatic>::SERVICE_ID
                    .map(|service| myko_federation::ServiceId::new(service.as_str())),
                handler_id: V::view_id_static().to_string(),
                source_node: view.source_node(target),
                scope_id: view.scope_id(target),
                params: serde_json::to_value(view)?,
            },
            keyed: true,
            apply_delta: apply_view_delta::<V::Item, LogPosition>,
        })
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
        self.prepare_query(source_node, scope_id, query)?
            .connect()
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
        self.prepare_report(report).await?.connect().await
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
        self.prepare_view(view).await?.connect().await
    }

    /// Open a reconnecting reactive durable query with an optional origin filter.
    /// The handle starts connecting and retries if its initial server is unavailable.
    /// Denial or challenge clears protected rows and retries on the same handle.
    /// Protocol and decoding failures invalidate the handle.
    ///
    /// # Errors
    ///
    /// Returns an error when this client has no durable handler connector.
    pub fn follow_query_reactive<Q>(
        &self,
        source_node: Option<NodeId>,
        scope_id: ScopeId,
        query: &Q,
    ) -> Result<ReactiveViewSubscription<Q::Item>, HandlerClientError>
    where
        Q: QueryParams,
        Q::Item: hyphae::CellValue + DeserializeOwned,
    {
        let policy = self.handler_connector()?.reconnect_policy();
        let client = self.clone();
        let query = query.clone();
        Ok(drive_view(
            move || {
                let client = client.clone();
                let query = query.clone();
                let scope_id = scope_id.clone();
                std::future::ready(client.prepare_query(source_node, scope_id, &query))
            },
            policy,
        ))
    }

    /// Open a reconnecting reactive durable report.
    /// The handle owns initial target resolution and connection retries.
    /// Once resolved, data selectors remain fixed across every connection attempt.
    /// Denial or challenge clears the value and retries on the same handle.
    /// Protocol and decoding failures invalidate the handle.
    ///
    /// # Errors
    ///
    /// Returns an error when this client has no durable handler connector.
    pub fn follow_report_reactive<R>(
        &self,
        report: &R,
    ) -> Result<ReactiveHandlerSubscription<<R as ReportOutputType>::Output>, HandlerClientError>
    where
        R: ReportParams,
        <R as ReportOutputType>::Output: hyphae::CellValue,
    {
        let policy = self.handler_connector()?.reconnect_policy();
        let client = self.clone();
        let report = report.clone();
        Ok(drive_handler(
            move || {
                let client = client.clone();
                let report = report.clone();
                async move { client.prepare_report(&report).await }
            },
            policy,
        ))
    }

    /// Open a reconnecting identity-preserving durable view.
    /// The handle owns initial target resolution and connection retries.
    /// Once resolved, data selectors remain fixed across every connection attempt.
    /// Denial or challenge clears protected rows and retries on the same handle.
    /// Protocol and decoding failures invalidate the handle.
    ///
    /// # Errors
    ///
    /// Returns an error when this client has no durable handler connector.
    pub fn follow_view_reactive<V>(
        &self,
        view: &V,
    ) -> Result<ReactiveViewSubscription<V::Item>, HandlerClientError>
    where
        V: ViewParams,
        V::Item: DeserializeOwned,
    {
        let policy = self.handler_connector()?.reconnect_policy();
        let client = self.clone();
        let view = view.clone();
        Ok(drive_view(
            move || {
                let client = client.clone();
                let view = view.clone();
                async move { client.prepare_view(&view).await }
            },
            policy,
        ))
    }
}

#[cfg(test)]
mod authorization_tests;

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

    struct ChannelConnection(flume::Receiver<HandlerFrame>);

    #[async_trait::async_trait]
    impl HandlerConnection for ChannelConnection {
        async fn recv(&mut self) -> Result<HandlerFrame, HandlerClientError> {
            self.0
                .recv_async()
                .await
                .map_err(|error| HandlerClientError::Transport(error.to_string()))
        }
    }

    struct ReconnectingConnector {
        _serial: crate::test_util::SchedulerTestPermit,
        connections: Mutex<VecDeque<(HandlerFrame, ChannelConnection)>>,
    }

    #[crate::myko_report(u64)]
    struct StartupReport;

    impl crate::report::ReportHandler for StartupReport {
        type Output = u64;
    }

    mod selectors;

    #[tokio::test]
    async fn reactive_report_opened_before_server_recovers_without_a_new_handle() {
        let serial = crate::test_util::scheduler_test_serial();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let connector = Arc::new(ReconnectingConnector {
                _serial: serial,
                connections: Mutex::new(VecDeque::new()),
            });
            let client = MykoClient::with_handler_connector(connector.clone());
            let owner = client
                .follow_report_reactive(&StartupReport)
                .expect("return a live handle while the server is absent");
            let live = owner.live_subscription();
            let mut publications = live.watch_publications();
            loop {
                let state = publications
                    .recv_async()
                    .await
                    .expect("connection state")
                    .state;
                if matches!(state.liveness, SubscriptionLiveness::Resynchronizing { .. }) {
                    assert_eq!(state.value, None);
                    break;
                }
            }

            let (sender, receiver) = flume::unbounded();
            connector
                .connections
                .lock()
                .expect("test connection lock")
                .push_back((
                    HandlerFrame::State {
                        revision: HandlerStreamRevision {
                            epoch: 1,
                            sequence: 0,
                        },
                        state: ErasedHandlerState {
                            value: Some(serde_json::json!(7)),
                            through: None,
                            liveness: SubscriptionLiveness::Current,
                            row_keys: None,
                        },
                    },
                    ChannelConnection(receiver),
                ));
            loop {
                let state = publications
                    .recv_async()
                    .await
                    .expect("recovered report")
                    .state;
                if state.liveness == SubscriptionLiveness::Current {
                    assert_eq!(state.value, Some(7));
                    break;
                }
            }
            drop(owner);
            while !sender.is_disconnected() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("initial connection must recover");
    }

    #[async_trait::async_trait]
    impl HandlerConnector for ReconnectingConnector {
        async fn target_node(&self) -> Result<NodeId, HandlerClientError> {
            if self
                .connections
                .lock()
                .expect("test connection lock")
                .is_empty()
            {
                Err(HandlerClientError::Transport("server is absent".to_owned()))
            } else {
                Ok(NodeId::new())
            }
        }

        async fn connect(
            &self,
            _request: HandlerRequest,
        ) -> Result<(HandlerFrame, Box<dyn HandlerConnection>), HandlerClientError> {
            let (frame, connection) = self
                .connections
                .lock()
                .expect("test connection lock")
                .pop_front()
                .ok_or_else(|| HandlerClientError::Transport("no ready peer".to_owned()))?;
            Ok((frame, Box::new(connection)))
        }

        fn at(&self, _destination: NodeId) -> Arc<dyn HandlerConnector> {
            panic!("test connector does not route")
        }

        fn reconnect_policy(&self) -> ReconnectPolicy {
            ReconnectPolicy::default()
        }
    }

    fn view_state(
        epoch: u64,
        sequence: u64,
        value: u64,
        liveness: SubscriptionLiveness,
    ) -> HandlerFrame {
        HandlerFrame::State {
            revision: HandlerStreamRevision { epoch, sequence },
            state: ErasedHandlerState {
                value: Some(serde_json::json!([{"value": value}])),
                through: None,
                liveness,
                row_keys: Some(vec!["row".to_owned()]),
            },
        }
    }

    #[tokio::test]
    async fn reconnecting_report_retains_coherent_value_until_peer_is_current() {
        let serial = crate::test_util::scheduler_test_serial();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (first_sender, first_receiver) = flume::unbounded();
            let (next_sender, next_receiver) = flume::unbounded();
            let catching_up = SubscriptionLiveness::Resynchronizing {
                reason: "report dependencies catching up".to_owned(),
            };
            let connector: Arc<dyn HandlerConnector> = Arc::new(ReconnectingConnector {
                _serial: serial,
                connections: Mutex::new(VecDeque::from([
                    (
                        view_state(1, 0, 1, SubscriptionLiveness::Current),
                        ChannelConnection(first_receiver),
                    ),
                    (
                        view_state(2, 0, 2, catching_up.clone()),
                        ChannelConnection(next_receiver),
                    ),
                ])),
            });
            let mut request = request();
            request.kind = HandlerKind::Report;
            let owner = drive_handler(
                move || {
                    let connector = Arc::clone(&connector);
                    let request = request.clone();
                    async move {
                        Ok(PreparedHandlerOpen {
                            connector,
                            request,
                            keyed: false,
                            apply_delta: reject_view_delta::<Vec<TestRow>, LogPosition>,
                        })
                    }
                },
                ReconnectPolicy::default(),
            );
            let live = owner.live_subscription();
            let mut publications = live.watch_publications();
            loop {
                let state = publications
                    .recv_async()
                    .await
                    .expect("initial report")
                    .state;
                if state.liveness == SubscriptionLiveness::Current {
                    assert_eq!(state.value, Some(vec![TestRow { value: 1 }]));
                    break;
                }
            }
            drop(first_sender);

            loop {
                let state = publications.recv_async().await.expect("report state").state;
                if state.liveness == catching_up {
                    assert_eq!(state.value, Some(vec![TestRow { value: 1 }]));
                    break;
                }
            }

            next_sender
                .send(view_state(2, 1, 3, SubscriptionLiveness::Current))
                .expect("publish ready report");
            let ready = publications.recv_async().await.expect("ready report").state;
            assert_eq!(ready.liveness, SubscriptionLiveness::Current);
            assert_eq!(ready.value, Some(vec![TestRow { value: 3 }]));
        })
        .await
        .expect("report recovery must finish");
    }

    #[tokio::test]
    async fn reconnecting_view_retains_coherent_rows_until_peer_is_current() {
        let serial = crate::test_util::scheduler_test_serial();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (first_sender, first_receiver) = flume::unbounded();
            let (next_sender, next_receiver) = flume::unbounded();
            let catching_up = SubscriptionLiveness::Resynchronizing {
                reason: "scope catching up".to_owned(),
            };
            let connector: Arc<dyn HandlerConnector> = Arc::new(ReconnectingConnector {
                _serial: serial,
                connections: Mutex::new(VecDeque::from([
                    (
                        view_state(1, 0, 1, SubscriptionLiveness::Current),
                        ChannelConnection(first_receiver),
                    ),
                    (
                        view_state(2, 0, 2, catching_up.clone()),
                        ChannelConnection(next_receiver),
                    ),
                ])),
            });
            let owner = drive_view(
                move || {
                    let connector = Arc::clone(&connector);
                    async move {
                        Ok(PreparedHandlerOpen {
                            connector,
                            request: request(),
                            keyed: true,
                            apply_delta: apply_view_delta::<TestRow, LogPosition>,
                        })
                    }
                },
                ReconnectPolicy::default(),
            );
            let live = owner.live_collection();
            let revisions = live.subscribe_revisions();
            let initial = revisions
                .receiver()
                .recv_async()
                .await
                .expect("initial view");
            assert_eq!(initial.state.liveness, SubscriptionLiveness::Current);
            drop(first_sender);

            let lost = revisions.receiver().recv_async().await.expect("peer loss");
            assert!(matches!(
                lost.state.liveness,
                SubscriptionLiveness::Resynchronizing { .. }
            ));
            let reconnected = revisions
                .receiver()
                .recv_async()
                .await
                .expect("peer snapshot");
            assert_eq!(reconnected.state.liveness, catching_up);
            assert_eq!(
                live.rows().snapshot(),
                vec![(Arc::from("row"), Arc::new(TestRow { value: 1 }))]
            );

            next_sender
                .send(view_state(2, 1, 3, SubscriptionLiveness::Current))
                .expect("publish ready scope");
            let ready = revisions
                .receiver()
                .recv_async()
                .await
                .expect("ready snapshot");
            assert_eq!(ready.state.liveness, SubscriptionLiveness::Current);
            assert_eq!(
                live.rows().snapshot(),
                vec![(Arc::from("row"), Arc::new(TestRow { value: 3 }))]
            );
        })
        .await
        .expect("view recovery must finish");
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
            service_id: None,
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
