use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    query::{QueryResponse, QueryWindow},
    shared::value_with_tx,
};
use crate::{
    TS,
    core::view::{ViewId, ViewItemType},
};

/// A positive, finite view delivery rate in updates per second.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, TS)]
#[serde(try_from = "f64", into = "f64")]
#[ts(type = "number")]
pub struct ViewSampleRate(f64);

impl ViewSampleRate {
    #[must_use]
    pub fn interval(self) -> std::time::Duration {
        // Construction validates that the reciprocal is representable.
        std::time::Duration::from_secs_f64(1.0 / self.0)
    }

    #[must_use]
    pub const fn hz(self) -> f64 {
        self.0
    }
}

impl TryFrom<f64> for ViewSampleRate {
    type Error = &'static str;

    fn try_from(hz: f64) -> Result<Self, Self::Error> {
        if !hz.is_finite() || hz <= 0.0 {
            return Err("sampleRate must be positive and finite");
        }
        match std::time::Duration::try_from_secs_f64(1.0 / hz) {
            Ok(interval) if !interval.is_zero() => Ok(Self(hz)),
            _ => Err("sampleRate interval is outside clock resolution"),
        }
    }
}

impl From<ViewSampleRate> for f64 {
    fn from(rate: ViewSampleRate) -> Self {
        rate.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ViewSampleRateUpdate {
    pub tx: String,
    /// None restores immediate delivery without replacing the subscription.
    pub sample_rate: Option<ViewSampleRate>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ViewWindowUpdate {
    pub tx: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<QueryWindow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct WrappedView {
    pub view: Value,
    pub view_id: Arc<str>,
    pub view_item_type: Arc<str>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<QueryWindow>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_rate: Option<ViewSampleRate>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ViewError {
    pub tx: String,
    pub view_id: String,
    pub message: String,
}

impl ViewError {
    pub fn new(
        tx: impl Into<String>,
        view_id: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            tx: tx.into(),
            view_id: view_id.into(),
            message: message.into(),
        }
    }
}

pub type ViewResponse = QueryResponse;

///
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub fn wrap_view<V: ViewId + ViewItemType + Serialize + Clone>(
    tx: Arc<str>,
    view: &V,
) -> Result<WrappedView, serde_json::Error> {
    Ok(WrappedView {
        view: value_with_tx(tx, view)?,
        view_id: view.view_id(),
        view_item_type: view.view_item_type(),
        window: None,
        sample_rate: None,
    })
}

#[cfg(test)]
mod sampling_tests {
    use super::*;

    #[test]
    fn sample_rate_rejects_invalid_numbers_and_roundtrips_fractional_fps() {
        for rate in [
            0.0,
            -1.0,
            f64::NAN,
            f64::INFINITY,
            f64::MIN_POSITIVE,
            f64::MAX,
        ] {
            assert!(ViewSampleRate::try_from(rate).is_err());
        }
        let rate = ViewSampleRate::try_from(29.97).unwrap();
        let json = serde_json::to_string(&rate).unwrap();
        assert_eq!(serde_json::from_str::<ViewSampleRate>(&json).unwrap(), rate);
        assert!(rate.interval().as_secs_f64() > 1.0 / 30.0);
    }
}
