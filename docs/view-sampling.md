# View delivery sample rate

A view subscription can set `sampleRate` in updates per second. Rates can be fractional, such as 29.97. Omit the setting, or use null, for immediate delivery. Rates must be positive and finite, with a representable, nonzero clock interval.

The setting belongs to the subscription envelope, alongside `viewId`, `viewItemType`, and `window`:

```json
{
  "event": "ws:m:view",
  "data": {
    "viewId": "MyView",
    "viewItemType": "MyRow",
    "view": { "tx": "subscription-1" },
    "sampleRate": 30
  }
}
```

Change an existing subscription's rate without cancelling it or clearing its cache:

```json
{
  "event": "ws:m:view-sample-rate",
  "data": { "tx": "subscription-1", "sampleRate": 60 }
}
```

Initial and reset snapshots send immediately with sequence zero. Subsequent updates retain the latest operation for each changed row until the next delivery slot. A delete followed by an upsert retains the upsert; an upsert followed by a delete retains the delete. Window order and counts reflect the latest state. Emitted payloads have contiguous sequence numbers, regardless of how many source updates were combined. Cancelling the view discards pending changes.

Sampling applies to view delivery before serialization. It does not limit reactive computation upstream, and it does not sample commands, reports, queries, events, or trigger presses. Sampled views can lag command completion by the selected interval; callers needing immediate read-your-writes should leave sampling disabled.

## Rust

```rust,ignore
use myko::{view::ViewRequest, wire::ViewSampleRate};

let watch = client.watch_view_map_state(
    ViewRequest::new(MyView {})
        .with_sample_rate(Some(ViewSampleRate::try_from(30.0)?)),
);
watch.set_sample_rate(Some(ViewSampleRate::try_from(60.0)?))?;
watch.set_sample_rate(None)?;
```

The map watch retains its latest rate across reconnects. Identical view parameters and initial rates share a client subscription; changing its rate affects every shared handle. Delivery settings do not alter the server's cached view parameter key.

Executors can derive the rate from their configured render target FPS. Update the rate when that setting changes, rather than cancelling and recreating the subscription.
