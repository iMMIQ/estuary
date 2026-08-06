use std::time::Duration;

use axum::response::sse::Event as AxumEvent;
use eventsource_stream::Event as ParsedEvent;
use serde_json::Value;

#[cfg(test)]
use crate::error::GatewayError;

#[derive(Clone, Debug)]
pub(crate) struct Event {
    name: Option<String>,
    data: String,
    id: Option<String>,
    retry: Option<Duration>,
}

impl Event {
    pub(crate) fn json(name: impl Into<String>, value: &Value) -> Self {
        Self {
            name: Some(name.into()),
            data: serde_json::to_string(&value).expect("serializing a JSON value cannot fail"),
            id: None,
            retry: None,
        }
    }

    pub(crate) fn from_parsed(event: ParsedEvent) -> Self {
        Self {
            name: (event.event != "message").then_some(event.event),
            data: event.data,
            id: (!event.id.is_empty()).then_some(event.id),
            retry: event.retry,
        }
    }

    pub(crate) fn data(&self) -> &str {
        &self.data
    }

    pub(crate) fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub(crate) fn set_data(&mut self, data: String) {
        self.data = data;
    }

    pub(crate) fn set_name(&mut self, name: String) {
        self.name = Some(name);
    }

    pub(crate) fn into_axum(self) -> AxumEvent {
        let mut event = AxumEvent::default();
        if let Some(name) = self.name {
            event = event.event(name);
        }
        if let Some(id) = self.id {
            event = event.id(id);
        }
        if let Some(retry) = self.retry {
            event = event.retry(retry);
        }
        event.data(self.data)
    }
}

#[cfg(test)]
pub(crate) async fn parse_chunks(chunks: Vec<bytes::Bytes>) -> Result<Vec<Event>, GatewayError> {
    use eventsource_stream::Eventsource;
    use futures_util::{StreamExt, stream};

    let mut source =
        stream::iter(chunks.into_iter().map(Ok::<_, std::convert::Infallible>)).eventsource();
    let mut events = Vec::new();
    while let Some(event) = source.next().await {
        events.push(Event::from_parsed(
            event.map_err(|_| GatewayError::InvalidUpstreamResponse)?,
        ));
    }
    Ok(events)
}

#[cfg(test)]
pub(crate) async fn encode(events: Vec<Event>) -> bytes::Bytes {
    use std::convert::Infallible;

    use axum::{
        body::to_bytes,
        response::{IntoResponse, Sse},
    };
    use futures_util::stream;

    let stream = stream::iter(
        events
            .into_iter()
            .map(|event| Ok::<_, Infallible>(event.into_axum())),
    );
    to_bytes(Sse::new(stream).into_response().into_body(), usize::MAX)
        .await
        .expect("test SSE body")
}
