use axum::response::sse::Event as AxumEvent;
use serde_json::Value;

#[derive(Clone, Debug)]
pub(crate) struct Event {
    name: Option<&'static str>,
    data: String,
}

impl Event {
    pub(crate) fn new(name: &'static str, data: String) -> Self {
        Self {
            name: (!name.is_empty()).then_some(name),
            data,
        }
    }

    pub(crate) fn json(name: &'static str, value: &Value) -> Self {
        Self::new(
            name,
            serde_json::to_string(value).expect("serializing a JSON value cannot fail"),
        )
    }

    #[cfg(test)]
    pub(crate) fn data(&self) -> &str {
        &self.data
    }

    pub(crate) fn into_axum(self) -> AxumEvent {
        let mut event = AxumEvent::default();
        if let Some(name) = self.name {
            event = event.event(name);
        }
        event.data(self.data)
    }
}

/// Bounded incremental SSE `data:` frame parser supporting `\n`, `\r`, and `\r\n`
/// line endings as well as multi-line `data:` fields.
#[derive(Debug)]
pub(crate) struct SseParser {
    max_bytes: usize,
    line: Vec<u8>,
    data: Vec<u8>,
    event_bytes: usize,
    line_nonempty: bool,
    oversized: bool,
    previous_cr: bool,
}

impl SseParser {
    pub(crate) fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            line: Vec::new(),
            data: Vec::new(),
            event_bytes: 0,
            line_nonempty: false,
            oversized: false,
            previous_cr: false,
        }
    }

    /// Feed a chunk of raw bytes. Invokes `on_event` with `Ok(data)` for each
    /// completed non-empty `data:` event, or `Err(())` the first time an event
    /// exceeds `max_bytes` (continuing to scan until the next blank line resets
    /// the parser).
    pub(crate) fn push(&mut self, bytes: &[u8], mut on_event: impl FnMut(Result<&[u8], ()>)) {
        for &byte in bytes {
            if byte == b'\n' && self.previous_cr {
                self.previous_cr = false;
                continue;
            }
            self.previous_cr = byte == b'\r';
            if byte == b'\r' || byte == b'\n' {
                self.finish_line(&mut on_event);
            } else {
                self.line_nonempty = true;
                self.event_bytes = self.event_bytes.saturating_add(1);
                if self.event_bytes > self.max_bytes {
                    if !self.oversized {
                        self.oversized = true;
                        on_event(Err(()));
                    }
                } else {
                    self.line.push(byte);
                }
            }
        }
    }

    /// Flush any trailing non-empty `data:` lines at EOF (used for best-effort
    /// inspection of truncated streams).
    pub(crate) fn finish(&mut self, mut on_event: impl FnMut(Result<&[u8], ()>)) {
        if self.line_nonempty {
            self.finish_line(&mut on_event);
        }
        if !self.oversized && !self.data.is_empty() {
            let data = std::mem::take(&mut self.data);
            on_event(Ok(&data));
            self.data = data;
        }
        self.data.clear();
        self.event_bytes = 0;
        self.oversized = false;
    }

    fn finish_line(&mut self, on_event: &mut impl FnMut(Result<&[u8], ()>)) {
        if !self.line_nonempty {
            if !self.oversized && !self.data.is_empty() {
                let data = std::mem::take(&mut self.data);
                on_event(Ok(&data));
                self.data = data;
            }
            self.data.clear();
            self.event_bytes = 0;
            self.oversized = false;
        } else if !self.oversized
            && let Some(payload) = self.line.strip_prefix(b"data:")
        {
            if !self.data.is_empty() {
                self.data.push(b'\n');
            }
            self.data
                .extend_from_slice(payload.strip_prefix(b" ").unwrap_or(payload));
        }
        self.line.clear();
        self.line_nonempty = false;
    }

    #[cfg(test)]
    pub(crate) fn buffered_bytes(&self) -> usize {
        self.line.len() + self.data.len()
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_rejects_oversized_events_and_resets_across_all_line_endings() {
        let mut parser = SseParser::new(8);
        let mut overflowed = false;
        parser.push(b"data: too large\n\n", |event| {
            if event.is_err() {
                overflowed = true;
            }
        });
        assert!(overflowed);

        for chunks in [
            [b"data: 1\n".as_slice(), b"\ndata: 2\n\n".as_slice()],
            [b"data: 1\r\n\r".as_slice(), b"\ndata: 2\r\n\r\n".as_slice()],
            [b"data: 1\r".as_slice(), b"\rdata: 2\r\r".as_slice()],
        ] {
            let mut parser = SseParser::new(8);
            let mut events = Vec::new();
            for chunk in chunks {
                parser.push(chunk, |event| {
                    events.push(String::from_utf8(event.unwrap().to_vec()).unwrap());
                });
            }
            assert_eq!(events, ["1", "2"]);
        }
    }
}
