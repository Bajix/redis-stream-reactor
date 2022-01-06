use backoff::ExponentialBackoff;
use env_url::ServiceURL;
use futures::{stream, StreamExt, TryStreamExt};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use redis::{
  streams::{StreamId, StreamKey, StreamRangeReply, StreamReadOptions, StreamReadReply},
  AsyncCommands, ErrorKind, RedisError,
};
use redis_swapplex::{get_connection, RedisEnvService};
use serde::{de::Deserialize, ser::Serialize};
use std::{
  borrow::Cow,
  collections::BTreeMap,
  fmt::Debug,
  marker::PhantomData,
  num::{ParseFloatError, ParseIntError},
  str::Utf8Error,
  sync::Arc,
  time::{Duration, SystemTime},
};
use thiserror::Error;
use tokio::{signal, time::sleep, try_join};
use tokio_util::sync::CancellationToken;
pub enum DeliveryStatus {
  /// Stream entry newly delivered to consumer group
  NewDelivery,
  /// Stream entry was claimed via XAUTOCLAIM
  MinIdleElapsed,
}

#[derive(Debug, Error)]
pub enum ParseError {
  #[error(transparent)]
  Utf8(#[from] Utf8Error),
  #[error(transparent)]
  ParseFloat(#[from] ParseFloatError),
  #[error(transparent)]
  ParseInt(#[from] ParseIntError),
  #[error(transparent)]
  Serde(#[from] serde_json::Error),
  #[error(transparent)]
  Redis(#[from] RedisError),
}

impl From<ParseError> for RedisError {
  fn from(err: ParseError) -> Self {
    match err {
      ParseError::Utf8(err) => RedisError::from((
        redis::ErrorKind::TypeError,
        "Invalid utf8 value",
        err.to_string(),
      )),
      ParseError::ParseFloat(err) => RedisError::from((
        redis::ErrorKind::TypeError,
        "Invalid float value",
        err.to_string(),
      )),
      ParseError::ParseInt(err) => RedisError::from((
        redis::ErrorKind::TypeError,
        "Invalid integer value",
        err.to_string(),
      )),
      ParseError::Serde(err) => {
        RedisError::from((ErrorKind::TypeError, "Invalid JSON value", err.to_string()))
      }
      ParseError::Redis(err) => err,
    }
  }
}

#[derive(Debug, Error)]
pub enum TaskError<T: Send + Debug> {
  /// Bypass error handling and delete stream entry via XDEL
  #[error("Stream entry marked for deletion")]
  Delete,
  /// Skip stream entry acknowledgement
  #[error("Stream entry acknowledgment skipped")]
  SkipAcknowledgement,
  /// Pass through of original error
  #[error(transparent)]
  Error(#[from] T),
}
pub trait StreamEntry: Send + Sync + Serialize + for<'de> Deserialize<'de> {
  /// Deserializes from a stringified key value map but only if every field is FromStr. See [`serde_with::PickFirst<(_, serde_with::DisplayFromStr)>`](https://docs.rs/serde_with/1.11.0/serde_with/guide/serde_as_transformations/index.html#pick-first-successful-deserialization)
  fn from_stream_id(stream_id: &StreamId) -> Result<Self, ParseError> {
    let data = stream_id
      .map
      .iter()
      .map(|(key, value)| match value {
        redis::Value::Data(ref bytes) => std::str::from_utf8(&bytes[..])
          .map(|value| serde_json::Value::String(String::from(value)))
          .map_err(|err| err.into())
          .map(|value| (key.to_owned(), value)),
        _ => Err(
          RedisError::from((
            redis::ErrorKind::TypeError,
            "invalid response type for JSON",
          ))
          .into(),
        ),
      })
      .collect::<Result<Vec<(String, serde_json::Value)>, ParseError>>()?;

    let data = serde_json::map::Map::from_iter(data.into_iter());

    let data = serde_json::from_value(serde_json::Value::Object(data))?;

    Ok(data)
  }

  /// Serialize into a stringified field value mapping for XADD
  fn xadd_map(&self) -> Result<BTreeMap<String, String>, ParseError> {
    let value = serde_json::to_value(&self)?;

    let data: Vec<(String, String)> = value
      .as_object()
      .into_iter()
      .flat_map(|map| {
        map.into_iter().filter_map(|(key, value)| match value {
          serde_json::Value::Null => None,
          serde_json::Value::Bool(value) => Some(Ok((key.to_owned(), value.to_string()))),
          serde_json::Value::Number(value) => Some(Ok((key.to_owned(), value.to_string()))),
          serde_json::Value::String(value) => Some(Ok((key.to_owned(), value.to_owned()))),

          serde_json::Value::Array(value) => {
            Some(serde_json::to_string(value).map(|value| (key.to_owned(), value)))
          }

          serde_json::Value::Object(value) => {
            Some(serde_json::to_string(value).map(|value| (key.to_owned(), value)))
          }
        })
      })
      .collect::<Result<Vec<(String, String)>, serde_json::Error>>()?;

    let data: BTreeMap<String, String> = BTreeMap::from_iter(data.into_iter());

    Ok(data)
  }
}

impl<T> StreamEntry for T where T: Send + Sync + Serialize + for<'de> Deserialize<'de> {}

pub trait ConsumerGroup {
  const GROUP_NAME: &'static str;
}

#[async_trait::async_trait]
pub trait StreamConsumer<T: StreamEntry, G: ConsumerGroup>: Default + Send + Sync {
  const XREAD_BLOCK_TIME: Duration = Duration::from_secs(60);
  const BATCH_SIZE: usize = 20;
  const CONCURRENCY: usize = 10;
  type Error: Send + Debug;
  type Data: Send + Sync;

  async fn process_event(
    &self,
    ctx: &Context<Self::Data>,
    event: &T,
    status: &DeliveryStatus,
  ) -> Result<(), TaskError<Self::Error>>;

  async fn process_event_stream(
    &self,
    ctx: &Context<Self::Data>,
    ids: Vec<StreamId>,
    status: &DeliveryStatus,
  ) -> Result<(), RedisError> {
    stream::iter(ids.into_iter())
      .map(Ok)
      .try_for_each_concurrent(Self::CONCURRENCY, |entry| async move {
        self.process_stream_entry(ctx, entry, status).await
      })
      .await?;

    Ok(())
  }

  async fn process_stream_entry(
    &self,
    ctx: &Context<Self::Data>,
    entry: StreamId,
    status: &DeliveryStatus,
  ) -> Result<(), RedisError> {
    let event = <T as StreamEntry>::from_stream_id(&entry)?;

    match self.process_event(ctx, &event, status).await {
      Ok(()) => {
        let mut conn = get_connection();

        let _: i64 = conn
          .xack(ctx.stream_key(), G::GROUP_NAME, &[&entry.id])
          .await?;
      }
      Err(err) => match err {
        TaskError::Delete => {
          let mut conn = get_connection();
          let _: i64 = conn.xdel(ctx.stream_key(), &[&entry.id]).await?;
        }
        TaskError::SkipAcknowledgement => (),
        TaskError::Error(err) => {
          log::error!("Error processing stream event: {:?}", err);
          if let Some(stream_key) = &ctx.error_stream_key {
            let mut conn = get_connection();
            let _: String = conn
              .xadd_map(
                stream_key.as_ref(),
                &entry.id,
                &event.xadd_map().map_err(|err| RedisError::from(err))?,
              )
              .await?;
          };
        }
      },
    }

    Ok(())
  }
}

pub struct Context<'a, T>
where
  T: Send + Sync,
{
  data: T,
  consumer_id: String,
  stream_key: Cow<'a, str>,
  error_stream_key: Option<Cow<'a, str>>,
  cancel_token: Arc<CancellationToken>,
}

impl<'a, T> Context<'a, T>
where
  T: Send + Sync,
{
  fn new(data: T, stream_key: Cow<'a, str>, error_stream_key: Option<Cow<'a, str>>) -> Self {
    Self {
      data,
      consumer_id: Self::generate_id(),
      stream_key,
      error_stream_key,
      cancel_token: Arc::new(CancellationToken::new()),
    }
  }

  pub fn data(&self) -> &T {
    &self.data
  }

  fn generate_id() -> String {
    thread_rng()
      .sample_iter(&Alphanumeric)
      .take(30)
      .map(char::from)
      .collect()
  }

  pub fn consumer_id(&self) -> &str {
    &self.consumer_id
  }
  pub fn stream_key(&self) -> &str {
    &self.stream_key
  }
  pub fn error_stream_key(&self) -> Option<&Cow<'a, str>> {
    self.error_stream_key.as_ref()
  }
  /// Process claimed stream entries and shutdown
  pub fn shutdown_gracefully(&self) {
    self.cancel_token.cancel();
  }
}

#[derive(Clone)]
/// The current state of the consumer group
pub enum ConsumerGroupState {
  Uninitialized,
  NewlyCreated,
  PreviouslyCreated,
}

/// [`EventReactor`] claim mode
pub enum ClaimMode {
  /// Process new entries while clearing the idle-pending backlog until none remain and max_idle has elapsed
  ClearBacklog {
    /// min-idle-time filter for XAUTOCLAIM, which transfers ownership to this consumer of messages pending for more than <min-idle-time>
    min_idle: Duration,
    /// max-idle time relative to reactor start time to await newly idle entries before proceeding to only claim new entries
    max_idle: Option<Duration>,
  },
  /// Process new entries while continuously autoclaiming entries from other consumers that have not been acknowledged within the span of min_idle
  Autoclaim {
    /// min-idle-time filter for XAUTOCLAIM, which transfers ownership to this consumer of messages pending for more than <min-idle-time>
    min_idle: Duration,
  },
  /// Only process new entries
  NewOnly,
}

/// Redis event stream reactor
pub struct EventReactor<'a, T, E, G>
where
  T: StreamConsumer<E, G>,
  E: StreamEntry,
  G: ConsumerGroup,
{
  group_status: ConsumerGroupState,
  consumer: Arc<T>,
  ctx: Context<'a, T::Data>,
  _marker: PhantomData<fn() -> (E, G)>,
}

impl<'a, T, E, G> EventReactor<'a, T, E, G>
where
  T: StreamConsumer<E, G>,
  E: StreamEntry,
  G: ConsumerGroup,
{
  pub fn new(
    data: T::Data,
    stream_key: Cow<'a, str>,
    error_stream_key: Option<Cow<'a, str>>,
  ) -> Self {
    EventReactor {
      group_status: ConsumerGroupState::Uninitialized,
      consumer: Arc::new(T::default()),
      ctx: Context::new(data, stream_key, error_stream_key),
      _marker: PhantomData,
    }
  }

  /// Manually initialize Redis consumer group; this will otherwise initialize automatically as needed and
  /// is useful for knowing if a consumer group was previously created
  pub async fn initialize_consumer_group(&mut self) -> Result<ConsumerGroupState, RedisError> {
    let url = RedisEnvService::service_url().map_err(|err| {
      RedisError::from((
        ErrorKind::InvalidClientConfig,
        "Invalid Redis connection URL",
        err.to_string(),
      ))
    })?;
    let client = redis::Client::open(url)?;
    let mut conn = client.get_async_connection().await?;

    let result: Result<(), RedisError> = if let Some(error_stream) = self.ctx.error_stream_key() {
      let mut pipe = redis::pipe();
      pipe
        .xgroup_create_mkstream(self.ctx.stream_key(), G::GROUP_NAME, "0")
        .ignore();
      pipe
        .xgroup_create_mkstream(error_stream.as_ref(), G::GROUP_NAME, "0")
        .ignore();

      pipe.query_async(&mut conn).await
    } else {
      conn
        .xgroup_create_mkstream(self.ctx.stream_key(), G::GROUP_NAME, "0")
        .await
        .map(|_: String| ())
    };

    match result {
      // It is expected behavior that this will fail when already initalized
      // Expected error: `BUSYGROUP: Consumer Group name already exists`
      Err(err) if err.code() == Some("BUSYGROUP") => {
        self.group_status = ConsumerGroupState::PreviouslyCreated;
        Ok(ConsumerGroupState::PreviouslyCreated)
      }
      Err(err) => Err(err),
      Ok(_) => {
        self.group_status = ConsumerGroupState::NewlyCreated;
        Ok(ConsumerGroupState::NewlyCreated)
      }
    }
  }

  async fn autoclaim_batch(
    &self,
    min_idle: &Duration,
    cursor: &str,
  ) -> Result<Option<(String, StreamRangeReply)>, RedisError> {
    if self.ctx.cancel_token.is_cancelled() {
      return Ok(None);
    }

    tokio::select! {
      reply = async {
        let mut conn = get_connection();

        let mut cmd = redis::cmd("XAUTOCLAIM");

        cmd.arg(self.ctx.stream_key())
          .arg(G::GROUP_NAME)
          .arg(&self.ctx.consumer_id())
          .arg(min_idle.as_millis() as usize)
          .arg(cursor)
          .arg("COUNT")
          .arg(T::BATCH_SIZE);

        let reply: (String, StreamRangeReply) = cmd.query_async(&mut conn).await?;

        Ok(Some(reply))
      } => reply,
      _ = self.ctx.cancel_token.cancelled() => Ok(None),
      _ = signal::ctrl_c() => Ok(None),
    }
  }

  async fn process_idle_pending(
    &self,
    min_idle: &Duration,
    max_idle: &Option<Duration>,
  ) -> Result<(), RedisError> {
    let mut cursor = String::from("0-0");

    let start_time = SystemTime::now();

    while let Some((next_cursor, reply)) = self.autoclaim_batch(&min_idle, cursor.as_str()).await? {
      cursor = next_cursor;

      let poll_time = SystemTime::now();

      if !reply.ids.is_empty() {
        self
          .consumer
          .process_event_stream(&self.ctx, reply.ids, &DeliveryStatus::MinIdleElapsed)
          .await?;
      } else if let Some(sleep_time) = min_idle.checked_div(4) {
        if let Some(max_idle) = max_idle {
          if let Ok(call_elapsed) = poll_time.duration_since(start_time) {
            if call_elapsed.gt(&max_idle) {
              continue;
            }
          }
        }

        tokio::select! {
            _ = self.ctx.cancel_token.cancelled() => {
              continue;
            }
            _ = sleep(sleep_time) => {}
        }
      }
    }

    Ok(())
  }

  async fn next_batch(&self) -> Result<Option<StreamReadReply>, RedisError> {
    if self.ctx.cancel_token.is_cancelled() {
      return Ok(None);
    }

    tokio::select! {
      reply = async {
        let mut conn = get_connection();

        let reply: StreamReadReply = conn
        .xread_options(
          &[self.ctx.stream_key()],
          &[">"],
          &StreamReadOptions::default()
            .group(G::GROUP_NAME, &self.ctx.consumer_id())
            .block(T::XREAD_BLOCK_TIME.as_millis() as usize)
            .count(T::BATCH_SIZE),
        )
        .await?;

        Ok(Some(reply))
      } => reply,
      _ = self.ctx.cancel_token.cancelled() => Ok(None),
      _ = signal::ctrl_c() => Ok(None)
    }
  }

  async fn process_stream(&self) -> Result<(), RedisError> {
    while let Some(reply) = self.next_batch().await? {
      if !reply.keys.is_empty() {
        let ids: Vec<StreamId> = reply
          .keys
          .into_iter()
          .flat_map(|key| {
            let StreamKey { ids, .. } = key;
            ids.into_iter()
          })
          .collect();

        self
          .consumer
          .process_event_stream(&self.ctx, ids, &DeliveryStatus::NewDelivery)
          .await?;
      }
    }

    Ok(())
  }

  /// Process idle-pending backlog without claiming new entries until none remain and max_idle has elapsed (relative to start_time)
  pub async fn clear_idle_backlog(
    &mut self,
    min_idle: &Duration,
    max_idle: &Option<Duration>,
  ) -> Result<(), RedisError> {
    if matches!(self.group_status, ConsumerGroupState::Uninitialized) {
      self.initialize_consumer_group().await?;
    }

    if matches!(self.group_status, ConsumerGroupState::PreviouslyCreated) {
      self
        .process_idle_pending(
          &min_idle,
          &Some(max_idle.unwrap_or_else(|| Duration::new(0, 0))),
        )
        .await?;
    }

    Ok(())
  }

  /// Process redis stream entries until shutdown signal received
  pub async fn start_reactor(&mut self, claim_mode: ClaimMode) -> Result<(), RedisError> {
    if matches!(self.group_status, ConsumerGroupState::Uninitialized) {
      self.initialize_consumer_group().await?;
    }

    match claim_mode {
      ClaimMode::ClearBacklog { min_idle, max_idle } => {
        if matches!(self.group_status, ConsumerGroupState::PreviouslyCreated) {
          try_join!(
            backoff::future::retry(ExponentialBackoff::default(), || async {
              self
                .process_idle_pending(
                  &min_idle,
                  &Some(max_idle.unwrap_or_else(|| Duration::new(0, 0))),
                )
                .await?;

              Ok(())
            }),
            backoff::future::retry(ExponentialBackoff::default(), || async {
              self.process_stream().await?;

              Ok(())
            })
          )?;
        } else {
          backoff::future::retry(ExponentialBackoff::default(), || async {
            self.process_stream().await?;

            Ok(())
          })
          .await?;
        }
      }
      ClaimMode::Autoclaim { min_idle } => {
        try_join!(
          backoff::future::retry(ExponentialBackoff::default(), || async {
            self.process_idle_pending(&min_idle, &None).await?;

            Ok(())
          }),
          backoff::future::retry(ExponentialBackoff::default(), || async {
            self.process_stream().await?;

            Ok(())
          })
        )?;
      }
      ClaimMode::NewOnly => {
        backoff::future::retry(ExponentialBackoff::default(), || async {
          self.process_stream().await?;

          Ok(())
        })
        .await?;
      }
    }

    Ok(())
  }

  pub fn shutdown_token(&self) -> Arc<CancellationToken> {
    self.ctx.cancel_token.clone()
  }
}

#[cfg(test)]
mod tests {
  use std::collections::{BTreeMap, HashMap};

  use crate::StreamEntry;
  use decimal::d128;
  use redis::{streams::StreamId, RedisError};
  use serde::{Deserialize, Serialize};
  use serde_aux::prelude::*;
  use serde_with::serde_as;
  #[derive(Debug, Serialize, Deserialize, PartialEq)]
  pub struct Author {
    name: String,
    tags: Vec<String>,
  }

  impl TryFrom<&str> for Author {
    type Error = serde_json::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
      serde_json::from_str(value)
    }
  }

  impl TryFrom<String> for Author {
    type Error = serde_json::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
      serde_json::from_str(value.as_str())
    }
  }

  #[derive(Debug, Serialize, Deserialize, PartialEq)]
  #[serde(tag = "type", rename_all = "camelCase")]
  pub enum CreateLorem {
    Sentences {
      #[serde(deserialize_with = "deserialize_number_from_string")]
      count: i64,
    },
    Paragraphs {
      #[serde(deserialize_with = "deserialize_number_from_string")]
      count: i64,
      #[serde(deserialize_with = "deserialize_number_from_string")]
      sentences_per_paragraph: i64,
    },
  }

  #[serde_as]
  #[derive(Debug, Serialize, Deserialize, PartialEq)]
  #[serde(tag = "op", rename_all = "camelCase")]
  pub enum Op {
    CreateLorem(CreateLorem),
    CreatePost {
      #[serde(deserialize_with = "deserialize_number_from_string")]
      id: i64,
      content: String,
      #[serde_as(as = "serde_with::PickFirst<(_, serde_with::json::JsonString)>")]
      author: Author,
    },
    SetWinRate {
      rate: d128,
    },
  }

  #[test]
  fn deserializes_from_stream_id() -> Result<(), RedisError> {
    let mut map: HashMap<String, redis::Value> = HashMap::new();
    map.insert(
      "op".into(),
      redis::Value::Data("createLorem".as_bytes().to_owned()),
    );
    map.insert(
      "type".into(),
      redis::Value::Data("sentences".as_bytes().to_owned()),
    );
    map.insert(
      "count".into(),
      redis::Value::Data("100".as_bytes().to_owned()),
    );

    let stream_id = StreamId {
      id: "0-0".into(),
      map,
    };

    let value = Op::from_stream_id(&stream_id)?;

    assert_eq!(
      value,
      Op::CreateLorem(CreateLorem::Sentences { count: 100 })
    );

    Ok(())
  }

  #[test]
  fn deserializes_nested_structs() -> Result<(), RedisError> {
    let author = Author {
      name: String::from("Bajix"),
      tags: vec!["Rustacean".into()],
    };

    let mut map: HashMap<String, redis::Value> = HashMap::new();
    map.insert(
      "op".into(),
      redis::Value::Data("createPost".as_bytes().to_owned()),
    );
    map.insert("id".into(), redis::Value::Data("1".as_bytes().to_owned()));
    map.insert(
      "content".into(),
      redis::Value::Data("Hello World!".as_bytes().to_owned()),
    );
    map.insert(
      "author".into(),
      redis::Value::Data(
        serde_json::to_string(&author)
          .unwrap()
          .as_bytes()
          .to_owned(),
      ),
    );

    let stream_id = StreamId {
      id: "0-0".into(),
      map,
    };

    let value = Op::from_stream_id(&stream_id)?;

    assert_eq!(
      value,
      Op::CreatePost {
        id: 1,
        content: String::from("Hello World!"),
        author
      }
    );

    Ok(())
  }

  #[test]
  fn deserializes_d128() -> Result<(), RedisError> {
    let mut map: HashMap<String, redis::Value> = HashMap::new();
    map.insert(
      "op".into(),
      redis::Value::Data("setWinRate".as_bytes().to_owned()),
    );
    map.insert(
      "rate".into(),
      redis::Value::Data(".5".as_bytes().to_owned()),
    );

    let stream_id = StreamId {
      id: "0-0".into(),
      map,
    };

    let value = Op::from_stream_id(&stream_id)?;

    assert_eq!(value, Op::SetWinRate { rate: d128!(0.5) });

    Ok(())
  }

  #[test]
  fn into_redis_btreemap() -> Result<(), RedisError> {
    let op = Op::CreatePost {
      id: 1,
      content: String::from("Hello World!"),
      author: Author {
        name: String::from("Bajix"),
        tags: vec!["Rustacean".into()],
      },
    };

    let data: Vec<(String, String)> = vec![
      (
        "author".into(),
        "{\"name\":\"Bajix\",\"tags\":[\"Rustacean\"]}".into(),
      ),
      ("content".into(), "Hello World!".into()),
      ("id".into(), "1".into()),
      ("op".into(), "createPost".into()),
    ];

    let data: BTreeMap<String, String> = BTreeMap::from_iter(data.into_iter());

    assert_eq!(op.xadd_map()?, data);

    Ok(())
  }
}
