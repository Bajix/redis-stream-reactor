use backoff::ExponentialBackoff;
use env_url::ServiceURL;
use futures::{stream, StreamExt, TryStreamExt};
use itertools::Itertools;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use redis::{
  streams::{StreamId, StreamKey, StreamRangeReply, StreamReadOptions, StreamReadReply},
  AsyncCommands, ErrorKind, RedisError, ToRedisArgs, Value,
};
use redis_swapplex::{get_connection, RedisEnvService};
use serde::de::Deserialize;
use std::{
  borrow::Cow,
  collections::HashMap,
  fmt::Debug,
  marker::PhantomData,
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

/// To use generic impl derive [`redis::toRedisArgs`](https://docs.rs/redis/0.21/redis/trait.ToRedisArgs.html) with [`derive_redis_json::RedisJsonValue`](https://docs.rs/derive-redis-json/0.1.1/derive_redis_json/derive.RedisJsonValue.html)
pub trait StreamEntry: Send + Sync + Sized {
  fn from_hashmap(data: &HashMap<String, Value>) -> Result<Self, RedisError>;
  fn as_redis_args(&self) -> Vec<(Vec<u8>, Vec<u8>)>;
}

impl<T> StreamEntry for T
where
  T: Send + Sync + Sized + for<'de> Deserialize<'de> + ToRedisArgs,
{
  fn from_hashmap(entry: &HashMap<String, Value>) -> Result<Self, RedisError> {
    let data = entry
      .iter()
      .map(|(key, value)| match *value {
        redis::Value::Data(ref bytes) => std::str::from_utf8(&bytes[..])
          .map_err(|err| {
            RedisError::from((
              redis::ErrorKind::TypeError,
              "Stream field value invalid utf8",
              err.to_string(),
            ))
          })
          .map(|value| (key.to_owned(), serde_json::Value::String(value.to_owned()))),
        _ => Err(RedisError::from((
          redis::ErrorKind::TypeError,
          "invalid response type for JSON",
        ))),
      })
      .collect::<Result<Vec<(String, serde_json::Value)>, RedisError>>()?;

    let data = serde_json::map::Map::from_iter(data.into_iter());

    serde_json::from_value(serde_json::Value::Object(data)).map_err(|err| {
      RedisError::from((
        ErrorKind::TypeError,
        "JSON deserialization failed",
        err.to_string(),
      ))
    })
  }

  fn as_redis_args(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
    self
      .to_redis_args()
      .into_iter()
      .tuple_windows::<(_, _)>()
      .collect_vec()
  }
}
pub trait ConsumerGroup {
  const GROUP_NAME: &'static str;
}

#[async_trait::async_trait]
pub trait StreamConsumer<T: StreamEntry, G: ConsumerGroup>: Default + Send + Sync {
  const MIN_IDLE_TIME: Duration = Duration::from_millis(1500);
  const XREAD_BLOCK_TIME: Duration = Duration::from_secs(60);
  const BATCH_SIZE: usize = 100;
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
    let event = <T as StreamEntry>::from_hashmap(&entry.map)?;

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

          match (ctx.error_stream_key(), ctx.error_hash_key()) {
            (Some(stream_key), Some(hash_key)) => {
              let mut conn = get_connection();

              let mut pipe = redis::pipe();

              let args = event.as_redis_args();

              pipe
                .xadd(stream_key.as_ref(), &entry.id, &args[..])
                .ignore();
              pipe
                .hset(hash_key.as_ref(), &entry.id, format!("{:?}", &err))
                .ignore();

              let _: () = pipe.query_async(&mut conn).await?;
            }
            (Some(key), None) => {
              let mut conn = get_connection();
              let args = event.as_redis_args();
              let _: String = conn.xadd(key.as_ref(), &entry.id, &args[..]).await?;
            }
            (None, Some(key)) => {
              let mut conn = get_connection();
              let _: i64 = conn
                .hset(key.as_ref(), &entry.id, format!("{:?}", &err))
                .await?;
            }
            _ => (),
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
  error_hash_key: Option<Cow<'a, str>>,
  cancel_token: CancellationToken,
}

impl<'a, T> Context<'a, T>
where
  T: Send + Sync,
{
  fn new(
    data: T,
    stream_key: Cow<'a, str>,
    error_stream_key: Option<Cow<'a, str>>,
    error_hash_key: Option<Cow<'a, str>>,
  ) -> Self {
    Self {
      data,
      consumer_id: Self::generate_id(),
      stream_key,
      error_stream_key,
      error_hash_key,
      cancel_token: CancellationToken::new(),
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
  pub fn error_hash_key(&self) -> Option<&Cow<'a, str>> {
    self.error_hash_key.as_ref()
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
    error_hash_key: Option<Cow<'a, str>>,
  ) -> Self {
    EventReactor {
      group_status: ConsumerGroupState::Uninitialized,
      consumer: Arc::new(T::default()),
      ctx: Context::new(data, stream_key, error_stream_key, error_hash_key),
      _marker: PhantomData,
    }
  }

  /// Manually initialize Redis consumer group; [`EventReactor::start_reactor`] will initialize otherwise
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
          .arg(T::MIN_IDLE_TIME.as_millis() as usize)
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

  async fn process_idle_pending(&self, exit_on_idle: bool) -> Result<(), RedisError> {
    let mut cursor = String::from("0-0");

    let start_time = SystemTime::now();

    while let Some((next_cursor, reply)) = self.autoclaim_batch(cursor.as_str()).await? {
      cursor = next_cursor;

      let poll_time = SystemTime::now();

      if !reply.ids.is_empty() {
        self
          .consumer
          .process_event_stream(&self.ctx, reply.ids, &DeliveryStatus::MinIdleElapsed)
          .await?;
      } else if let Some(sleep_time) = T::MIN_IDLE_TIME.checked_div(4) {
        if exit_on_idle {
          if let Ok(call_elapsed) = poll_time.duration_since(start_time) {
            if call_elapsed.gt(&T::MIN_IDLE_TIME) {
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

  /// Process redis stream entries until shutdown signal received, optionally clearing all the potential idle-pending backlog before claiming new entries
  pub async fn start_reactor(mut self, clear_backlog: bool) -> Result<(), RedisError> {
    if matches!(self.group_status, ConsumerGroupState::Uninitialized) {
      self.initialize_consumer_group().await?;
    }

    if clear_backlog && matches!(self.group_status, ConsumerGroupState::PreviouslyCreated) {
      self.process_idle_pending(true).await?;
    }

    try_join!(
      backoff::future::retry(ExponentialBackoff::default(), || async {
        self.process_idle_pending(false).await?;

        Ok(())
      }),
      backoff::future::retry(ExponentialBackoff::default(), || async {
        self.process_stream().await?;

        Ok(())
      })
    )?;

    Ok(())
  }
}
