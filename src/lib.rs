use backoff::ExponentialBackoff;
use env_url::ServiceURL;
use futures::{stream, StreamExt, TryStreamExt};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use redis::{
  streams::{StreamId, StreamKey, StreamRangeReply, StreamReadOptions, StreamReadReply},
  AsyncCommands, ErrorKind, RedisError, Value,
};
use redis_swapplex::{get_connection, RedisEnvService};
use std::{collections::HashMap, fmt::Debug, marker::PhantomData, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::{signal, time::sleep, try_join};

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
  XDEL,
  /// Skip stream entry acknowledgement
  #[error("Stream entry acknowledgment skipped")]
  SkipAcknowledgement,
  #[error(transparent)]
  Error(#[from] T),
}

pub trait StreamEvent: Send + Sync + Sized + 'static {
  fn from_hashmap(data: &HashMap<String, Value>) -> Option<Self>;
  fn as_redis_args<'a>(&'a self) -> Vec<(&'a str, &'a str)>;
}

pub trait ConsumerGroup: 'static {
  const GROUP_NAME: &'static str;
}

#[async_trait::async_trait]
pub trait StreamConsumer<T: StreamEvent, G: ConsumerGroup>:
  Default + Send + Sync + 'static
{
  const STREAM_KEY: &'static str;
  const ERROR_STREAM_KEY: Option<&'static str>;
  const ERROR_HASH_KEY: Option<&'static str>;
  const MIN_IDLE_TIME: Duration = Duration::from_secs(2);
  const XREAD_BLOCK_TIME: Duration = Duration::from_secs(2);
  const BATCH_SIZE: usize = 100;
  const CONCURRENCY: usize = 10;
  const XACK: bool = true;
  type Error: Send + Debug;

  async fn process_event(
    &self,
    event: &T,
    status: &DeliveryStatus,
  ) -> Result<(), TaskError<Self::Error>>;

  async fn process_event_stream(
    &self,
    ids: Vec<StreamId>,
    status: &DeliveryStatus,
  ) -> Result<(), RedisError> {
    stream::iter(ids.into_iter())
      .map(Ok)
      .try_for_each_concurrent(Self::CONCURRENCY, |entry| async move {
        self.process_stream_entry(entry, &status).await
      })
      .await?;

    Ok(())
  }

  async fn process_stream_entry(
    &self,
    entry: StreamId,
    status: &DeliveryStatus,
  ) -> Result<(), RedisError> {
    if let Some(event) = <T as StreamEvent>::from_hashmap(&entry.map) {
      match self.process_event(&event, &status).await {
        Ok(()) => {
          let mut conn = get_connection();

          if Self::XACK {
            let _: i64 = conn
              .xack(Self::STREAM_KEY, G::GROUP_NAME, &[&entry.id])
              .await?;
          } else {
            let _: i64 = conn.xdel(Self::STREAM_KEY, &[&entry.id]).await?;
          }
        }
        Err(err) => match err {
          TaskError::XDEL => {
            let mut conn = get_connection();
            let _: i64 = conn.xdel(Self::STREAM_KEY, &[&entry.id]).await?;
          }
          TaskError::SkipAcknowledgement => (),
          TaskError::Error(err) => {
            log::error!("Error processing stream event: {:?}", err);

            match (Self::ERROR_STREAM_KEY, Self::ERROR_HASH_KEY) {
              (Some(stream_key), Some(hash_key)) => {
                let mut conn = get_connection();

                let mut pipe = redis::pipe();

                let event = event.as_redis_args();

                pipe.xadd(stream_key, &entry.id, &event[..]).ignore();
                pipe
                  .hset(hash_key, &entry.id, format!("{:?}", &err))
                  .ignore();

                let _: () = pipe.query_async(&mut conn).await?;
              }
              (Some(key), None) => {
                let mut conn = get_connection();
                let event = event.as_redis_args();
                let _: String = conn.xadd(key, &entry.id, &event[..]).await?;
              }
              (None, Some(key)) => {
                let mut conn = get_connection();
                let _: i64 = conn.hset(key, &entry.id, format!("{:?}", &err)).await?;
              }
              _ => (),
            };
          }
        },
      }
    }

    Ok(())
  }
}

pub struct EventReactor<T, E, G>
where
  T: StreamConsumer<E, G>,
  E: StreamEvent,
  G: ConsumerGroup,
{
  consumer: Arc<T>,
  consumer_id: String,
  _marker: PhantomData<fn() -> (E, G)>,
}

impl<T, E, G> Default for EventReactor<T, E, G>
where
  T: StreamConsumer<E, G>,
  E: StreamEvent,
  G: ConsumerGroup,
{
  fn default() -> Self {
    let consumer = Arc::new(T::default());

    EventReactor {
      consumer,
      consumer_id: Self::generate_id(),
      _marker: PhantomData,
    }
  }
}

impl<T, E, G> Clone for EventReactor<T, E, G>
where
  T: StreamConsumer<E, G>,
  E: StreamEvent,
  G: ConsumerGroup,
{
  fn clone(&self) -> Self {
    Self {
      consumer: self.consumer.clone(),
      consumer_id: self.consumer_id.clone(),
      _marker: PhantomData,
    }
  }
}

impl<T, E, G> EventReactor<T, E, G>
where
  T: StreamConsumer<E, G>,
  E: StreamEvent,
  G: ConsumerGroup,
{
  fn generate_id() -> String {
    thread_rng()
      .sample_iter(&Alphanumeric)
      .take(30)
      .map(char::from)
      .collect()
  }

  async fn initialize_consumer_group(&self) -> Result<(), RedisError> {
    let url = RedisEnvService::service_url().map_err(|_| {
      RedisError::from((
        ErrorKind::InvalidClientConfig,
        "Invalid Redis connection URL",
      ))
    })?;
    let client = redis::Client::open(url)?;
    let mut conn = client.get_async_connection().await?;

    if let Some(error_stream) = T::ERROR_STREAM_KEY {
      let mut pipe = redis::pipe();
      pipe
        .xgroup_create_mkstream(T::STREAM_KEY, G::GROUP_NAME, "0")
        .ignore();
      pipe
        .xgroup_create_mkstream(error_stream, G::GROUP_NAME, "0")
        .ignore();

      let _: () = pipe.query_async(&mut conn).await?;

      Ok(())
    } else {
      let _: String = conn
        .xgroup_create_mkstream(T::STREAM_KEY, G::GROUP_NAME, "0")
        .await?;

      Ok(())
    }
  }

  async fn autoclaim_batch(
    &self,
    cursor: &str,
  ) -> Result<Option<(String, StreamRangeReply)>, RedisError> {
    tokio::select! {
      reply = async {
        let mut conn = get_connection();

        let mut cmd = redis::cmd("XAUTOCLAIM");

        cmd.arg(T::STREAM_KEY)
          .arg(G::GROUP_NAME)
          .arg(&self.consumer_id)
          .arg(T::MIN_IDLE_TIME.as_millis() as usize)
          .arg(cursor)
          .arg("COUNT")
          .arg(T::BATCH_SIZE);

        let reply: (String, StreamRangeReply) = cmd.query_async(&mut conn).await?;

        Ok(Some(reply))
      } => reply,
      _ = signal::ctrl_c() => Ok(None),
    }
  }

  async fn process_idle_pending(&self) -> Result<(), RedisError> {
    let mut cursor = String::from("0-0");

    while let Some((next_cursor, reply)) = self.autoclaim_batch(cursor.as_str()).await? {
      cursor = next_cursor;

      if !reply.ids.is_empty() {
        self
          .consumer
          .process_event_stream(reply.ids, &DeliveryStatus::MinIdleElapsed)
          .await?;
      } else if let Some(sleep_time) = T::MIN_IDLE_TIME.checked_div(4) {
        sleep(sleep_time).await;
      }
    }

    Ok(())
  }

  async fn next_batch(&self) -> Result<Option<StreamReadReply>, RedisError> {
    tokio::select! {
      reply = async {
        let mut conn = get_connection();

        let reply: StreamReadReply = conn
        .xread_options(
          &[T::STREAM_KEY],
          &[">"],
          &StreamReadOptions::default()
            .group(G::GROUP_NAME, &self.consumer_id)
            .block(T::XREAD_BLOCK_TIME.as_millis() as usize)
            .count(T::BATCH_SIZE),
        )
        .await?;

        Ok(Some(reply))
      } => reply,
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
          .process_event_stream(ids, &DeliveryStatus::NewDelivery)
          .await?;
      }
    }

    Ok(())
  }

  pub async fn start_reactor(self) -> Result<(), RedisError> {
    match self.initialize_consumer_group().await {
      // It is expected behavior that this will fail when already initalized
      // Expected error: `BUSYGROUP: Consumer Group name already exists`
      Err(err) if err.code() == Some("BUSYGROUP") => Ok(()),
      Err(err) => Err(err),
      Ok(_) => Ok(()),
    }?;

    try_join!(
      backoff::future::retry(ExponentialBackoff::default(), || async {
        self.process_idle_pending().await?;

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
