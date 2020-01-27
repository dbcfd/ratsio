use ratsio::nats_client::NatsClient;
use log::info;
use futures::StreamExt;
use ratsio::error::RatsioError;
use ratsio::{protocol, StanClient, StanOptions};
use ratsio::nuid;
use std::time::Duration;

pub fn logger_setup() {
    use log::LevelFilter;
    use std::io::Write;
    use env_logger::Builder;

    let _ = Builder::new()
        .format(|buf, record| {
            writeln!(buf,
                     "[{}] - {}",
                     record.level(),
                     record.args()
            )
        })
        .filter(None, LevelFilter::Trace)
        .try_init();
}


#[tokio::test]
async fn test_nats() -> Result<(), RatsioError> {
    logger_setup();

    let nats_client = NatsClient::new("nats://localhost:4222").await?;
    let (sid, mut subscription) = nats_client.subscribe("foo").await?;
    tokio::spawn(async move {
        while let Some(message) = subscription.next().await {
            info!(" << 1 >> got message --- {:?}\n\t{:?}", &message,
                  String::from_utf8_lossy(message.payload.as_ref()));
        }
    });

    let (_sid, mut subscription2) = nats_client.subscribe("foo").await?;
    tokio::spawn(async move {
        while let Some(message) = subscription2.next().await {
            info!(" << 2 >> got message --- {:?}\n\t{:?}", &message,
                  String::from_utf8_lossy(message.payload.as_ref()));
        }
    });

    tokio::time::delay_for(Duration::from_secs(5)).await;
    let _ = nats_client.publish("foo", b"Publish Message 1").await?;
    tokio::time::delay_for(Duration::from_secs(1)).await;
    let _ = nats_client.un_subscribe(&sid).await?;
    tokio::time::delay_for(Duration::from_secs(3)).await;

    tokio::time::delay_for(Duration::from_secs(1)).await;
    let _ = nats_client.publish("foo", b"Publish Message 2").await?;


    let _discover_subject = "_STAN.discover.test-cluster";
    let client_id = "test-1";
    let conn_id = nuid::next();
    let heartbeat_inbox = format!("_HB.{}", &conn_id);
    let _connect_request = protocol::ConnectRequest {
        client_id: client_id.into(),
        conn_id: conn_id.clone().as_bytes().into(),
        heartbeat_inbox: heartbeat_inbox.clone(),
        ..Default::default()
    };
    tokio::time::delay_for(std::time::Duration::from_secs(2)).await;
    Ok(())
}

#[tokio::test]
async fn test_stan() -> Result<(), RatsioError> {
    logger_setup();

    let opts = StanOptions::with_options(
        "nats://localhost:4222".to_owned(),
        "test-cluster",
        "stan-test",
    );
    let cli = StanClient::from_options(opts).await?;
    let (sid, mut subscription) = cli.subscribe("foo", None, None).await?;
    tokio::spawn(async move {
        while let Some(message) = subscription.next().await {
            info!(" << 1 >> got message --- {:?}\n\t{:?}", &message,
                  String::from_utf8_lossy(message.payload.as_ref()));
        }
    });

    let (_sid, mut subscription2) = cli.subscribe("foo", None, None).await?;
    tokio::spawn(async move {
        while let Some(message) = subscription2.next().await {
            info!(" << 2 >> got message --- {:?}\n\t{:?}", &message,
                  String::from_utf8_lossy(message.payload.as_ref()));
        }
    });

    tokio::time::delay_for(Duration::from_secs(5)).await;
    let payload = b"Publish Message 1".to_vec();
    let _ = cli.publish("foo", payload.as_slice()).await?;
    tokio::time::delay_for(Duration::from_secs(1)).await;
    let _ = cli.un_subscribe(&sid).await?;
    tokio::time::delay_for(Duration::from_secs(3)).await;

    tokio::time::delay_for(Duration::from_secs(1)).await;
    let _ = cli.publish("foo", b"Publish Message 2").await?;


    let _discover_subject = "_STAN.discover.test-cluster";
    let client_id = "test-1";
    let conn_id = nuid::next();
    let heartbeat_inbox = format!("_HB.{}", &conn_id);
    let _connect_request = protocol::ConnectRequest {
        client_id: client_id.into(),
        conn_id: conn_id.clone().as_bytes().into(),
        heartbeat_inbox: heartbeat_inbox.clone(),
        ..Default::default()
    };
    tokio::time::delay_for(std::time::Duration::from_secs(2)).await;
    Ok(())
}