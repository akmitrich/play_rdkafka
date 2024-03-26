use chrono::TimeZone;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    message::ToBytes,
    producer::{FutureProducer, FutureRecord},
    ClientConfig, Message,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[tokio::main]
async fn main() {
    let bootstrap_server = "alexander-VirtualBox:9092";
    let mut stdout = tokio::io::stdout();
    let _ = stdout
        .write(format!("Welcome to {:?}!\n", rdkafka::util::get_rdkafka_version()).as_bytes())
        .await
        .unwrap();
    let mut input_lines = BufReader::new(tokio::io::stdin()).lines();
    let producer = create_producer(bootstrap_server);
    let consumer = create_consumer(bootstrap_server);
    consumer.subscribe(&["chat"]).unwrap();
    loop {
        let _ = stdout.write(b"> ").await.unwrap();
        stdout.flush().await.unwrap();
        tokio::select! {
            message = consumer.recv() => {
                let msg = message_representation(message.expect("Failed to read message"));
                let _ = stdout.write(msg.to_bytes()).await.unwrap();
                let _=stdout.write(b"\n").await.unwrap();
            }

            line = input_lines.next_line()=>match line {
                Ok(Some(line)) if line.is_empty() => break,
                Ok(Some(line)) => {
                    let _x = producer
                        .send(
                            FutureRecord::<_, _>::to("chat").key(std::env::args().nth(1).unwrap_or_else(|| "Master".to_owned()).as_bytes()).payload(&line),
                            rdkafka::util::Timeout::After(tokio::time::Duration::from_secs(5)),
                        )
                        .await
                        .expect("Failed to send line");
                }
                _ => break,
            }
        }
    }
}

fn create_producer(bootstrap_server: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_server)
        .set("queue.buffering.max.ms", "0")
        .create()
        .expect("Failed to create producer")
}

fn create_consumer(bootstrap_server: &str) -> StreamConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_server)
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", "earliest")
        // We'll give each session its own (unique) consumer group id,
        // so that each session will receive all messages
        .set("group.id", format!("chat-{}", uuid::Uuid::new_v4()))
        .create()
        .expect("Failed to create consumer")
}

fn message_representation(message: rdkafka::message::BorrowedMessage) -> String {
    let t = if let rdkafka::Timestamp::CreateTime(t) = message.timestamp() {
        format!(
            "[{}]",
            chrono::Local
                .timestamp_millis_opt(t)
                .unwrap()
                .format("%d/%m/%Y %T")
        )
    } else {
        "".into()
    };
    format!(
        "{} {} -> {}",
        t,
        message
            .key()
            .and_then(|s| std::str::from_utf8(s).ok())
            .unwrap_or_default(),
        message
            .payload()
            .and_then(|s| std::str::from_utf8(s).ok())
            .unwrap_or_default()
    )
}
