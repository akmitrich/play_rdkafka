use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[tokio::main]
async fn main() {
    let mut stdout = tokio::io::stdout();
    let _ = stdout
        .write(format!("Welcome to {:?}!\n", rdkafka::util::get_rdkafka_version()).as_bytes())
        .await
        .unwrap();
    let mut input_lines = BufReader::new(tokio::io::stdin()).lines();
    let producer = create_producer("alexander-VirtualBox:9092");
    loop {
        let _ = stdout.write(b"> ").await.unwrap();
        stdout.flush().await.unwrap();
        match input_lines.next_line().await.unwrap() {
            Some(line) if line.is_empty() => break,
            Some(line) => {
                let _x = producer
                    .send(
                        FutureRecord::<(), _>::to("chat").payload(&line),
                        rdkafka::util::Timeout::After(tokio::time::Duration::from_secs(5)),
                    )
                    .await
                    .expect("Failed to send line");
            }
            None => break,
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
