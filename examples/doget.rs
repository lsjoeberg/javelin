use arrow_flight::client::FlightClient;
use futures::{StreamExt, TryStreamExt};
use tonic::codegen::Bytes;
use tonic::transport::Channel;

#[tokio::main]
async fn main() {
    let channel = Channel::from_static("http://0.0.0.0:50051").connect().await;
    let channel = match channel {
        Ok(ch) => ch,
        Err(e) => {
            eprintln!("error connecting: {e}");
            std::process::exit(1);
        }
    };

    let mut client = FlightClient::new(channel);

    let ticket = arrow_flight::Ticket {
        ticket: Bytes::from("tkt"),
    };
    let response = match client.do_get(ticket).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error calling do_get: {e}");
            std::process::exit(1);
        }
    };
    if let Some(schema) = response.schema() {
        println!("recv stream with schema:\n{schema}");
    }

    // Create a stream of RecordBatches from stream of low-level FlightData.
    let mut record_batch_stream = response.into_stream();

    // Iterate over record batches in response stream.
    let mut n: usize = 0;
    while let Some(b) = record_batch_stream.next().await {
        match b {
            Ok(b) => {
                if n == 0 {
                    println!("recv stream with schema:\n{:#?}", b.schema())
                }
                println!("recv rec {:0>3}: rows: {}", n, b.num_rows());
                n += 1;
            }
            Err(e) => eprintln!("{e}"),
        }
    }
}
