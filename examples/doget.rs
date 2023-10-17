use arrow::datatypes::SchemaRef;
use arrow_flight::client::FlightClient;
use futures::{StreamExt, TryStreamExt};
use std::env;
use std::time::Instant;
use tonic::codegen::Bytes;
use tonic::transport::Channel;

#[tokio::main]
async fn main() {
    let mut args = env::args();
    args.next();
    let query = match args.next() {
        Some(q) => q,
        None => String::from("SELECT * FROM nyc"),
    };

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
        ticket: Bytes::from(query),
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
    let mut nrec: usize = 0;
    let mut nrow: usize = 0;
    let mut schema: Option<SchemaRef> = None;
    let t0 = Instant::now();
    while let Some(b) = record_batch_stream.next().await {
        match b {
            Ok(b) => {
                if nrec == 0 {
                    schema = Some(b.schema());
                    println!("recv stream with schema:\n{:#?}", schema)
                }
                println!("recv rec {:0>3}: rows: {}", nrec, b.num_rows());
                nrec += 1;
                nrow += b.num_rows();
            }
            Err(e) => eprintln!("{e}"),
        }
    }
    let elapsed = t0.elapsed().as_secs_f64();
    println!("recv {nrec} records, {nrow} rows in {elapsed:.3}s; with schema:\n{schema:#?}");
}
