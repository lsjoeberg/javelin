use std::time::Instant;

use arrow_flight::client::FlightClient;
use clap::{Parser, ValueEnum};
use datafusion::arrow::datatypes::SchemaRef;
use futures::{StreamExt, TryStreamExt};
use tonic::codegen::Bytes;
use tonic::transport::{Channel, Uri};

#[derive(Clone, Debug, ValueEnum)]
enum FlightMethod {
    ListFlights,
    DoGet,
}

#[derive(Parser, Debug)]
struct Args {
    /// Flight method to execute
    #[arg(value_enum)]
    method: FlightMethod,
    /// Query for method
    #[arg(long, short)]
    query: Option<String>,
    /// Server address
    #[arg(long, short = 's', default_value = "0.0.0.0")]
    host: String,
    /// Server port
    #[arg(long, short = 'p', default_value_t = 50051)]
    port: u16,
}

async fn do_get(client: &mut FlightClient, query: Option<String>) {
    let query = query.unwrap_or("SELECT 1".to_string());
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
                    println!("recv stream with schema:\n{:#?}", b.schema());
                    schema = Some(b.schema());
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

async fn list_flights(client: &mut FlightClient, expr: Option<String>) {
    let expr = expr.unwrap_or("".to_string());
    let response = match client.list_flights(expr).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error calling do_get: {e}");
            std::process::exit(1);
        }
    };
    let mut stream = response.into_stream();
    while let Some(info) = stream.next().await {
        match info {
            Ok(info) => println!("{info:#?}"),
            Err(err) => eprintln!("{err}"),
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let uri = match format!("http://{}:{}", args.host, args.port).parse::<Uri>() {
        Ok(u) => u,
        Err(_) => {
            eprintln!("Invalid URI parts: {}:{}", args.host, args.port);
            std::process::exit(1);
        }
    };
    let channel = Channel::builder(uri).connect().await;
    let channel = match channel {
        Ok(ch) => ch,
        Err(e) => {
            eprintln!("error connecting: {e}");
            std::process::exit(1);
        }
    };

    let mut client = FlightClient::new(channel);

    match &args.method {
        FlightMethod::ListFlights => list_flights(&mut client, args.query).await,
        FlightMethod::DoGet => do_get(&mut client, args.query).await,
    }
}
