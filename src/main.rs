use arrow_flight::flight_service_server::FlightServiceServer;
use clap::Parser;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::prelude::SessionContext;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use tonic::transport::Server;

#[derive(Parser, Debug)]
struct Config {
    /// List of dataset root paths to serve
    #[arg(long, short, num_args = 0.., required = true)]
    datasets: Vec<String>,
    /// List of table names to use for each dataset
    #[arg(long, short, num_args = 0..)]
    table_names: Option<Vec<String>>,
    /// Server address
    #[arg(long, short = 's', default_value = "0.0.0.0")]
    host: String,
    /// Server port
    #[arg(long, short = 'p', default_value_t = 50051)]
    port: u16,
}

fn table_name_from_path(path: &str) -> Option<String> {
    let p = Path::new(path);
    match p.file_stem() {
        Some(s) => match s.to_ascii_lowercase().into_string() {
            Ok(s) => Some(s),
            _ => None,
        },
        None => None,
    }
}

fn parse_cfg() -> Config {
    let mut cfg = Config::parse();

    // Dataset paths and table names are pairwise.
    match &cfg.table_names {
        Some(names) => {
            if names.len() != cfg.datasets.len() {
                eprintln!("must provide exactly one table name for each dataset");
                std::process::exit(1);
            }
        }
        None => {
            let mut names: Vec<String> = vec![];
            for ds in &cfg.datasets {
                match table_name_from_path(ds) {
                    Some(t) => names.push(t.to_string()),
                    None => {
                        eprintln!("failed to parse table name from path {}", ds);
                        std::process::exit(1);
                    }
                }
            }
            cfg.table_names = Some(names);
        }
    }
    cfg
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = parse_cfg();

    let ctx = SessionContext::new();
    let file_format = ParquetFormat::default()
        .with_enable_pruning(Some(true))
        .with_skip_metadata(Some(true));
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(".parquet")
        .with_collect_stat(true);

    // Register user datasets as tables.
    for (tn, ds) in cfg.table_names.unwrap().iter().zip(cfg.datasets.iter()) {
        ctx.register_listing_table(tn, ds, listing_options.clone(), None, None)
            .await?;
        println!("Registered table '{tn}' for dataset '{ds}'")
    }

    let service = javelin::Javelin::new(ctx);
    let svc = FlightServiceServer::new(service);

    let addr: SocketAddr = format!("{}:{}", cfg.host, cfg.port).parse()?;
    println!("Starting service on {}", addr);
    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
