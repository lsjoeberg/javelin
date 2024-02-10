use std::net::SocketAddr;
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::prelude::SessionContext;
use tonic::transport::Server;

use javelin::{cli, flight};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = cli::parse_cfg();

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

    let service = flight::Javelin::new(ctx);
    let svc = FlightServiceServer::new(service);

    let addr: SocketAddr = format!("{}:{}", cfg.host, cfg.port).parse()?;
    println!("Starting service on {}", addr);
    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
