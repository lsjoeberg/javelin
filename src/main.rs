use std::net::SocketAddr;
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use clap::Parser;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::prelude::SessionContext;
use tonic::transport::Server;

use javelin::{cli, flight};

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C signal handler");
    println!("Received CTRL+C: shutting down");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = cli::Config::parse();

    // Create a DataFusion session context and register tables from sources.
    let ctx = SessionContext::new();
    let file_format = ParquetFormat::default()
        .with_enable_pruning(Some(true))
        .with_skip_metadata(Some(true));
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(".parquet")
        .with_collect_stat(true);

    // Register user datasets as tables.
    for ts in cfg.tables {
        ctx.register_listing_table(&ts.name, &ts.path, listing_options.clone(), None, None)
            .await?;
        println!("Registered table '{}' for path '{}'", ts.name, ts.path)
    }

    // Create Flight service.
    let javelin = flight::Javelin::new(ctx);
    let service = FlightServiceServer::new(javelin);

    // Start gRPC server.
    let addr: SocketAddr = format!("{}:{}", cfg.host, cfg.port).parse()?;
    println!("Starting service on {}", addr);
    let router = Server::builder().add_service(service);

    router
        .serve_with_shutdown(addr, async { shutdown_signal().await })
        .await?;

    Ok(())
}
