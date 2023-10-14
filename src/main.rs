use arrow_flight::flight_service_server::FlightServiceServer;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::prelude::SessionContext;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = "0.0.0.0:50051".parse()?;

    let ctx = SessionContext::new();
    let file_format = ParquetFormat::default().with_enable_pruning(Some(true));
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(".parquet")
        .with_collect_stat(true);
    ctx.register_listing_table("nyc", "./data/nyc", listing_options, None, None)
        .await?;

    let service = javelin::Javelin::new(ctx);
    let svc = FlightServiceServer::new(service);

    println!("Starting service on {}", addr);
    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
