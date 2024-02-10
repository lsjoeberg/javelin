use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty, FlightData,
    FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult,
    SchemaResult, Ticket,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::prelude::*;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use tonic::{Request, Response, Status, Streaming};

pub struct Javelin {
    ctx: SessionContext,
}

impl Javelin {
    pub fn new(ctx: SessionContext) -> Self {
        Self { ctx }
    }

    pub async fn get_arrow_schema(&self, table: &str) -> Option<SchemaRef> {
        let sr = self
            .ctx
            .catalog("datafusion")?
            .schema("public")?
            .table(table)
            .await?
            .schema();
        Some(sr)
    }

    pub fn get_table_names(&self) -> Option<Vec<String>> {
        let table_names = self
            .ctx
            .catalog("datafusion")?
            .schema("public")?
            .table_names();
        Some(table_names)
    }
}

#[tonic::async_trait]
impl FlightService for Javelin {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Implement handshake"))
    }
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        let mut info_list: Vec<Result<FlightInfo, Status>> = vec![];

        // Get available tables.
        let table_names = match self.get_table_names() {
            Some(names) => names,
            None => return Err(Status::unavailable("No tables available")),
        };

        // Create one FlightInfo per table.
        // TODO: Implement ticketing strategy.
        for t in table_names {
            match self.get_arrow_schema(&t).await {
                Some(s) => match FlightInfo::new().try_with_schema(&s) {
                    Ok(mut info) => {
                        info = info
                            .with_descriptor(FlightDescriptor::new_path(vec![t]))
                            .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new("tkt")));
                        info_list.push(Ok(info))
                    }
                    Err(e) => info_list.push(Err(Status::internal(e.to_string()))),
                },
                None => info_list.push(Err(Status::internal(format!(
                    "Failed to get schema for table: {t}"
                )))),
            }
        }

        // Send FlightInfo to stream.
        if !info_list.is_empty() {
            let output = futures::stream::iter(info_list);
            Ok(Response::new(Box::pin(output) as Self::ListFlightsStream))
        } else {
            Err(Status::internal("Failed to encode schema"))
        }
    }
    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Implement get_flight_info"))
    }
    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Implement get_schema"))
    }
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let query = match std::str::from_utf8(&ticket.ticket) {
            Ok(t) => {
                println!("Recv ticket: {}", t);
                t.to_string()
            }
            Err(e) => return Err(Status::internal(e.to_string())),
        };

        // execute the query
        let df = match self.ctx.sql(&query).await {
            Ok(df) => df,
            Err(e) => return Err(Status::internal(e.to_string())),
        };
        let input_stream = df
            .execute_stream()
            .await
            .unwrap()
            .map_err(|e| FlightError::from(Status::internal(e.to_string())))
            .into_stream();

        let flight_data_stream = FlightDataEncoderBuilder::new().build(input_stream);
        Ok(Response::new(Box::pin(
            flight_data_stream.map_err(Status::from),
        )))
    }

    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Implement do_put"))
    }

    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Implement do_exchange"))
    }

    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Implement do_action"))
    }

    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Implement list_actions"))
    }
}
