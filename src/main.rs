use chrono::Utc;
use clap::Parser;
use futures::prelude::*;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server, StatusCode};
use influxdb2::models::{DataPoint, FieldValue::F64};
use influxdb2::Client;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::convert::Infallible;

/// A simple program that listens to raditation data from GQ GMC
/// series of radiation monitors from GQ Electronics LLC.
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// The url of the influxdb server
    #[clap(short = 'h', long)]
    influxdb_host: String,

    /// The influxdb organization
    #[clap(short = 'o', long)]
    influxdb_org: String,

    /// The influxdb token
    #[clap(short = 't', long)]
    influxdb_token: String,

    /// The influxdb bucket
    #[clap(short = 'b', long)]
    influxdb_bucket: String,
}

lazy_static! {
    // making this static is just lazy (no, not a lazy_static joke), need to move this proper DI
    static ref ARGS : Args = Args::parse();
    static ref BUCKET : String = ARGS.influxdb_bucket.clone();
    static ref CLIENT : Client = Client::new(ARGS.influxdb_host.as_str(), ARGS.influxdb_org.as_str(), ARGS.influxdb_token.as_str());
}

async fn hello(request: Request<Body>) -> Result<Response<Body>, Infallible> {
    let params: HashMap<String, String> = request
        .uri()
        .query()
        .map(|v| {
            url::form_urlencoded::parse(v.as_bytes())
                .into_owned()
                .collect()
        })
        .unwrap_or_else(HashMap::new);

    let cpm = params.get("CPM").unwrap().parse::<f64>().unwrap();
    let acpm = params.get("ACPM").unwrap().parse::<f64>().unwrap();
    let usv = params.get("uSV").unwrap().parse::<f64>().unwrap();

    let measurement = "sievert";
    let location = "home";

    let points = vec![
        DataPoint::builder(measurement.clone())
            .tag("location", location.clone())
            .field("cpm", F64(cpm))
            .build()
            .unwrap(),
        DataPoint::builder(measurement.clone())
            .tag("location", location.clone())
            .field("acpm", F64(acpm))
            .build()
            .unwrap(),
        DataPoint::builder(measurement.clone())
            .tag("location", location.clone())
            .field("usv", F64(usv))
            .build()
            .unwrap(),
    ];

    match CLIENT.write(BUCKET.as_str(), stream::iter(points)).await {
        Ok(_) => println!(
            "REQUEST: {},{},{},{}",
            Utc::now().timestamp(),
            cpm,
            acpm,
            usv
        ),
        Err(err) => println!("ERROR: {:?}", err),
    }

    let response = Response::builder()
        .status(StatusCode::OK)
        .body(Body::from(""));
    Ok(response.unwrap())
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    pretty_env_logger::init();

    println!(
        "Using influx: {} with org: {}",
        ARGS.influxdb_host.clone(),
        ARGS.influxdb_bucket.clone()
    );

    // For every connection, we must make a `Service` to handle all
    // incoming HTTP requests on said connection.
    let make_svc = make_service_fn(|_conn| {
        // This is the `Service` that will handle the connection.
        // `service_fn` is a helper to convert a function that
        // returns a Response into a `Service`.
        async { Ok::<_, Infallible>(service_fn(hello)) }
    });

    let addr = ([0, 0, 0, 0], 80).into();

    let server = Server::bind(&addr).http1_keepalive(false).serve(make_svc);

    println!("Listening on http://{}", addr);

    server.await?;

    Ok(())
}
