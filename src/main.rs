use chrono::Utc;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use std::collections::HashMap;
use std::convert::Infallible;
use std::fs::OpenOptions;
use std::io::Write;

async fn hello(request: Request<Body>) -> Result<Response<Body>, Infallible> {
    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open("radiation.csv")
        .unwrap();

    let params: HashMap<String, String> = request
        .uri()
        .query()
        .map(|v| {
            url::form_urlencoded::parse(v.as_bytes())
                .into_owned()
                .collect()
        })
        .unwrap_or_else(HashMap::new);

    let line = format!(
        "{},{},{},{}",
        Utc::now().timestamp(),
        params.get("CPM").unwrap(),
        params.get("ACPM").unwrap(),
        params.get("uSV").unwrap()
    );
    if let Err(e) = writeln!(file, "{}", line) {
        eprintln!("Couldn't write to file: {}", e);
    }
    println!("REQUEST: {:?}", line);
    Ok(Response::new(Body::from("")))
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    pretty_env_logger::init();

    // For every connection, we must make a `Service` to handle all
    // incoming HTTP requests on said connection.
    let make_svc = make_service_fn(|_conn| {
        // This is the `Service` that will handle the connection.
        // `service_fn` is a helper to convert a function that
        // returns a Response into a `Service`.
        async { Ok::<_, Infallible>(service_fn(hello)) }
    });

    let addr = ([0, 0, 0, 0], 80).into();

    let server = Server::bind(&addr).serve(make_svc);

    println!("Listening on http://{}", addr);

    server.await?;

    Ok(())
}
