use std::convert::TryInto;

use astarte_sdk::AstarteOptions;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Cli {
    // Realm name
    #[structopt(short, long)]
    realm: String,
    // Device id
    #[structopt(short, long)]
    device_id: String,
    // Credentials secret
    #[structopt(short, long)]
    credentials_secret: String,
    // Pairing URL
    #[structopt(short, long)]
    pairing_url: String,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let Cli {
        realm,
        device_id,
        credentials_secret,
        pairing_url,
    } = Cli::from_args();

    let mut sdk_options =
        AstarteOptions::new(&realm, &device_id, &credentials_secret, &pairing_url);
    sdk_options
        .add_interface_files("./examples/interfaces")
        .unwrap();

    sdk_options.build().await.unwrap();

    let mut device = sdk_options.connect().await.unwrap();

    let w = device.clone();

    tokio::task::spawn(async move {
        loop {
            std::thread::sleep(std::time::Duration::from_millis(5000));

            // object aggregation
            let mut obj: std::collections::HashMap<&str, astarte_sdk::types::AstarteType> =
                std::collections::HashMap::new();
            obj.insert("latitude", 37.534543.try_into().unwrap());
            obj.insert("longitude", 45.543.try_into().unwrap());
            obj.insert("altitude", 650.6.try_into().unwrap());
            obj.insert("accuracy", 12.0.try_into().unwrap());
            obj.insert("altitudeAccuracy", 10.0.try_into().unwrap());
            obj.insert("heading", 237.0.try_into().unwrap());
            obj.insert("speed", 250.0.try_into().unwrap());

            w.send_object_timestamp(
                "org.astarte-platform.genericsensors.Geolocation",
                "/1/",
                obj,
                None,
            )
            .await
            .unwrap();
        }
    });

    loop {
        if let Ok(data) = device.poll().await {
            println!("incoming data: {:?}", data);
        }
    }
}
