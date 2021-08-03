use std::vec;

use astarte_sdk::{types::AstarteType, AstarteOptions};
use serde::de::IntoDeserializer;
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
    // Interfaces directory
    #[structopt(short, long)]
    interfaces_directory: String,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let Cli {
        realm,
        device_id,
        credentials_secret,
        pairing_url,
        interfaces_directory,
    } = Cli::from_args();

    let mut sdk_options =
        AstarteOptions::new(&realm, &device_id, &credentials_secret, &pairing_url);
    sdk_options
        .add_interface_files(&interfaces_directory)
        .unwrap();

    let mut device = sdk_options.build().await.unwrap();

    let w = device.clone();

    let alltypes: Vec<AstarteType> = vec![
        (4.5).into(),
        (-4).into(),
        true.into(),
        45543543534_i64.into(),
        "hello".into(),
        b"hello".to_vec().into(),
        chrono::TimeZone::timestamp(&chrono::Utc, 1627580808, 0).into(),
        vec![1.2, 3.4, 5.6, 7.8].into(),
        vec![1, 3, 5, 7].into(),
        vec![true, false, true, true].into(),
        vec![45543543534_i64, 45543543535_i64, 45543543536_i64].into(),
        vec!["hello".to_owned(), "world".to_owned()].into(),
        vec![b"hello".to_vec(), b"world".to_vec()].into(),
        vec![
            chrono::TimeZone::timestamp(&chrono::Utc, 1627580808, 0),
            chrono::TimeZone::timestamp(&chrono::Utc, 1627580809, 0),
            chrono::TimeZone::timestamp(&chrono::Utc, 1627580810, 0),
        ]
        .into(),
    ];

    let allendpoints = vec![
        "double",
        "integer",
        "boolean",
        "longinteger",
        "string",
        "binaryblob",
        "datetime",
        "doublearray",
        "integerarray",
        "booleanarray",
        "longintegerarray",
        "stringarray",
        "binaryblobarray",
        "datetimearray",
    ];

    tokio::task::spawn(async move {
        loop {
            let data = alltypes.iter().zip(allendpoints.iter());

            for i in data {
                w.send("com.test.everything", &format!("/{}", i.1), i.0.clone())
                    .await
                    .unwrap();

                std::thread::sleep(std::time::Duration::from_millis(5));
            }

            let mut lol: std::collections::HashMap<&str, astarte_sdk::types::AstarteType> =
                std::collections::HashMap::new();
            lol.insert("bottone", true.into());
            lol.insert("uptimeSeconds", 67.into());

            w.send_object_timestamp("com.test4.object", "/", lol, None)
                .await
                .unwrap();

            std::thread::sleep(std::time::Duration::from_millis(5000));
        }
    });

    loop {
        if let Ok(Some(data)) = device.poll().await {
            println!("incoming data: {:?}", data);
        }
    }
}
