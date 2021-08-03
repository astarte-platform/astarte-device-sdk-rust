use std::vec;

use astarte_sdk::{AstarteOptions, types::AstarteType};
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
    tokio::task::spawn(async move {
        loop {
            w.send("com.test.everything", "/double", 4.5).await.unwrap();
            std::thread::sleep(std::time::Duration::from_millis(5));
            w.send("com.test.everything", "/integer", -4).await.unwrap();
            std::thread::sleep(std::time::Duration::from_millis(5));
            w.send("com.test.everything", "/boolean", true)
                .await
                .unwrap();
            std::thread::sleep(std::time::Duration::from_millis(5));
            w.send("com.test.everything", "/longinteger", 45543543534_i64)
                .await
                .unwrap();
            std::thread::sleep(std::time::Duration::from_millis(5));
            w.send("com.test.everything", "/string", "hello")
                .await
                .unwrap();
            std::thread::sleep(std::time::Duration::from_millis(5));
            w.send("com.test.everything", "/binaryblob", b"hello".to_vec())
                .await
                .unwrap();
            std::thread::sleep(std::time::Duration::from_millis(5));
            w.send(
                "com.test.everything",
                "/datetime",
                chrono::TimeZone::timestamp(&chrono::Utc, 1627580808, 0),
            )
            .await
            .unwrap();
            std::thread::sleep(std::time::Duration::from_millis(5));
            w.send(
                "com.test.everything",
                "/doublearray",
                vec![1.2, 3.4, 5.6, 7.8],
            )
            .await
            .unwrap();
            std::thread::sleep(std::time::Duration::from_millis(5));
            w.send("com.test.everything", "/integerarray", vec![1, 3, 5, 7])
                .await
                .unwrap();
            std::thread::sleep(std::time::Duration::from_millis(5));
            w.send(
                "com.test.everything",
                "/booleanarray",
                vec![true, false, true, true],
            )
            .await
            .unwrap();
            std::thread::sleep(std::time::Duration::from_millis(5));
            w.send(
                "com.test.everything",
                "/longintegerarray",
                vec![45543543534_i64, 45543543535_i64, 45543543536_i64],
            )
            .await
            .unwrap();
            std::thread::sleep(std::time::Duration::from_millis(5));
            w.send(
                "com.test.everything",
                "/stringarray",
                vec!["hello".to_owned(), "world".to_owned()],
            )
            .await
            .unwrap();
            std::thread::sleep(std::time::Duration::from_millis(5));
            w.send(
                "com.test.everything",
                "/binaryblobarray",
                vec![b"hello".to_vec(), b"world".to_vec()],
            )
            .await
            .unwrap();
            std::thread::sleep(std::time::Duration::from_millis(5));
            w.send(
                "com.test.everything",
                "/datetimearray",
                vec![
                    chrono::TimeZone::timestamp(&chrono::Utc, 1627580808, 0),
                    chrono::TimeZone::timestamp(&chrono::Utc, 1627580809, 0),
                    chrono::TimeZone::timestamp(&chrono::Utc, 1627580810, 0)],
            )
            .await
            .unwrap();
            std::thread::sleep(std::time::Duration::from_millis(5));



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
