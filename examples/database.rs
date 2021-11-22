use astarte_sdk::{builder::AstarteBuilder, database::AstarteSqliteDatabase};
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

    let db = AstarteSqliteDatabase::new("sqlite://astarte-example-db.sqlite")
        .await
        .unwrap();

    let mut sdk_builder =
        AstarteBuilder::new(&realm, &device_id, &credentials_secret, &pairing_url);

    sdk_builder
        .add_interface_files("./examples/interfaces")
        .unwrap()
        .with_database(db);

    sdk_builder.build().await.unwrap();

    let mut device = sdk_builder.connect().await.unwrap();

    let w = device.clone();
    tokio::task::spawn(async move {
        loop {
            w.send(
                "org.astarte-platform.genericsensors.AvailableSensors",
                "/1/name",
                "foo",
            )
            .await
            .unwrap();

            std::thread::sleep(std::time::Duration::from_millis(100));

            w.send(
                "org.astarte-platform.genericsensors.AvailableSensors",
                "/1/name",
                "foo",
            )
            .await
            .unwrap();

            std::thread::sleep(std::time::Duration::from_millis(100));

            w.send(
                "org.astarte-platform.genericsensors.AvailableSensors",
                "/1/name",
                "bar",
            )
            .await
            .unwrap();

            std::thread::sleep(std::time::Duration::from_millis(1000));
        }
    });

    loop {
        match device.poll().await {
            Ok(data) => {
                println!("incoming: {:?}", data);
            }
            Err(err) => log::error!("{:?}", err),
        }
    }
}
