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

    let db = AstarteSqliteDatabase::new("sqlite:///tmp/astaerte-example-db.sqlite")
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
        let mut i: i64 = 0;
        loop {
            w.send("com.test.Everything", "/longinteger", i)
                .await
                .unwrap();
            println!("Sent {}", i);

            i += 11;

            std::thread::sleep(std::time::Duration::from_millis(1000));
        }
    });

    loop {
        if let Ok(data) = device.poll().await {
            println!("incoming: {:?}", data);
        }
    }
}
