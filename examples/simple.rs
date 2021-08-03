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
        let mut i: f64 = 0.0;
        loop {
            w.send("com.test.everything", "/double", i).await.unwrap();
            println!("Sent {}", i);

            i += 1.1;

            std::thread::sleep(std::time::Duration::from_millis(1000));
        }
    });

    loop {
        if let Ok(Some(data)) = device.poll().await {
            println!("incoming path: {}, data: {:?}", data.0, data.1);
        }
    }
}
