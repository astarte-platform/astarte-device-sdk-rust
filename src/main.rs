use astarte_sdk::DeviceBuilder;
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
    let Cli {
        realm,
        device_id,
        credentials_secret,
        pairing_url,
    } = Cli::from_args();

    let mut d = DeviceBuilder::new(&realm, &device_id)
        .credentials_secret(&credentials_secret)
        .pairing_url(&pairing_url)
        .build()
        .unwrap();

    if let Err(e) = d.obtain_credentials().await {
        println!("Error: {:?}", e);
        return;
    }

    println!("Device: {:?}", d);
}
