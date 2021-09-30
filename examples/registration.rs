use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Cli {
    // Realm name
    #[structopt(short, long)]
    realm: String,
    // Device id
    #[structopt(short, long)]
    device_id: String,
    // Token
    #[structopt(short, long)]
    token: String,
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
        token,
        pairing_url,
    } = Cli::from_args();

    let credentials_secret =
        astarte_sdk::registration::register_device(&token, &pairing_url, &realm, &device_id)
            .await
            .unwrap();

    println!("{}", credentials_secret);
}
