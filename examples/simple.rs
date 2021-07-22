use astarte_sdk::Device;
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

    let mut device = Device::new(&realm, &device_id, &credentials_secret, &pairing_url);
    device.add_interface_files(&interfaces_directory);


    device.connect().await.unwrap();

    loop {
        std::thread::sleep(std::time::Duration::from_millis(1000));
        device.publish("/com.test/data",[0x09, 0x00, 0x00, 0x00, 0x08, 0x76, 0x00, 0x01, 0x00]).await.unwrap();
    }
}
