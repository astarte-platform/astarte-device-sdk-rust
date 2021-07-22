use astarte_sdk::AstarteOptions;
use std::{fs, path::Path};
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

    let mut sdk_options = AstarteOptions::new(&realm, &device_id, &credentials_secret, &pairing_url);
    sdk_options.add_interface_files(&interfaces_directory);

    let mut device = sdk_options.build().await.unwrap();

    //device.connect().await.unwrap();



    let w = device.clone();
    tokio::task::spawn(async move {
        loop {
            std::thread::sleep(std::time::Duration::from_millis(1000));
            w.publish("/com.test/data",[0x09, 0x00, 0x00, 0x00, 0x08, 0x76, 0x00, 0x01, 0x00]).await.unwrap();
        }
    });

    loop {
        let data = device.poll().await;
    }


}
