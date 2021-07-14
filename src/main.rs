use astarte_sdk::DeviceBuilder;
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
    let Cli {
        realm,
        device_id,
        credentials_secret,
        pairing_url,
        interfaces_directory,
    } = Cli::from_args();

    let mut device_builder = DeviceBuilder::new(&realm, &device_id);
    device_builder.credentials_secret(&credentials_secret);
    device_builder.pairing_url(&pairing_url);
    let interface_files = fs::read_dir(Path::new(&interfaces_directory)).unwrap();
    interface_files
        .filter_map(Result::ok)
        .filter(|f| {
            if let Some(ext) = f.path().extension() {
                ext == "json"
            } else {
                false
            }
        })
        .for_each(|f| {
            device_builder.add_interface_file(&f.path());
        });

    let mut d = device_builder.build().unwrap();

    if let Err(e) = d.connect().await {
        println!("Error: {:?}", e);
        return;
    }

    loop {}
}
