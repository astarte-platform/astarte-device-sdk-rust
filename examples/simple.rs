use astarte_sdk::AstarteOptions;
use chrono::TimeZone;
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

    let w = device.clone();
    tokio::task::spawn(async move {
        loop {
            std::thread::sleep(std::time::Duration::from_millis(5000));


            w.send("com.test.everything", "/double", 4.5).await;
            std::thread::sleep(std::time::Duration::from_millis(5));
            w.send("com.test.everything", "/integer", -4).await;
            std::thread::sleep(std::time::Duration::from_millis(5));
            w.send("com.test.everything", "/boolean", true).await;
            std::thread::sleep(std::time::Duration::from_millis(5));
            w.send("com.test.everything", "/longinteger", 45543543534_i64).await;
            std::thread::sleep(std::time::Duration::from_millis(5));
            w.send("com.test.everything", "/string", "hello").await;
            std::thread::sleep(std::time::Duration::from_millis(5));
            w.send("com.test.everything", "/binaryblob", b"hello".to_vec()).await;
            std::thread::sleep(std::time::Duration::from_millis(5));
            w.send("com.test.everything", "/datetime", chrono::Utc.timestamp(1627580808, 0)).await;
            std::thread::sleep(std::time::Duration::from_millis(5));
            w.send("com.test.everything", "/doublearray", vec![1.2,3.4,5.6,7.8]).await;
            std::thread::sleep(std::time::Duration::from_millis(5));
            w.send("com.test.everything", "/integerarray", vec![1,3,5,7]).await;
            std::thread::sleep(std::time::Duration::from_millis(5));
            w.send("com.test.everything", "/booleanarray", vec![true,false,true,true]).await;
            std::thread::sleep(std::time::Duration::from_millis(5));
            w.send("com.test.everything", "/longintegerarray", vec![45543543534_i64,45543543535_i64,45543543536_i64]).await;
            std::thread::sleep(std::time::Duration::from_millis(5));
            w.send("com.test.everything", "/stringarray", vec!["hello".to_owned(),"world".to_owned()]).await;
            std::thread::sleep(std::time::Duration::from_millis(5));
            w.send("com.test.everything", "/binaryblobarray", vec![b"hello".to_vec(),b"world".to_vec()]).await;
            std::thread::sleep(std::time::Duration::from_millis(5));




            let mut lol: std::collections::HashMap<&str, astarte_sdk::types::AstarteType> = std::collections::HashMap::new();
            lol.insert("bottone", true.into());
            lol.insert("uptimeSeconds", 67.into());

            w.send_object_timestamp("com.test4.object", "/pop", lol, None).await;
        }
    });

    loop {
        if let Some(data) = device.poll().await {
            println!("incoming data: {:?}", data);
        }
    }


}
