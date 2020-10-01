use astarte_sdk::DeviceBuilder;

fn main() {
    let realm = "rbino";
    let device_id = "le5YqVm6TdORty9NqYD8Yw";
    let credentials_secret = "foobar";
    let pairing_url = "https://api.astarte.example.com/pairing";

    let d = DeviceBuilder::new(&realm, &device_id)
        .credentials_secret(credentials_secret)
        .pairing_url(pairing_url)
        .build()
        .unwrap();

    println!("{:?}", d);
}
