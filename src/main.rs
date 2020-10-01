use astarte_sdk::Device;

fn main() {
    let realm = String::from("rbino");
    let device_id = String::from("le5YqVm6TdORty9NqYD8Yw");
    let credentials_secret = String::from("foobar");

    let d = Device::new(&realm, &device_id, &credentials_secret).unwrap();

    println!("{:?}", d);
}
