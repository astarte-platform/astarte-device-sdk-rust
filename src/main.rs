use astarte_sdk::Device;

fn main() {
    let realm = "rbino";
    let device_id = "le5YqVm6TdORty9NqYD8Yw";
    let credentials_secret = "foobar";

    let d = Device::new(&realm, &device_id, &credentials_secret).unwrap();

    println!("{:?}", d);
}
