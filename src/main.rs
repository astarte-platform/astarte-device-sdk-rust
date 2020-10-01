use astarte_sdk::Device;

fn main() {
    let realm = String::from("rbino");
    let device_id = String::from("le5YqVm6TdORty9NqYD8Yw");

    let d = Device::new(&realm, &device_id).unwrap();

    println!("{:?}", d);
}
