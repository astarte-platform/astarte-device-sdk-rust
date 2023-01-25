/*
 * This file is part of Astarte.
 *
 * Copyright 2022 SECO Mind Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

use std::convert::TryInto;
use std::{collections::HashMap, panic};

use base64::Engine;
use chrono::{TimeZone, Utc};
use serde_json::Value;

use astarte_device_sdk::builder::AstarteOptions;
use astarte_device_sdk::types::AstarteType;

fn get_data() -> HashMap<String, AstarteType> {
    let alltypes: Vec<AstarteType> = vec![
        AstarteType::Double(4.5),
        (-4).into(),
        true.into(),
        45543543534_i64.into(),
        "hello".into(),
        b"hello".to_vec().into(),
        TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap().into(),
        vec![1.2, 3.4, 5.6, 7.8].into(),
        vec![1, 3, 5, 7].into(),
        vec![true, false, true, true].into(),
        vec![45543543534_i64, 45543543535_i64, 45543543536_i64].into(),
        vec!["hello".to_owned(), "world".to_owned()].into(),
        vec![b"hello".to_vec(), b"world".to_vec()].into(),
        vec![
            TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap(),
            TimeZone::timestamp_opt(&Utc, 1627580809, 0).unwrap(),
            TimeZone::timestamp_opt(&Utc, 1627580810, 0).unwrap(),
        ]
        .into(),
    ];

    let allendpoints = vec![
        "double",
        "integer",
        "boolean",
        "longinteger",
        "string",
        "binaryblob",
        "datetime",
        "doublearray",
        "integerarray",
        "booleanarray",
        "longintegerarray",
        "stringarray",
        "binaryblobarray",
        "datetimearray",
    ];

    let allendpoints = allendpoints
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

    let data = alltypes
        .iter()
        .cloned()
        .zip(allendpoints.iter().cloned())
        .collect::<Vec<(AstarteType, String)>>();

    let mut data_map = HashMap::new();

    for i in &data {
        data_map.insert(i.1.clone(), i.0.clone());
    }

    data_map
}

fn get_data_obj() -> HashMap<String, AstarteType> {
    let mut data: HashMap<String, AstarteType> = HashMap::new();
    data.insert("latitude".into(), AstarteType::Double(1.34));
    data.insert("longitude".into(), AstarteType::Double(2.34));
    data.insert("altitude".into(), AstarteType::Double(3.34));
    data.insert("accuracy".into(), AstarteType::Double(4.34));
    data.insert("altitudeAccuracy".into(), AstarteType::Double(5.34));
    data.insert("heading".into(), AstarteType::Double(6.34));
    data.insert("speed".into(), AstarteType::Double(7.34));

    data
}

#[tokio::main]
async fn main() {
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        println!("Test failed");
        orig_hook(panic_info);
        std::process::exit(1);
    }));

    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    let realm = "test";
    let device_id = std::env::var("E2E_DEVICE_ID").unwrap();
    let credentials_secret = std::env::var("E2E_CREDENTIALS_SECRET").unwrap();
    let pairing_url = "https://api.autotest.astarte-platform.org/pairing";

    let sdk_options = AstarteOptions::new(realm, &device_id, &credentials_secret, pairing_url)
        .interface_file(std::path::Path::new(
            "./examples/interfaces/org.astarte-platform.test.Everything.json",
        ))
        .unwrap()
        .interface_file(std::path::Path::new(
            "./examples/interfaces/org.astarte-platform.genericsensors.Geolocation.json",
        ))
        .unwrap()
        .interface_file(std::path::Path::new(
            "./examples/interfaces/org.astarte-platform.genericsensors.SamplingRate.json",
        ))
        .unwrap()
        .ignore_ssl_errors()
        .build();

    let mut device = astarte_device_sdk::AstarteDeviceSdk::new(&sdk_options)
        .await
        .unwrap();

    let data = get_data();

    let w = device.clone();
    tokio::task::spawn(async move {
        for _ in 0..3 {
            // individual aggregation
            for i in &data {
                w.send(
                    "org.astarte-platform.test.Everything",
                    &format!("/{}", i.0),
                    i.1.clone(),
                )
                .await
                .unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

        let json = reqwest::Client::new()
            .get(format!(
                "https://api.autotest.astarte-platform.org/appengine/v1/{}/devices/{}/interfaces/org.astarte-platform.test.Everything",
                realm, device_id
            ))
            .header(
                "Authorization",
                "Bearer ".to_string() + &std::env::var("E2E_TOKEN").unwrap(),
            )
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

        check_json(&data, json);

        println!("Test 1 completed successfully");
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

        let data = get_data_obj();

        w.send_object(
            "org.astarte-platform.genericsensors.Geolocation",
            "/45",
            data.clone(),
        )
        .await
        .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

        let json = reqwest::Client::new()
        .get(format!(
            "https://api.autotest.astarte-platform.org/appengine/v1/{}/devices/{}/interfaces/org.astarte-platform.genericsensors.Geolocation",
            realm, device_id
        ))
        .header(
            "Authorization",
            "Bearer ".to_string() + &std::env::var("E2E_TOKEN").unwrap(),
        )
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

        println!("----------------\n{:?}", data);
        println!("----------------\n{:?}", json);

        check_json_obj(&data, json);

        w.send(
            "org.astarte-platform.genericsensors.SamplingRate",
            "/1/enable",
            true,
        )
        .await
        .unwrap();

        w.unset(
            "org.astarte-platform.genericsensors.SamplingRate",
            "/1/enable",
        )
        .await
        .unwrap();

        std::process::exit(0);
    });

    loop {
        match device.handle_events().await {
            Ok(data) => {
                println!("incoming: {:?}", data);
            }
            Err(err) => {
                println!("poll error {:?}", err);
                std::process::exit(1);
            }
        }
    }
}

fn check_json(data: &HashMap<String, AstarteType>, json: String) {
    fn parse_response_json(json: &str) -> HashMap<String, Value> {
        let mut ret = HashMap::new();
        let v: Value = serde_json::from_str(json).unwrap();

        println!("{:#?}", v);

        if let Value::Object(data) = v {
            if let Value::Object(data) = &data["data"] {
                for dat in data {
                    if let Value::Object(dat2) = dat.1 {
                        ret.insert(dat.0.clone(), dat2["value"].clone());
                    }
                }
            }
        }

        ret
    }

    let json = parse_response_json(&json);

    for i in data {
        let atype = &i.1;
        let jtype = &json
            .get(i.0)
            .unwrap_or_else(|| panic!("Can't find {} in json {:?}", i.0, json));

        println!("{:?} {:?}", atype, jtype);
        assert!(compare_json_with_astartetype(atype, jtype));
    }
}

fn compare_json_with_astartetype(astype: &AstarteType, jstype: &Value) -> bool {
    match astype {
        AstarteType::Double(d) => jstype.as_f64().unwrap() == *d,
        AstarteType::Integer(i) => jstype.as_i64().unwrap() == *i as i64,
        AstarteType::Boolean(b) => jstype.as_bool().unwrap() == *b,
        AstarteType::LongInteger(i) => jstype.as_i64().unwrap() == *i,
        AstarteType::String(d) => jstype.as_str().unwrap() == *d,
        AstarteType::BinaryBlob(d) => jstype.as_str().unwrap() == encode_blob(d),
        AstarteType::DateTime(d) => {
            jstype.as_str().unwrap() == d.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
        }
        AstarteType::DoubleArray(d) => jstype
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_f64())
            .zip(d.iter())
            .all(|f| f.0.unwrap() == *f.1),
        AstarteType::IntegerArray(d) => jstype
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_i64())
            .zip(d.iter())
            .all(|f| f.0.unwrap() == *f.1 as i64),
        AstarteType::BooleanArray(d) => jstype
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_bool())
            .zip(d.iter())
            .all(|f| f.0.unwrap() == *f.1),
        AstarteType::LongIntegerArray(d) => jstype
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_i64())
            .zip(d.iter())
            .all(|f| f.0.unwrap() == *f.1),
        AstarteType::StringArray(d) => jstype
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str())
            .zip(d.iter())
            .all(|f| f.0.unwrap() == *f.1),
        AstarteType::BinaryBlobArray(d) => jstype
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str())
            .zip(d.iter())
            .all(|f| f.0.unwrap() == encode_blob(f.1)),
        AstarteType::DateTimeArray(d) => jstype
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str())
            .zip(d.iter())
            .all(|f| f.0.unwrap() == f.1.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
        AstarteType::Unset => todo!(),
    }
}

fn encode_blob(blob: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(blob)
}

fn check_json_obj(data: &HashMap<String, AstarteType>, json: String) {
    fn parse_response_json(json: &str) -> HashMap<String, AstarteType> {
        let mut ret = HashMap::new();
        let v: Value = serde_json::from_str(json).unwrap();

        println!("{:#?}", v);

        if let Value::Object(data) = v {
            if let Value::Object(data) = &data["data"] {
                if let Value::Array(data) = &data["45"] {
                    if let Value::Object(data) = &data[0] {
                        for dat in data {
                            if let Value::Number(dat2) = dat.1 {
                                ret.insert(
                                    dat.0.clone(),
                                    AstarteType::Double(dat2.as_f64().unwrap()),
                                );
                            }
                        }
                    }
                }
            }
        }

        ret
    }

    let json = parse_response_json(&json);

    for i in data {
        let jtype = json
            .get(i.0)
            .unwrap_or_else(|| panic!("Can't find {} in json {:?}", i.0, json));

        println!("{:?} {:?}", i.1, jtype);
        assert!(compare_json_with_astartetype(
            i.1,
            &Value::Number(
                serde_json::Number::from_f64(jtype.clone().try_into().unwrap()).unwrap()
            )
        ));
    }
}

#[cfg(test)]
mod tests {
    use crate::{check_json, get_data};

    #[test]
    fn json() {
        let json = r#"{
            "data":{
               "binaryblob":{
                  "reception_timestamp":"2021-09-10T12:22:42.073Z",
                  "timestamp":"2021-09-10T12:22:42.073Z",
                  "value":"aGVsbG8="
               },
               "binaryblobarray":{
                  "reception_timestamp":"2021-09-10T12:22:42.097Z",
                  "timestamp":"2021-09-10T12:22:42.097Z",
                  "value":[
                     "aGVsbG8=",
                     "d29ybGQ="
                  ]
               },
               "boolean":{
                  "reception_timestamp":"2021-11-11T21:11:38.637Z",
                  "timestamp":"2021-11-11T21:11:36.063Z",
                  "value":true
               },
               "booleanarray":{
                  "reception_timestamp":"2021-09-10T12:22:42.074Z",
                  "timestamp":"2021-09-10T12:22:42.074Z",
                  "value":[
                     true,
                     false,
                     true,
                     true
                  ]
               },
               "datetime":{
                  "reception_timestamp":"2021-09-10T12:22:42.073Z",
                  "timestamp":"2021-09-10T12:22:42.073Z",
                  "value":"2021-07-29T17:46:48.000Z"
               },
               "datetimearray":{
                  "reception_timestamp":"2021-09-10T12:22:42.098Z",
                  "timestamp":"2021-09-10T12:22:42.098Z",
                  "value":[
                     "2021-07-29T17:46:48.000Z",
                     "2021-07-29T17:46:49.000Z",
                     "2021-07-29T17:46:50.000Z"
                  ]
               },
               "double":{
                  "reception_timestamp":"2021-11-11T21:11:38.637Z",
                  "timestamp":"2021-11-11T21:11:36.063Z",
                  "value":4.5
               },
               "doublearray":{
                  "reception_timestamp":"2021-09-10T12:22:42.073Z",
                  "timestamp":"2021-09-10T12:22:42.073Z",
                  "value":[
                     1.2,
                     3.4,
                     5.6,
                     7.8
                  ]
               },
               "integer":{
                  "reception_timestamp":"2021-11-11T21:11:38.637Z",
                  "timestamp":"2021-11-11T21:11:36.063Z",
                  "value":-4
               },
               "integerarray":{
                  "reception_timestamp":"2021-09-10T12:22:42.074Z",
                  "timestamp":"2021-09-10T12:22:42.074Z",
                  "value":[
                     1,
                     3,
                     5,
                     7
                  ]
               },
               "longinteger":{
                  "reception_timestamp":"2022-02-11T15:26:13.483Z",
                  "timestamp":"2022-02-11T15:26:13.483Z",
                  "value":45543543534
               },
               "longintegerarray":{
                  "reception_timestamp":"2021-09-10T12:22:42.097Z",
                  "timestamp":"2021-09-10T12:22:42.097Z",
                  "value":[
                     45543543534,
                     45543543535,
                     45543543536
                  ]
               },
               "string":{
                  "reception_timestamp":"2021-09-10T12:22:42.050Z",
                  "timestamp":"2021-09-10T12:22:42.050Z",
                  "value":"hello"
               },
               "stringarray":{
                  "reception_timestamp":"2021-09-10T12:22:42.097Z",
                  "timestamp":"2021-09-10T12:22:42.097Z",
                  "value":[
                     "hello",
                     "world"
                  ]
               }
            }
         }"#;
        let data = get_data();

        check_json(&data, json.to_string());

        let json = "{\"data\":{\"45\":[{\"accuracy\":4.34,\"altitude\":3.34,\"altitudeAccuracy\":5.34,\"heading\":6.34,\"latitude\":1.34,\"longitude\":2.34,\"speed\":7.34,\"timestamp\":\"2022-03-23T14:43:21.909Z\"}]}}";

        let data = crate::get_data_obj();
        crate::check_json_obj(&data, json.to_string());
    }
}
