use std::collections::HashMap;

use crate::{interface::traits::Mapping, types::AstarteType, AstarteError, Interface};

#[derive(Clone)]
pub struct Interfaces {
    pub interfaces: HashMap<String, Interface>,
}

impl Interfaces {
    pub fn new(interfaces: HashMap<String, Interface>) -> Self {
        Interfaces { interfaces }
    }

    pub fn get_introspection_string(&self) -> String {
        use crate::interface::traits::Interface;

        let mut introspection: String = self
            .interfaces
            .iter()
            .map(|f| format!("{}:{}:{};", f.0, f.1.version().0, f.1.version().1))
            .collect();
        introspection.pop(); // remove last ";"
        introspection
    }

    pub fn get_mapping(
        &self,
        interface_name: &str,
        interface_path: &str,
    ) -> Option<crate::interface::Mapping> {
        self.interfaces
            .iter()
            .find(|i| i.0 == interface_name)
            .and_then(|f| f.1.mapping(interface_path))
    }

    pub fn get_mqtt_reliability(&self, interface_name: &str, interface_path: &str) -> rumqttc::QoS {
        use rumqttc::QoS;

        let mapping = self.get_mapping(interface_name, interface_path);

        let reliability = match mapping {
            Some(crate::interface::Mapping::Datastream(m)) => m.reliability,
            _ => Default::default(),
        };

        match reliability {
            crate::interface::Reliability::Unreliable => QoS::AtMostOnce,
            crate::interface::Reliability::Guaranteed => QoS::AtLeastOnce,
            crate::interface::Reliability::Unique => QoS::ExactlyOnce,
        }
    }

    /// returns major version if the property exists, None otherwise
    pub fn get_property_major(&self, ifpath: &str) -> Option<i32> {
        // todo: this could be optimized
        self.interfaces
            .iter()
            .map(|f| f.1.get_properties_paths())
            .flatten()
            .filter(|f| f.0 == *ifpath)
            .map(|f| f.1)
            .next()
    }

    pub fn validate_float(data: &AstarteType) -> Result<(), AstarteError> {
        fn validate_float(d: &f64) -> Result<(), AstarteError> {
            let error = Err(AstarteError::SendError(
                "You are sending the wrong type for this mapping".into(),
            ));

            if d.is_infinite() || d.is_nan() || d.is_subnormal() {
                error
            } else {
                Ok(())
            }
        }

        match data {
            AstarteType::Double(d) => validate_float(d)?,
            AstarteType::DoubleArray(d) => {
                for n in d {
                    validate_float(n)?
                }
            }
            _ => {}
        };

        Ok(())
    }

    pub fn validate_send(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: &[u8],
        timestamp: &Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), AstarteError> {
        let data_deserialized = crate::AstarteSdk::deserialize(data)?;

        let interface = self
            .interfaces
            .get(interface_name)
            .ok_or_else(|| AstarteError::SendError("Interface does not exists".into()))?;

        match data_deserialized {
            crate::Aggregation::Individual(individual) => {
                let mapping = self
                    .get_mapping(interface_name, interface_path)
                    .ok_or_else(|| AstarteError::SendError("Mapping doesn't exist".into()))?;

                if individual != mapping.mapping_type() {
                    return Err(AstarteError::SendError(format!(
                        "You are sending the wrong type for this mapping: got {:?}, expected {:?}",
                        individual,
                        mapping.mapping_type()
                    )));
                }

                Interfaces::validate_float(&individual)?;

                match mapping {
                    crate::interface::Mapping::Datastream(map) => {
                        if !map.explicit_timestamp && timestamp.is_some() {
                            return Err(AstarteError::SendError(
                                "Do not send timestamp to a mapping without explicit timestamp"
                                    .into(),
                            ));
                        }
                    }
                    crate::interface::Mapping::Properties(map) => {
                        if data.is_empty() && !map.allow_unset {
                            return Err(AstarteError::SendError(
                                "Do not unset a mapping without allow_unset".into(),
                            ));
                        }
                    }
                }
            }
            crate::Aggregation::Object(object) => {
                for obj in &object {
                    Interfaces::validate_float(obj.1)?;

                    let mapping = self
                        .get_mapping(interface_name, &format!("{}{}", interface_path, obj.0))
                        .ok_or_else(|| AstarteError::SendError("Mapping doesn't exist".into()))?;

                    if *obj.1 != mapping.mapping_type() {
                        return Err(AstarteError::SendError(
                            format!("You are sending the wrong type for this object mapping: got {:?}, expected {:?}", *obj.1, mapping.mapping_type()),
                        ));
                    }

                    match mapping {
                        crate::interface::Mapping::Datastream(map) => {
                            if !map.explicit_timestamp && timestamp.is_some() {
                                return Err(AstarteError::SendError(
                                    "Do not send timestamp to a mapping without explicit timestamp"
                                        .into(),
                                ));
                            }
                        }
                        crate::interface::Mapping::Properties(_) => {
                            return Err(AstarteError::SendError(
                                "Can't send object to properties".into(),
                            ));
                        }
                    }
                }

                if object.len() < interface.mappings_len() {
                    return Err(AstarteError::SendError(
                        "You are missing some mappings from the object".into(),
                    ));
                }
            }
        }

        Ok(())
    }

    pub fn validate_receive(
        &self,
        interface_name: &str,
        interface_path: &str,
        bdata: Vec<u8>,
    ) -> Result<(), AstarteError> {
        let interface = self
            .interfaces
            .get(interface_name)
            .ok_or_else(|| AstarteError::ReceiveError("Interface does not exists".into()))?;

        let data = crate::AstarteSdk::deserialize(&bdata)?;

        match data {
            crate::Aggregation::Individual(individual) => {
                let mapping = self
                    .get_mapping(interface_name, interface_path)
                    .ok_or_else(|| AstarteError::ReceiveError("Mapping doesn't exist".into()))?;

                match mapping {
                    crate::interface::Mapping::Datastream(_) => {}
                    crate::interface::Mapping::Properties(map) => {
                        if bdata.is_empty() && !map.allow_unset {
                            return Err(AstarteError::ReceiveError(
                                "Do not unset a mapping without allow_unset".into(),
                            ));
                        }
                    }
                }

                if individual != mapping.mapping_type() {
                    return Err(AstarteError::ReceiveError(
                        "You are receiving the wrong type for this mapping".into(),
                    ));
                }

                Interfaces::validate_float(&individual)?;
            }
            crate::Aggregation::Object(object) => {
                for obj in &object {
                    Interfaces::validate_float(obj.1)?;

                    let mapping = self
                        .get_mapping(interface_name, &format!("{}{}", interface_path, obj.0))
                        .ok_or_else(|| {
                            AstarteError::ReceiveError("Mapping doesn't exist".into())
                        })?;

                    if *obj.1 != mapping.mapping_type() {
                        return Err(AstarteError::ReceiveError(
                            "You are receiving the wrong type for this object mapping".into(),
                        ));
                    }

                    if let crate::interface::Mapping::Properties(_) = mapping {
                        return Err(AstarteError::ReceiveError(
                            "Can't send object to properties".into(),
                        ));
                    }
                }

                if object.len() < interface.mappings_len() {
                    return Err(AstarteError::ReceiveError(
                        "You are missing some mappings from the object".into(),
                    ));
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::convert::TryInto;

    use crate::{types::AstarteType, AstarteOptions, AstarteSdk};

    #[test]
    fn test_individual() {
        let mut options = AstarteOptions::new("test", "test", "test", "test");
        options.add_interface_files("examples/interfaces/").unwrap();
        let ifa = super::Interfaces::new(options.interfaces);

        let buf = AstarteSdk::serialize_individual(AstarteType::Boolean(true), None).unwrap();

        ifa.validate_send("com.test.Everything", "/boolean", &buf, &None)
            .unwrap();
        ifa.validate_send("com.test.Everything", "/double", &buf, &None)
            .unwrap_err();
        ifa.validate_send("com.test.Everything", "/booleanarray", &buf, &None)
            .unwrap_err();
        ifa.validate_send("com.test.Everything", "/fake", &buf, &None)
            .unwrap_err();
        ifa.validate_send("com.fake.fake", "/fake", &buf, &None)
            .unwrap_err();
        ifa.validate_send("com.fake.fake", "/boolean", &buf, &None)
            .unwrap_err();

        let timestamp = Some(chrono::TimeZone::timestamp(&chrono::Utc, 1537449422, 0));

        ifa.validate_send("com.test.Everything", "/boolean", &buf, &timestamp)
            .unwrap();
        ifa.validate_send("com.test.Everything", "/double", &buf, &timestamp)
            .unwrap_err();
        ifa.validate_send("com.test.Everything", "/booleanarray", &buf, &timestamp)
            .unwrap_err();
        ifa.validate_send("com.test.Everything", "/fake", &buf, &timestamp)
            .unwrap_err();
        ifa.validate_send("com.fake.fake", "/fake", &buf, &timestamp)
            .unwrap_err();
        ifa.validate_send("com.fake.fake", "/boolean", &buf, &timestamp)
            .unwrap_err();

        let buf = AstarteSdk::serialize_individual(AstarteType::Double(f64::NAN), None).unwrap(); // NaN

        ifa.validate_send("com.test.Everything", "/double", &buf, &None)
            .unwrap_err();

        let buf = AstarteSdk::serialize_individual(
            AstarteType::DoubleArray(vec![1.0, 2.0, f64::NAN, 4.0]),
            None,
        )
        .unwrap(); // NaN

        ifa.validate_send("com.test.Everything", "/double", &buf, &None)
            .unwrap_err();
    }

    #[test]
    fn test_object() {
        let mut options = AstarteOptions::new("test", "test", "test", "test");
        options.add_interface_files("examples/interfaces/").unwrap();
        let ifa = super::Interfaces::new(options.interfaces);

        let mut obj: std::collections::HashMap<&str, AstarteType> =
            std::collections::HashMap::new();
        obj.insert("latitude", 37.534543.try_into().unwrap());
        obj.insert("longitude", 45.543.try_into().unwrap());
        obj.insert("altitude", 650.6.try_into().unwrap());
        obj.insert("accuracy", 12.0.try_into().unwrap());
        obj.insert("altitudeAccuracy", 10.0.try_into().unwrap());
        obj.insert("heading", 237.0.try_into().unwrap());
        obj.insert("speed", 250.0.try_into().unwrap());

        let buf = AstarteSdk::serialize_object(obj.clone(), None).unwrap();

        ifa.validate_send(
            "org.astarte-platform.genericsensors.Geolocation",
            "/1/",
            &buf,
            &None,
        )
        .unwrap();

        ifa.validate_send(
            "org.astarte-platform.genericsensors.Geolocation",
            "/1/2/3/",
            &buf,
            &None,
        )
        .unwrap_err();

        ifa.validate_send("org.doesnotexists.doesnotexists", "/1/", &buf, &None)
            .unwrap_err();

        // nonexisting object field
        let mut obj2 = obj.clone();
        obj2.insert("latitudef", 37.534543.try_into().unwrap());
        let buf = AstarteSdk::serialize_object(obj2, None).unwrap();
        ifa.validate_send(
            "org.astarte-platform.genericsensors.Geolocation",
            "/1/",
            &buf,
            &None,
        )
        .unwrap_err();

        // wrong type
        let mut obj2 = obj.clone();
        obj2.insert("latitude", AstarteType::Boolean(false));
        let buf = AstarteSdk::serialize_object(obj2, None).unwrap();
        ifa.validate_send(
            "org.astarte-platform.genericsensors.Geolocation",
            "/1/",
            &buf,
            &None,
        )
        .unwrap_err();

        // missing object field
        let mut obj2 = obj.clone();
        obj2.remove("latitude");
        let buf = AstarteSdk::serialize_object(obj2, None).unwrap();
        ifa.validate_send(
            "org.astarte-platform.genericsensors.Geolocation",
            "/1/",
            &buf,
            &None,
        )
        .unwrap_err();

        // invalid float
        let mut obj2 = obj.clone();
        obj2.insert("latitude", AstarteType::Double(f64::NAN));
        let buf = AstarteSdk::serialize_object(obj2, None).unwrap();
        ifa.validate_send(
            "org.astarte-platform.genericsensors.Geolocation",
            "/1/",
            &buf,
            &None,
        )
        .unwrap_err();
    }
}
