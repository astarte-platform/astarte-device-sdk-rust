use std::collections::HashMap;

use crate::Interface;

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
}
