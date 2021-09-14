use std::collections::HashMap;

use crate::{interface::traits::Mapping, AstarteError, Interface};

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

    pub async fn validate_send(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: &[u8],
        timestamp: &Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), AstarteError> {
        let mapping = self
            .get_mapping(interface_name, interface_path)
            .ok_or_else(|| AstarteError::SendError("Mapping doesn't exist".into()))?;

        let data = crate::AstarteSdk::deserialize(data.to_vec())?;

        match data {
            crate::Aggregation::Individual(individual) => {
                if individual != mapping.mapping_type() {
                    return Err(AstarteError::SendError(
                        "You are sending the wrong type for this mapping".into(),
                    ));
                }
            }
            crate::Aggregation::Object(object) => {
                for obj in object {
                    println!("{:?} {:?}", mapping, obj.1);
                }
            }
        }

        match mapping {
            crate::interface::Mapping::Datastream(map) => {
                if !map.explicit_timestamp && timestamp.is_some() {
                    return Err(AstarteError::SendError(
                        "Do not send timestamp to a mapping without explicit timestamp".into(),
                    ));
                }
            }
            crate::interface::Mapping::Properties(_map) => {}
        }

        Ok(())
    }
}
