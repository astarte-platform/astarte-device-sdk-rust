// This file is part of Astarte.
//
// Copyright 2024 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the MQTT connection

use std::{fmt::Debug, io, path::Path, time::Duration};

use async_trait::async_trait;
use log::debug;
use rumqttc::{MqttOptions, NetworkOptions, Transport};
use serde::{Deserialize, Serialize};
use tokio::fs;
use url::Url;

use crate::{
    builder::{ConnectionConfig, DeviceBuilder, DEFAULT_CHANNEL_SIZE},
    store::PropertyStore,
    transport::mqtt::{
        config::connection::TransportProvider, error::MqttError, registration::register_device,
        SessionData,
    },
    Error,
};

use self::tls::is_env_ignore_ssl;

use super::{
    client::AsyncClient, pairing::ApiClient, Mqtt, PairingError, DEFAULT_CONNECTION_TIMEOUT,
    DEFAULT_KEEP_ALIVE,
};

pub(crate) mod connection;
mod tls;

/// File where the credential secret is stored
pub const CREDENTIAL_FILE: &str = "credential";
/// File where the certificate is stored in PEM format
pub const CERTIFICATE_FILE: &str = "certificate.pem";
/// File where the private key is stored in PEM format
pub const PRIVATE_KEY_FILE: &str = "priv-key.der";

/// Credentials for the [`Mqtt`] connection.
#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(untagged)]
pub enum Credential {
    /// Credential secret to authenticate the device
    Secret {
        /// The JWT secret to authenticate the device to astarte.
        credentials_secret: String,
    },
    /// Pairing token to register the device
    ///
    /// ## Note
    ///
    /// You need to set a writable directory on the builder to store the registered credential
    /// secret used for authentication. You can either use the [`DeviceBuilder::writable_dir`] or
    /// [`DeviceBuilder::store_dir`] methods.
    ParingToken {
        /// The JWT secret to pair the device to astarte.
        pairing_token: String,
    },
}

impl Credential {
    /// Create a [`Credential::Secret`]
    pub fn secret(secret: impl Into<String>) -> Self {
        Credential::Secret {
            credentials_secret: secret.into(),
        }
    }

    /// Create a [`Credential::ParingToken`]
    pub fn paring_token(token: impl Into<String>) -> Self {
        Credential::ParingToken {
            pairing_token: token.into(),
        }
    }
}

impl Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "REDACTED")
    }
}

/// Configuration for the mqtt connection
///
/// As a default this configuration:
///
/// - does not ignore SSL errors.
/// - has a keepalive of 30 seconds
/// - has a default bounded channel size of [`crate::builder::DEFAULT_CHANNEL_SIZE`]
#[derive(Debug, Serialize, Deserialize)]
pub struct MqttConfig {
    pub(crate) realm: String,
    pub(crate) device_id: String,
    #[serde(flatten)]
    pub(crate) credential: Credential,
    pub(crate) pairing_url: String,
    pub(crate) ignore_ssl_errors: bool,
    pub(crate) keepalive: Duration,
    pub(crate) conn_timeout: Duration,
    pub(crate) bounded_channel_size: usize,
}

impl MqttConfig {
    /// Create a new instance of MqttConfig
    ///
    /// ```no_run
    /// use astarte_device_sdk::transport::mqtt::{MqttConfig, Credential};
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let realm = "realm_name";
    ///     let device_id = "device_id";
    ///     let credentials_secret = Credential::secret("device_credentials_secret");
    ///     let pairing_url = "astarte_cluster_pairing_url";
    ///
    ///     let mut mqtt_options = MqttConfig::new(realm, device_id, credentials_secret, pairing_url);
    /// }
    /// ```
    pub fn new(
        realm: impl Into<String>,
        device_id: impl Into<String>,
        credential: Credential,
        pairing_url: impl Into<String>,
    ) -> Self {
        Self {
            realm: realm.into(),
            device_id: device_id.into(),
            credential,
            pairing_url: pairing_url.into(),
            ignore_ssl_errors: false,
            keepalive: Duration::from_secs(DEFAULT_KEEP_ALIVE),
            conn_timeout: Duration::from_secs(DEFAULT_CONNECTION_TIMEOUT),
            bounded_channel_size: DEFAULT_CHANNEL_SIZE,
        }
    }

    /// Create a new instance with the given credentials for authentication.
    ///
    /// ```no_run
    /// use astarte_device_sdk::transport::mqtt::MqttConfig;
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let realm = "realm_name";
    ///     let device_id = "device_id";
    ///     let credentials_secret = "device_credentials_secret";
    ///     let pairing_url = "astarte_cluster_pairing_url";
    ///
    ///     let mut mqtt_options =
    ///         MqttConfig::with_credential_secret(realm, device_id, credentials_secret, pairing_url);
    /// }
    /// ```
    pub fn with_credential_secret(
        realm: impl Into<String>,
        device_id: impl Into<String>,
        credentials_secret: impl Into<String>,
        pairing_url: impl Into<String>,
    ) -> Self {
        Self::new(
            realm,
            device_id,
            Credential::secret(credentials_secret),
            pairing_url,
        )
    }

    /// Create a new instance with the given paring token to register the device with.
    ///
    /// ## Note
    ///
    /// Remember to set a writable directory on the builder to store the credentials secret after
    /// registration.
    ///
    /// ```no_run
    /// use astarte_device_sdk::transport::mqtt::MqttConfig;
    /// use astarte_device_sdk::builder::DeviceBuilder;
    ///
    /// # type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync + 'static>>;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()>{
    ///     let realm = "realm_name";
    ///     let device_id = "device_id";
    ///     let pairing_token = "device_credentials_secret";
    ///     let pairing_url = "astarte_cluster_pairing_url";
    ///
    ///     let mut mqtt_options = MqttConfig::with_credential_secret(realm, device_id, pairing_token, pairing_url);
    ///
    ///     let builder = DeviceBuilder::new()
    ///         .store_dir("/some/dir").await?
    ///         .connect(mqtt_options).await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn with_pairing_token(
        realm: impl Into<String>,
        device_id: impl Into<String>,
        pairing_token: impl Into<String>,
        pairing_url: impl Into<String>,
    ) -> Self {
        Self::new(
            realm,
            device_id,
            Credential::paring_token(pairing_token),
            pairing_url,
        )
    }

    /// Configure the keep alive timeout.
    ///
    /// The MQTT broker will be pinged when no data exchange has append
    /// for the duration of the keep alive timeout.
    pub fn keepalive(&mut self, duration: Duration) -> &mut Self {
        self.keepalive = duration;

        self
    }

    /// Ignore TLS/SSL certificate errors.
    pub fn ignore_ssl_errors(&mut self) -> &mut Self {
        self.ignore_ssl_errors = true;

        self
    }

    /// Sets the MQTT connection timeout.
    pub fn connection_timeout(&mut self, conn_timeout: Duration) -> &mut Self {
        self.conn_timeout = conn_timeout;

        self
    }

    /// Sets the size for the underlying bounded channel used by the eventloop of [`rumqttc`].
    pub fn bounded_channel_size(&mut self, bounded_channel_size: usize) -> &mut Self {
        self.bounded_channel_size = bounded_channel_size;

        self
    }

    /// Retrieves the credentials for the connection
    async fn credentials<S, C>(
        &mut self,
        builder: &DeviceBuilder<S, C>,
    ) -> Result<String, MqttError> {
        // We need to clone to not return something owning a mutable reference to self
        match &self.credential {
            Credential::Secret { credentials_secret } => Ok(credentials_secret.clone()),
            Credential::ParingToken { pairing_token } => {
                debug!("pairing token provided, retrieving credentials secret");

                let Some(dir) = &builder.writable_dir else {
                    return Err(MqttError::NoStorePairintToken);
                };

                let secret = self.read_secret_or_register(dir, pairing_token).await?;

                self.credential = Credential::secret(secret.clone());

                Ok(secret)
            }
        }
    }

    /// Register the device and stores the credentials secret in the given directory
    async fn read_secret_or_register(
        &self,
        store_dir: &Path,
        pairing_token: &str,
    ) -> Result<String, PairingError> {
        let credential_file = store_dir.join(CREDENTIAL_FILE);

        match fs::read_to_string(&credential_file).await {
            Ok(secret) => return Ok(secret),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                debug!("no credential file {}", credential_file.display())
            }
            Err(err) => {
                return Err(PairingError::ReadCredential {
                    path: credential_file,
                    backtrace: err,
                });
            }
        }

        let secret = register_device(
            pairing_token,
            &self.pairing_url,
            &self.realm,
            &self.device_id,
        )
        .await?;

        // We can register the device multiple times with the same pairing token if the device
        // hasn't connected. If the call to write the file fails, we will just re-register the
        // device.
        fs::write(&credential_file, &secret).await.map_err(|err| {
            PairingError::WriteCredential {
                path: credential_file,
                backtrace: err,
            }
        })?;

        Ok(secret)
    }

    /// Builds the options to connect to the broker
    async fn build_mqtt_opts(
        &self,
        transport: Transport,
        broker_url: &Url,
    ) -> Result<(MqttOptions, NetworkOptions), PairingError> {
        let client_id = format!("{}/{}", self.realm, self.device_id);

        let host = broker_url
            .host_str()
            .ok_or_else(|| PairingError::Config("missing host in url".to_string()))?;
        let port = broker_url
            .port()
            .ok_or_else(|| PairingError::Config("missing port in url".to_string()))?;

        let mut mqtt_opts = MqttOptions::new(client_id, host, port);

        let keep_alive = self.keepalive.as_secs();
        let conn_timeout = self.conn_timeout.as_secs();
        if keep_alive <= conn_timeout {
            return Err(PairingError::Config(
            format!("Keep alive ({keep_alive}s) should be greater than the connection timeout ({conn_timeout}s)")
        ));
        }

        let mut net_opts = NetworkOptions::new();
        net_opts.set_connection_timeout(conn_timeout);

        mqtt_opts.set_keep_alive(self.keepalive);

        mqtt_opts.set_transport(transport);

        Ok((mqtt_opts, net_opts))
    }
}

#[async_trait]
impl ConnectionConfig for MqttConfig {
    type Con = Mqtt;
    type Err = Error;

    async fn connect<S, C>(mut self, builder: &DeviceBuilder<S, C>) -> Result<Self::Con, Self::Err>
    where
        S: PropertyStore,
        C: Send + Sync,
    {
        let secret = self.credentials(builder).await?;

        let pairing_url = self
            .pairing_url
            .parse()
            .map_err(|err| MqttError::Pairing(PairingError::InvalidUrl(err)))?;

        let insecure_ssl = self.ignore_ssl_errors || is_env_ignore_ssl();
        let provider = TransportProvider::new(
            pairing_url,
            secret.clone(),
            builder.writable_dir.clone(),
            insecure_ssl,
        );

        let client = ApiClient::from_transport(&provider, &self.realm, &self.device_id);

        let borker_url = client.get_broker_url().await.map_err(MqttError::Pairing)?;

        let transport = provider
            .transport(&client)
            .await
            .map_err(MqttError::Pairing)?;

        let (mqtt_opts, net_opts) = self
            .build_mqtt_opts(transport, &borker_url)
            .await
            .map_err(MqttError::Pairing)?;

        debug!("{:?}", mqtt_opts);

        let (client, mut eventloop) = AsyncClient::new(mqtt_opts, self.bounded_channel_size);

        eventloop.set_network_options(net_opts);

        let session_data = SessionData::try_from_props(&builder.interfaces, &builder.store).await?;

        let mut connection = Mqtt::new(self.realm, self.device_id, client, eventloop, provider);
        // to correctly initialize the connection to astarte we should wait for the connack
        connection.wait_for_connack(session_data).await?;

        Ok(connection)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_mqtt_config() {
        let mqtt_config = MqttConfig::with_credential_secret("test", "test", "test", "test");

        assert_eq!(mqtt_config.realm, "test");
        assert_eq!(mqtt_config.device_id, "test");
        assert_eq!(mqtt_config.credential, Credential::secret("test"));
        assert_eq!(mqtt_config.pairing_url, "test");
        assert_eq!(mqtt_config.keepalive, Duration::from_secs(30));
        assert!(!mqtt_config.ignore_ssl_errors);
    }

    #[test]
    fn test_override_mqtt_config() {
        let mut mqtt_config = MqttConfig::with_credential_secret("test", "test", "test", "test");

        mqtt_config
            .ignore_ssl_errors()
            .keepalive(Duration::from_secs(60));

        assert_eq!(mqtt_config.realm, "test");
        assert_eq!(mqtt_config.device_id, "test");
        assert_eq!(mqtt_config.credential, Credential::secret("test"));
        assert_eq!(mqtt_config.pairing_url, "test");
        assert_eq!(mqtt_config.keepalive, Duration::from_secs(60));
        assert!(mqtt_config.ignore_ssl_errors);
    }

    #[test]
    fn test_redacted_credentials_secret() {
        let mqtt_config = MqttConfig::with_credential_secret("test", "test", "secret=", "test");

        let debug_string = format!("{:?}", mqtt_config);

        assert!(!debug_string.contains("secret="));
        assert!(debug_string.contains("REDACTED"));
    }

    #[test]
    fn test_credential_secret_constructors() {
        let secret = Credential::secret("foo");
        assert_eq!(
            secret,
            Credential::Secret {
                credentials_secret: "foo".to_string()
            }
        );
        let token = Credential::paring_token("bar");
        assert_eq!(
            token,
            Credential::ParingToken {
                pairing_token: "bar".to_string()
            }
        );
    }

    #[test]
    fn should_deserialize_credential_secret() {
        let expected = Credential::secret("foo");

        let ser = serde_json::to_string(&expected).unwrap();

        assert_eq!(ser, r#"{"credentials_secret":"foo"}"#);

        let secret: Credential = serde_json::from_str(&ser).unwrap();

        assert_eq!(secret, expected);
    }
}