// This file is part of Astarte.
//
// Copyright 2024 - 2025 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the MQTT connection

use rumqttc::{MqttOptions, NetworkOptions, Transport};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Display},
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::fs;
use tracing::debug;
use url::Url;

use crate::{
    builder::{BuildConfig, Config, ConnectionConfig, DEFAULT_REQUEST_TIMEOUT, DeviceTransport},
    logging::security::{SecurityEvent, notify_security_event},
    store::{StoreCapabilities, wrapper::StoreWrapper},
    transport::mqtt::{
        ClientId, config::transport::TransportProvider, connection::MqttConnection,
        error::MqttError, retention::MqttRetention,
    },
};

use self::tls::is_env_ignore_ssl;

use super::{
    Mqtt, MqttClient, PairingError,
    pairing::{ApiClient, ClientArgs},
};

mod tls;
pub(crate) mod transport;

/// File where the credential secret is stored
pub const CREDENTIAL_FILE: &str = "credential";
/// File where the certificate is stored in PEM format
pub const CERTIFICATE_FILE: &str = "certificate.pem";
/// File where the private key is stored in PEM format
pub const PRIVATE_KEY_FILE: &str = "priv-key.der";

/// Credentials for the [`Mqtt`] connection.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
    /// secret used for authentication. You can set it with the
    /// [`crate::builder::DeviceBuilder::writable_dir`] methods.
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
        match self {
            Credential::Secret { .. } => f
                .debug_struct("Credential::Secret")
                .field("credentials_secret", &"REDACTED")
                .finish(),
            Credential::ParingToken { .. } => f
                .debug_struct("Credential::PairingToken")
                .field("pairing_token", &"REDACTED")
                .finish(),
        }
    }
}

/// Arguments to create the MQTT options.
#[derive(Debug)]
pub struct MqttArgs {
    /// Astarte realm of the device.
    pub realm: String,
    /// Device id.
    pub device_id: String,
    /// Credential to use to connect to Astarte.
    pub credential: Credential,
    /// Astarte pairing url.
    ///
    /// Example <http://api.astarte.localhost/pairing>
    pub pairing_url: Url,
}

/// Configuration for the mqtt connection
///
/// As a default this configuration:
///
/// - does not ignore SSL errors.
/// - has a keepalive of 30 seconds
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MqttConfig {
    pub(crate) realm: String,
    pub(crate) device_id: String,
    #[serde(flatten)]
    pub(crate) credential: Credential,
    pub(crate) pairing_url: Url,
    pub(crate) ignore_ssl_errors: bool,
    pub(crate) keepalive: Duration,
}

pub(crate) struct MqttTransportOptions {
    pub(crate) mqtt_opts: MqttOptions,
    pub(crate) net_opts: NetworkOptions,
    pub(crate) provider: TransportProvider,
}

impl MqttConfig {
    /// Create a new instance of MqttConfig
    ///
    /// ```
    /// use astarte_device_sdk::transport::mqtt::{MqttArgs, MqttConfig, Credential};
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let args = MqttArgs {
    ///         realm: "realm_name".to_string(),
    ///         device_id: "device_id".to_string(),
    ///         credential: Credential::secret("device_credentials_secret"),
    ///         pairing_url: "http://api.astarte.localhost/pairing".parse().expect("should be a valid url"),
    ///     };
    ///
    ///     let mut mqtt = MqttConfig::new(args);
    /// }
    /// ```
    pub fn new(args: MqttArgs) -> Self {
        let MqttArgs {
            realm,
            device_id,
            credential,
            pairing_url,
        } = args;

        Self {
            realm,
            device_id,
            credential,
            pairing_url,
            ignore_ssl_errors: false,
            keepalive: DEFAULT_REQUEST_TIMEOUT,
        }
    }

    /// Configure the keep alive timeout.
    ///
    /// The MQTT broker will be pinged when no data exchange has append
    /// for the duration of the keep alive timeout.
    pub fn keepalive(mut self, duration: Duration) -> Self {
        self.keepalive = duration;

        self
    }

    /// Ignore TLS/SSL certificate errors.
    pub fn ignore_ssl_errors(mut self) -> Self {
        self.ignore_ssl_errors = true;

        self
    }

    /// Retrieves the credentials for the connection
    async fn credentials(&mut self, config: &Config) -> Result<String, MqttError> {
        // We need to clone to not return something owning a mutable reference to self
        match &self.credential {
            Credential::Secret { credentials_secret } => Ok(credentials_secret.clone()),
            Credential::ParingToken { pairing_token } => {
                debug!("pairing token provided, retrieving credentials secret");

                let Some(dir) = &config.writable_dir else {
                    return Err(MqttError::NoStorePairingToken);
                };

                let secret = self
                    .read_secret_or_register(config, dir, pairing_token)
                    .await?;

                self.credential = Credential::secret(secret.clone());

                Ok(secret)
            }
        }
    }

    /// Register the device and stores the credentials secret in the given directory
    async fn read_secret_or_register(
        &self,
        config: &Config,
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

        let insecure_ssl = self.ignore_ssl_errors || is_env_ignore_ssl();

        if insecure_ssl {
            notify_security_event(SecurityEvent::TlsValidationCheckDisabledSuccessfully);
        }

        let tls = if insecure_ssl {
            tls::insecure_tls_config_builder()?.with_no_client_auth()
        } else {
            let roots = tls::read_root_cert_store().await?;
            tls::tls_config_builder(Arc::new(roots))?.with_no_client_auth()
        };

        let client = ApiClient::create(
            ClientArgs {
                realm: &self.realm,
                device_id: &self.device_id,
                pairing_url: &self.pairing_url,
                token: pairing_token,
            },
            config,
            tls,
        )?;

        let secret = client.register_device().await?;

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
    fn build_mqtt_opts(
        &self,
        transport: Transport,
        broker_url: &Url,
        timeout: Duration,
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
        let conn_timeout = timeout.as_secs();
        if keep_alive >= conn_timeout {
            return Err(PairingError::Config(format!(
                "Keep alive ({keep_alive}s) should be less than the connection timeout ({conn_timeout}s)"
            )));
        }

        let mut net_opts = NetworkOptions::new();
        net_opts.set_connection_timeout(conn_timeout);

        mqtt_opts.set_keep_alive(self.keepalive);

        mqtt_opts.set_transport(transport);

        // Set the clean_session since this is the first connection.
        mqtt_opts.set_clean_session(true);

        Ok((mqtt_opts, net_opts))
    }

    pub(crate) async fn try_create_transport(
        &mut self,
        config: &Config,
    ) -> Result<MqttTransportOptions, MqttError> {
        let secret = self.credentials(config).await?;

        let pairing_url = self.pairing_url.clone();

        let insecure_ssl = self.ignore_ssl_errors || is_env_ignore_ssl();

        if insecure_ssl {
            notify_security_event(SecurityEvent::TlsValidationCheckDisabledSuccessfully);
        }

        let provider = TransportProvider::configure(
            pairing_url,
            secret,
            config.writable_dir.clone(),
            insecure_ssl,
        )
        .await
        .map_err(MqttError::Pairing)?;

        let client = ApiClient::from_transport(config, &provider, &self.realm, &self.device_id)
            .map_err(MqttError::Pairing)?;

        let borker_url = client.get_broker_url().await.map_err(MqttError::Pairing)?;

        let transport = provider
            .transport(&client)
            .await
            .map_err(MqttError::Pairing)?;

        let (mqtt_opts, net_opts) = self
            .build_mqtt_opts(transport, &borker_url, config.connection_timeout)
            .map_err(MqttError::Pairing)?;

        debug!("{:?}", mqtt_opts);

        Ok(MqttTransportOptions {
            mqtt_opts,
            net_opts,
            provider,
        })
    }
}

impl<S> ConnectionConfig<S> for MqttConfig
where
    S: StoreCapabilities,
{
    type Conn = Mqtt<Self::Store>;
    type Store = S;
    type Err = MqttError;

    async fn connect(
        self,
        config: BuildConfig<S>,
    ) -> Result<DeviceTransport<Self::Conn>, Self::Err> {
        let BuildConfig { store, state } = config;

        let store_wrapper = StoreWrapper::new(store);
        let client_id = ClientId {
            device_id: self.device_id.clone(),
            realm: self.realm.clone(),
        };

        let (retention_tx, retention_rx) = async_channel::bounded(state.config.channel_size.get());
        let retention = MqttRetention::new(retention_rx);

        let client = MqttClient::without_transport(
            client_id.clone(),
            retention_tx,
            store_wrapper.clone(),
            Arc::clone(&state),
        );

        let connection = MqttConnection::without_transport(
            self.clone(),
            // NOTE pass client to connection so that the [`AsyncClient`] used by the clients can be updated.
            Arc::clone(&client.client),
        );

        let connection = Mqtt::new(
            client_id,
            connection,
            retention,
            store_wrapper.clone(),
            state,
        );

        Ok(DeviceTransport {
            sender: client,
            connection,
            store: store_wrapper,
        })
    }
}

/// Private keys file, to be a type safe when passed to functions.
#[derive(Debug, Clone)]
pub(crate) struct PrivateKeyFile(PathBuf);

impl PrivateKeyFile {
    /// Create a path to the key file.
    pub(crate) fn new(dir: impl AsRef<Path>) -> Self {
        Self(dir.as_ref().join(PRIVATE_KEY_FILE))
    }

    /// Gets the path to the file.
    pub(crate) fn path(&self) -> &Path {
        &self.0
    }
}

impl Display for PrivateKeyFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.display())
    }
}

impl AsRef<Path> for PrivateKeyFile {
    fn as_ref(&self) -> &Path {
        self.path()
    }
}

/// Certificate file, to be a type safe check.
#[derive(Debug, Clone)]
pub(crate) struct CertificateFile(PathBuf);

impl CertificateFile {
    /// Create a path to the certificate file.
    pub(crate) fn new(dir: impl AsRef<Path>) -> Self {
        Self(dir.as_ref().join(CERTIFICATE_FILE))
    }

    /// Gets the path to the file.
    pub(crate) fn path(&self) -> &Path {
        &self.0
    }
}

impl Display for CertificateFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.display())
    }
}

impl AsRef<Path> for CertificateFile {
    fn as_ref(&self) -> &Path {
        self.path()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_mqtt_config() {
        let args = MqttArgs {
            realm: "realm".to_string(),
            device_id: "device_id".to_string(),
            credential: Credential::secret("secret"),
            pairing_url: "http://api.astarte.localhost/pairing".parse().unwrap(),
        };

        let mqtt_config = MqttConfig::new(args);

        let exp = MqttConfig {
            realm: "realm".to_string(),
            device_id: "device_id".to_string(),
            credential: Credential::secret("secret"),
            pairing_url: "http://api.astarte.localhost/pairing".parse().unwrap(),
            ignore_ssl_errors: false,
            keepalive: Duration::from_secs(15),
        };

        assert_eq!(mqtt_config, exp)
    }

    #[test]
    fn test_override_mqtt_config() {
        let args = MqttArgs {
            realm: "realm".to_string(),
            device_id: "device_id".to_string(),
            credential: Credential::secret("secret"),
            pairing_url: "http://api.astarte.localhost/pairing".parse().unwrap(),
        };

        let mqtt_config = MqttConfig::new(args)
            .ignore_ssl_errors()
            .keepalive(Duration::from_secs(60));

        let exp = MqttConfig {
            realm: "realm".to_string(),
            device_id: "device_id".to_string(),
            credential: Credential::secret("secret"),
            pairing_url: "http://api.astarte.localhost/pairing".parse().unwrap(),
            ignore_ssl_errors: true,
            keepalive: Duration::from_secs(60),
        };

        assert_eq!(mqtt_config, exp)
    }

    #[test]
    fn test_redacted_credentials_secret() {
        let args = MqttArgs {
            realm: "realm".to_string(),
            device_id: "device_id".to_string(),
            credential: Credential::secret("secret"),
            pairing_url: "http://api.astarte.localhost/pairing".parse().unwrap(),
        };

        let mqtt_config = MqttConfig::new(args);

        let debug_string = format!("{mqtt_config:?}");

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

    #[test]
    fn check_key_and_cert_file() {
        let key = PrivateKeyFile::new("/foo");
        assert_eq!(key.path(), Path::new("/foo/priv-key.der"));

        let cert = CertificateFile::new("/foo");
        assert_eq!(cert.path(), Path::new("/foo/certificate.pem"));
    }
}
