// This file is part of Astarte.
//
// Copyright 2024-2026 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the MQTT connection

use astarte_device_error::{Error, ResultExt, WrapError};
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display};
use std::io;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tracing::{debug, error, info, warn};
use url::Url;

use crate::builder::{BuildConfig, ConnectionConfig, DEFAULT_REQUEST_TIMEOUT};
use crate::error::{AstarteError, ErrorKind};
use crate::state::{ConnectionState, SharedState};
use crate::store::StoreCapabilities;
use crate::transport::mqtt::ClientId;
use crate::transport::mqtt::error::MqttError;

use self::transport::safe_write_private;

use super::client::{MqttClient, MqttEncoder};
use super::connection::{Connection, MqttConnection};
use super::pairing::PairingApiError;
use super::pairing::client::{ApiClient, ClientArgs};
use super::pairing::mk_connection::MakeConnection;
use super::retention::RetentionTask;

pub(crate) mod tls;
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
pub struct Mqtt {
    #[serde(flatten)]
    pub(crate) client_id: ClientId,
    #[serde(flatten)]
    pub(crate) credential: Credential,
    pub(crate) pairing_url: Url,
    pub(crate) keepalive: Duration,
}

impl Mqtt {
    /// Create a new instance of Mqtt
    ///
    /// ```
    /// use astarte_device_sdk::transport::mqtt::{MqttArgs, Mqtt, Credential};
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
    ///     let mut mqtt = Mqtt::new(args);
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
            client_id: ClientId { realm, device_id },
            credential,
            pairing_url,
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

    /// Retrieves the credentials for the connection
    pub(crate) async fn credentials<S>(
        &mut self,
        ctx: &ConnectionState<S>,
    ) -> Result<String, Error<PairingApiError>> {
        // We need to clone to not return something owning a mutable reference to self
        match &self.credential {
            Credential::Secret { credentials_secret } => Ok(credentials_secret.clone()),
            Credential::ParingToken { pairing_token } => {
                debug!("pairing token provided, retrieving credentials secret");

                let secret = self.read_secret_or_register(ctx, pairing_token).await?;

                Ok(secret)
            }
        }
    }

    /// Register the device and stores the credentials secret in the given directory
    async fn read_secret_or_register<S>(
        &self,
        state: &ConnectionState<S>,
        pairing_token: &str,
    ) -> Result<String, Error<PairingApiError>> {
        let credential_file = state
            .config()
            .writable_dir
            .as_ref()
            .map(|dir| dir.join(CREDENTIAL_FILE))
            .ok_or(Error::with(
                PairingApiError::InvalidArgument,
                "missing writable dir to store credentials",
            ))?;

        match tokio::fs::read_to_string(&credential_file).await {
            Ok(secret) => {
                info!("secret read from file");

                return Ok(secret);
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                info!("no credential file {}", credential_file.display())
            }
            Err(err) => {
                return Err(Error::with(
                    PairingApiError::Io(err.kind()),
                    "while reading credential file",
                )
                .set_source(err)
                .set_ctx(format!("from {}", credential_file.display())));
            }
        }

        let args = ClientArgs {
            client_id: self.client_id.as_ref(),
            pairing_url: &self.pairing_url,
            token: pairing_token,
        };

        let client = ApiClient::create(args, state.config(), state.tls().clone())?;

        let secret = client.register_device().await?;

        // We can register the device multiple times with the same pairing token if the device
        // hasn't connected. If the call to write the file fails, we will just re-register the
        // device.
        safe_write_private(&credential_file, secret.as_bytes())
            .await
            .wrap_err_with(|err| {
                Error::with(
                    PairingApiError::Io(err.kind()),
                    "while writing credential secret",
                )
                .set_ctx(format!("to {}", credential_file.display()))
            })?;

        Ok(secret)
    }
}

impl ConnectionConfig for Mqtt {
    type Store<S>
        = S
    where
        S: Send + Sync + 'static;

    type Connection = MqttConnection;

    type Client = MqttClient;

    type Encoder = MqttEncoder;

    async fn configure<S>(
        &mut self,
        state: SharedState<S>,
    ) -> Result<BuildConfig<S, Self::Encoder>, AstarteError>
    where
        S: StoreCapabilities,
    {
        #[cfg(debug_assertions)]
        if !self.pairing_url.path().ends_with("/pairing") {
            warn!("Pairing URL doesn't end with `/pairing`")
        }

        Ok(BuildConfig {
            state,
            encoder: MqttEncoder {},
        })
    }

    async fn is_registered<S>(&mut self, state: &SharedState<S>) -> Result<bool, AstarteError>
    where
        S: StoreCapabilities,
    {
        if matches!(self.credential, Credential::Secret { .. }) {
            return Ok(true);
        }

        let credential_file = state
            .config
            .writable_dir
            .as_ref()
            .map(|p| p.join(CREDENTIAL_FILE))
            .ok_or_else(|| {
                Error::with(
                    ErrorKind::Mqtt(MqttError::PairingApi(PairingApiError::InvalidArgument)),
                    "store directory not configured for pairing with token",
                )
            })?;

        tokio::fs::try_exists(&credential_file)
            .await
            .wrap_err_with(|err| {
                Error::with(ErrorKind::Io(err.kind()), "while reading credential file")
                    .set_ctx(credential_file.display().to_string())
            })
    }

    async fn register<S>(
        &mut self,
        state: &ConnectionState<S>,
    ) -> Result<ControlFlow<(Self::Client, Self::Connection)>, AstarteError>
    where
        S: StoreCapabilities,
    {
        let secret = self
            .credentials(state)
            .await
            .map_kind(|k| ErrorKind::Mqtt(MqttError::PairingApi(k)))?;

        let mut mk_conn = MakeConnection {
            keepalive: self.keepalive,
            state,
            args: ClientArgs {
                client_id: self.client_id.as_ref(),
                pairing_url: &self.pairing_url,
                token: &secret,
            },
        };

        // TODO: should check API error codes
        let (client, eventloop) = match mk_conn.create().await.map_kind(MqttError::PairingApi) {
            Ok(connection) => connection,
            Err(error) => {
                error!(%error, "couldn't connect to Astarte");

                return Ok(ControlFlow::Continue(()));
            }
        };

        let (retention_tx, retention_rx) =
            tokio::sync::mpsc::channel(state.config().channel_size.get());

        let retention = RetentionTask::spawn(state.clone(), retention_rx);

        let client = MqttClient {
            id: self.client_id.clone(),
            sender: client,
            retention: retention_tx,
            session_synced: false,
        };

        let connection = MqttConnection {
            config: self.clone(),
            connection: Connection {
                eventloop: sync_wrapper::SyncWrapper::new(eventloop),
                retention,
                retention_joined: false,
            },
        };

        Ok(ControlFlow::Break((client, connection)))
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
    use std::sync::Arc;

    use tempfile::TempDir;

    use crate::interfaces::Interfaces;
    use crate::state::ConnStatus;
    use crate::state::tests::mock_state;
    use crate::store::memory::MemoryStore;
    use crate::transport::mqtt::DEFAULT_KEEP_ALIVE;

    use super::*;

    #[test]
    fn test_default_mqtt_config() {
        let args = MqttArgs {
            realm: "realm".to_string(),
            device_id: "device_id".to_string(),
            credential: Credential::secret("secret"),
            pairing_url: "http://api.astarte.localhost/pairing".parse().unwrap(),
        };

        let mqtt_config = Mqtt::new(args);

        let exp = Mqtt {
            client_id: ClientId {
                realm: "realm".to_string(),
                device_id: "device_id".to_string(),
            },
            credential: Credential::secret("secret"),
            pairing_url: "http://api.astarte.localhost/pairing".parse().unwrap(),
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

        let mqtt_config = Mqtt::new(args).keepalive(Duration::from_secs(60));

        let exp = Mqtt {
            client_id: ClientId {
                realm: "realm".to_string(),
                device_id: "device_id".to_string(),
            },
            credential: Credential::secret("secret"),
            pairing_url: "http://api.astarte.localhost/pairing".parse().unwrap(),
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

        let mqtt_config = Mqtt::new(args);

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

    #[tokio::test]
    async fn should_get_credentials() {
        let temp_dir = TempDir::new().unwrap();
        let mut pairing = Mqtt {
            client_id: ClientId {
                realm: "test".to_string(),
                device_id: "Kwfp-1ahSFOw6fnV1eC46g".to_string(),
            },
            credential: Credential::ParingToken {
                pairing_token: "paring-token".to_string(),
            },
            pairing_url: "http://api.astarte.host/pairing".parse().unwrap(),
            keepalive: DEFAULT_KEEP_ALIVE,
        };

        let exp = "credential-secret";
        tokio::fs::write(temp_dir.path().join("credential"), exp)
            .await
            .unwrap();

        let (status_tx, _status_rx) = tokio::sync::watch::channel(ConnStatus::Offline);
        let mut shared_state = mock_state(MemoryStore::new(), status_tx, Interfaces::new());
        shared_state.config.writable_dir = Some(temp_dir.path().to_path_buf());
        let ctx = ConnectionState::new(Arc::new(shared_state));

        let res = pairing.credentials(&ctx).await.unwrap();

        assert_eq!(res, exp);
    }
}
