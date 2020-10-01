mod crypto;

use crypto::Bundle;
use openssl::error::ErrorStack;
use std::fmt;

pub struct Device {
    realm: String,
    device_id: String,
    credentials_secret: String,
    crypto: Bundle,
}

#[derive(thiserror::Error, Debug)]
pub enum InitError {
    #[error("private key or CSR creation failed")]
    CryptoGeneration(#[from] ErrorStack),
}

impl Device {
    pub fn new(realm: &str, device_id: &str, credential_secret: &str) -> Result<Device, InitError> {
        let cn = format!("{}/{}", realm, device_id);

        let device = Device {
            realm: String::from(realm),
            device_id: String::from(device_id),
            credentials_secret: String::from(credential_secret),
            crypto: Bundle::new(&cn)?,
        };

        Ok(device)
    }
}

impl fmt::Debug for Device {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Bundle(priv_key, csr) = &self.crypto;

        let private_key_bytes = priv_key.private_key_to_pem_pkcs8()?;
        let private_key_pem = String::from_utf8(private_key_bytes).unwrap();

        let csr_bytes = csr.to_pem()?;
        let csr_pem = String::from_utf8(csr_bytes).unwrap();

        f.debug_struct("Device")
            .field("realm", &self.realm)
            .field("device_id", &self.device_id)
            .field("credentials_secret", &self.credentials_secret)
            .field("private_key", &private_key_pem)
            .field("csr", &csr_pem)
            .finish()
    }
}
