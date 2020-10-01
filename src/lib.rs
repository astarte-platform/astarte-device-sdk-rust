mod crypto;

use crypto::Bundle;
use openssl::error::ErrorStack;
use std::fmt;

pub struct Device {
    realm: String,
    device_id: String,
    crypto: Bundle,
}

#[derive(thiserror::Error, Debug)]
pub enum InitError {
    #[error("private key or CSR creation failed")]
    CryptoGeneration(#[from] ErrorStack),
}

impl Device {
    pub fn new(realm: &String, device_id: &String) -> Result<Device, InitError> {
        let cn = format!("{}/{}", realm, device_id);

        let device = Device {
            realm: realm.clone(),
            device_id: device_id.clone(),
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
            .field("private_key", &private_key_pem)
            .field("csr", &csr_pem)
            .finish()
    }
}
