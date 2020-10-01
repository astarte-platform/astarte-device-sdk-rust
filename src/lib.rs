use openssl::{
    ec::{EcGroup, EcKey},
    error::ErrorStack,
    hash::MessageDigest,
    nid::Nid,
    pkey::PKey,
    x509::X509NameBuilder,
    x509::X509ReqBuilder,
};

pub struct Device {
    pub realm: String,
    pub device_id: String,
    pub private_key_pem: String,
    pub csr_pem: String,
}

#[derive(thiserror::Error, Debug)]
pub enum InitError {
    #[error("private key or CSR creation failed")]
    CryptoGeneration(#[from] ErrorStack),
}

impl Device {
    pub fn new(realm: &String, device_id: &String) -> Result<Device, InitError> {
        let group = EcGroup::from_curve_name(Nid::SECP521R1)?;
        let ec_key = EcKey::generate(&group)?;
        let ec_key_pem = ec_key.private_key_to_pem()?;

        let cn = format!("{}/{}", realm, device_id);
        let mut subject_builder = X509NameBuilder::new()?;
        subject_builder.append_entry_by_nid(Nid::COMMONNAME, &cn[..])?;

        let subject_name = subject_builder.build();

        let pkey = PKey::from_ec_key(ec_key)?;
        let mut req_builder = X509ReqBuilder::new()?;
        req_builder.set_pubkey(&pkey)?;
        req_builder.set_subject_name(&subject_name)?;
        req_builder.sign(&pkey, MessageDigest::sha512())?;

        let req_pem = req_builder.build().to_pem()?;

        let device = Device {
            realm: realm.clone(),
            device_id: device_id.clone(),
            private_key_pem: String::from_utf8(ec_key_pem).unwrap(),
            csr_pem: String::from_utf8(req_pem).unwrap(),
        };

        Ok(device)
    }
}
