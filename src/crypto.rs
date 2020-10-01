use openssl::{
    ec::{EcGroup, EcKey},
    hash::MessageDigest,
    nid::Nid,
    pkey::{PKey, Private},
    x509::{X509NameBuilder, X509Req, X509ReqBuilder},
};

use crate::InitError;

pub struct Bundle(pub PKey<Private>, pub X509Req);

impl Bundle {
    pub fn new(cn: &String) -> Result<Bundle, InitError> {
        let group = EcGroup::from_curve_name(Nid::SECP521R1)?;
        let ec_key = EcKey::generate(&group)?;

        let mut subject_builder = X509NameBuilder::new()?;
        subject_builder.append_entry_by_nid(Nid::COMMONNAME, &cn)?;

        let subject_name = subject_builder.build();

        let pkey = PKey::from_ec_key(ec_key)?;
        let mut req_builder = X509ReqBuilder::new()?;
        req_builder.set_pubkey(&pkey)?;
        req_builder.set_subject_name(&subject_name)?;
        req_builder.sign(&pkey, MessageDigest::sha512())?;

        let req = req_builder.build();

        Ok(Bundle(pkey, req))
    }
}
