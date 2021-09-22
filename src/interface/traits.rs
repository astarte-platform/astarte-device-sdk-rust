use super::{BaseInterface, BaseMapping, MappingType, Ownership};
use itertools::{EitherOrBoth::*, Itertools};

pub(crate) trait Interface {
    fn base_interface(&self) -> &BaseInterface;

    fn name(&self) -> &str {
        self.base_interface().interface_name.as_ref()
    }

    fn version(&self) -> (i32, i32) {
        let major = self.base_interface().version_major;

        let minor = self.base_interface().version_minor;

        (major, minor)
    }

    fn ownership(&self) -> Ownership {
        self.base_interface().ownership
    }

    fn description(&self) -> Option<&str> {
        self.base_interface().description.as_deref()
    }

    fn doc(&self) -> Option<&str> {
        self.base_interface().doc.as_deref()
    }
}

pub(crate) trait Mapping {
    fn base_mapping(&self) -> &BaseMapping;

    fn endpoint(&self) -> &str {
        self.base_mapping().endpoint.as_ref()
    }

    fn mapping_type(&self) -> MappingType {
        self.base_mapping().mapping_type
    }

    fn description(&self) -> Option<&str> {
        self.base_mapping().description.as_deref()
    }

    fn doc(&self) -> Option<&str> {
        self.base_mapping().doc.as_deref()
    }

    fn is_compatible(&self, path: &str) -> bool {
        if !path.starts_with('/') {
            return false;
        }

        let endpoint_tokens = self.endpoint().trim_start_matches('/').split('/');
        let path_tokens = path.trim_start_matches('/').split('/');

        for pair in endpoint_tokens.zip_longest(path_tokens) {
            match pair {
                // Those two means tokens were not the same size, so not compatible
                Left(_) => return false,
                Right(_) => return false,
                Both(endpoint_token, path_token) => {
                    if endpoint_token.starts_with("%{") && endpoint_token.ends_with('}') {
                        continue;
                    }

                    if endpoint_token != path_token {
                        return false;
                    }
                }
            }
        }

        true
    }
}
