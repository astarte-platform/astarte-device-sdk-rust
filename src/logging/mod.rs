// This file is part of Astarte.
//
// Copyright 2025 SECO Mind Srl
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

pub(crate) mod security {
    use tracing::Level;

    /// Represents a security device event.
    #[repr(u16)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum SecurityEvent {
        /// Original Description: "Storing/writing certificates in the device failed"
        CertificateWriteFailed = 0,
        /// Original Description: "Stored certificates in the device successfully"
        CertificateStoredSuccessfully = 1,
        /// Original Description: "CSR approved and certificate issued successfully"
        CsrApproved = 2,
        /// Original Description: "Certificate about to expire"
        CertificateAboutToExpire = 3,
        /// Original Description: "CSR is pending for approval"
        CsrPendingApproval = 4,
        /// Original Description: "Certificate Signing request failed"
        CsrFailed = 5,
        /// Original Description: "Certificate validation succeeded"
        CertificateValidationSucceeded = 6,
        /// Original Description: "Certificates transferred to the device successfully"
        CertificateTransferredSuccessfully = 7,
        /// Original Description: "Certificate validation failed - Certificate expired"
        CertificateValidationFailedExpired = 8,
        /// Original Description: "Certificate validation failed"
        CertificateValidationFailed = 9,
        /// Original Description: "Failed to transfer certificates to the device"
        CertificateTransferFailed = 10,
        /// Original Description: "Authentication error received"
        AuthenticationErrorReceived = 11,
        /// Original Description: "Critical operation authentication successful"
        CriticalOperationAuthSuccessful = 12,
        /// Original Description: "Unexpected message received"
        UnexpectedMessageReceived = 13,
        /// Original Description: "Critical operation authentication failed"
        CriticalOperationAuthFailed = 14,
        /// Original Description: "Alarm: unsecure communication"
        AlarmUnsecureCommunication = 15,
        /// Original Description: "Alarm: expired certificate"
        AlarmExpiredCertificate = 16,
        /// Original Description: "Alarm: certificate unavailable"
        AlarmCertificateUnavailable = 17,
        /// Original Description: "TLS certificate validation check disabled successfully"
        TlsValidationCheckDisabledSuccessfully = 18,
        /// Original Description: "SSL Connection failed - Certificate validation failed"
        SslConnectionFailedCertificateValidation = 19,
        /// Original Description: "Alarm: algorithm not supported"
        AlarmAlgorithmNotSupported = 20,
    }

    impl SecurityEvent {
        /// Returns the original string description for the device event.
        fn description(&self) -> &'static str {
            match self {
                SecurityEvent::CertificateWriteFailed => {
                    "Storing/writing certificates in the device failed"
                }
                SecurityEvent::CertificateStoredSuccessfully => {
                    "Stored certificates in the device successfully"
                }
                SecurityEvent::CsrApproved => "CSR approved and certificate issued successfully",
                SecurityEvent::CertificateAboutToExpire => "Certificate about to expire",
                SecurityEvent::CsrPendingApproval => "CSR is pending for approval",
                SecurityEvent::CsrFailed => "Certificate Signing request failed",
                SecurityEvent::CertificateValidationSucceeded => "Certificate validation succeeded",
                SecurityEvent::CertificateTransferredSuccessfully => {
                    "Certificates transferred to the device successfully"
                }
                SecurityEvent::CertificateValidationFailedExpired => {
                    "Certificate validation failed - Certificate expired"
                }
                SecurityEvent::CertificateValidationFailed => "Certificate validation failed",
                SecurityEvent::CertificateTransferFailed => {
                    "Failed to transfer certificates to the device"
                }
                SecurityEvent::AuthenticationErrorReceived => "Authentication error received",
                SecurityEvent::CriticalOperationAuthSuccessful => {
                    "Critical operation authentication successful"
                }
                SecurityEvent::UnexpectedMessageReceived => "Unexpected message received",
                SecurityEvent::CriticalOperationAuthFailed => {
                    "Critical operation authentication failed"
                }
                SecurityEvent::AlarmUnsecureCommunication => "Alarm: unsecure communication",
                SecurityEvent::AlarmExpiredCertificate => "Alarm: expired certificate",
                SecurityEvent::AlarmCertificateUnavailable => "Alarm: certificate unavailable",
                SecurityEvent::TlsValidationCheckDisabledSuccessfully => {
                    "TLS certificate validation check disabled successfully"
                }
                SecurityEvent::SslConnectionFailedCertificateValidation => {
                    "SSL Connection failed - Certificate validation failed"
                }
                SecurityEvent::AlarmAlgorithmNotSupported => "Alarm: algorithm not supported",
            }
        }

        fn level(&self) -> Level {
            match self {
                SecurityEvent::CertificateWriteFailed => Level::ERROR,
                SecurityEvent::CertificateStoredSuccessfully => Level::INFO,
                SecurityEvent::CsrApproved => Level::INFO,
                SecurityEvent::CertificateAboutToExpire => Level::WARN,
                SecurityEvent::CsrPendingApproval => Level::INFO,
                SecurityEvent::CsrFailed => Level::ERROR,
                SecurityEvent::CertificateValidationSucceeded => Level::INFO,
                SecurityEvent::CertificateTransferredSuccessfully => Level::INFO,
                SecurityEvent::CertificateValidationFailedExpired => Level::ERROR,
                SecurityEvent::CertificateValidationFailed => Level::ERROR,
                SecurityEvent::CertificateTransferFailed => Level::ERROR,
                SecurityEvent::AuthenticationErrorReceived => Level::ERROR,
                SecurityEvent::CriticalOperationAuthSuccessful => Level::INFO,
                SecurityEvent::UnexpectedMessageReceived => Level::ERROR,
                SecurityEvent::CriticalOperationAuthFailed => Level::ERROR,
                SecurityEvent::AlarmUnsecureCommunication => Level::WARN,
                SecurityEvent::AlarmExpiredCertificate => Level::ERROR,
                SecurityEvent::AlarmCertificateUnavailable => Level::WARN,
                SecurityEvent::TlsValidationCheckDisabledSuccessfully => Level::WARN,
                SecurityEvent::SslConnectionFailedCertificateValidation => Level::ERROR,
                SecurityEvent::AlarmAlgorithmNotSupported => Level::ERROR,
            }
        }
    }

    pub(crate) fn notify_security_event(event: SecurityEvent) {
        let id = event as u16;
        let message = event.description();

        match event.level() {
            Level::ERROR => tracing::error!(target:"security-event", id, "{message}"),
            Level::WARN => tracing::warn!(target:"security-event", id, "{message}"),
            Level::INFO => tracing::info!(target:"security-event", id, "{message}"),
            Level::DEBUG => tracing::debug!(target:"security-event", id, "{message}"),
            Level::TRACE => tracing::trace!(target:"security-event", id, "{message}"),
        }
    }

    pub(crate) fn notify_tls_error(err: &rumqttc::TlsError) {
        notify_security_event(SecurityEvent::AuthenticationErrorReceived);

        if let rumqttc::TlsError::TLS(tls_error) = err {
            match tls_error {
                rustls::Error::InvalidMessage(
                    rustls::InvalidMessage::UnsupportedCurveType
                    | rustls::InvalidMessage::UnsupportedKeyExchangeAlgorithm(..),
                ) => notify_security_event(SecurityEvent::AlarmAlgorithmNotSupported),
                rustls::Error::InvalidCertificate(..) => {
                    notify_security_event(SecurityEvent::SslConnectionFailedCertificateValidation)
                }
                _ => (),
            }
        }
    }
}
