#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use mqtt_async_client::client::{Client, KeepAlive, QoS};
use rustls::{ClientConfig, OwnedTrustAnchor, RootCertStore};
use std::{fs::File, io::BufReader, time::Duration};

// #[cfg(feature = "tls")]
// use std::io::Cursor;

#[cfg(feature = "tls")]
use rustls;

#[cfg(feature = "tls")]
use webpki_roots;

use crate::config::MqttConfig;

/// create a mqtt client using config properties
pub fn client_from_args(args: &MqttConfig) -> mqtt_async_client::Result<Client> {
    debug!("create client for parameters : {:?}", args);
    let mut b = Client::builder();
    b.set_url_string(&args.url)?
        .set_username(args.username.clone())
        .set_password(args.password.clone().map(|s| s.as_bytes().to_vec()))
        .set_client_id(args.client_id.clone())
        .set_connect_retry_delay(Duration::from_secs(1))
        .set_keep_alive(KeepAlive::from_secs(args.keep_alive))
        .set_operation_timeout(Duration::from_secs(args.op_timeout as u64))
        .set_automatic_connect(true);

    #[cfg(feature = "tls")]
    {
        let cc = if let Some(s) = &args.tls_server_ca_file {
            let mut root_store = RootCertStore::empty();
            let certfile = File::open(s).expect("Cannot open CA file");
            let mut reader = BufReader::new(certfile);

            root_store.add_parsable_certificates(
                &rustls_pemfile::certs(&mut reader).expect("cannot read the certificate"),
            );

            let cc = ClientConfig::builder()
                .with_safe_defaults()
                .with_root_certificates(root_store)
                .with_no_client_auth();

            Some(cc)
        } else if args.tls_mozilla_root_cas {
            let mut root_store = RootCertStore::empty();
            root_store.add_trust_anchors(webpki_roots::TLS_SERVER_ROOTS.iter().map(|ta| {
                OwnedTrustAnchor::from_subject_spki_name_constraints(
                    ta.subject,
                    ta.spki,
                    ta.name_constraints,
                )
            }));

            let cc = rustls::ClientConfig::builder()
                .with_safe_defaults()
                .with_root_certificates(root_store)
                .with_no_client_auth();
            // cc.root_store
            //     .add_server_trust_anchors(&webpki_roots::TLS_SERVER_ROOTS);

            Some(cc)
        } else {
            None
        };

        // let cc = if let Some((crt_file, key_file)) = args
        //     .tls_client_crt_file
        //     .clone()
        //     .zip(args.tls_client_rsa_key_file.clone())
        // {
        //     let cert_bytes = std::fs::read(crt_file)?;
        //     let client_cert = rustls::internal::pemfile::certs(&mut Cursor::new(&cert_bytes[..]))
        //         .map_err(|_| Error::from("Error parsing client cert file"))?[0]
        //         .clone();

        //     let key_bytes = std::fs::read(key_file)?;
        //     let client_key =
        //         rustls::internal::pemfile::rsa_private_keys(&mut Cursor::new(&key_bytes[..]))
        //             .map_err(|_| Error::from("Error parsing client key file"))?[0]
        //             .clone();

        //     let mut cc = cc.unwrap_or_else(rustls::ClientConfig::new);
        //     cc.set_single_client_cert(vec![client_cert], client_key)
        //         .map_err(|e| Error::from(format!("Error setting client cert: {}", e)))?;
        //     Some(cc)
        // } else {
        //     cc
        // };

        if let Some(c) = cc {
            b.set_tls_client_config(c);
        }
    }

    b.build()
}

/// convert integer to QOS enum
pub fn int_to_qos(qos: u8) -> QoS {
    match qos {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => panic!("Not reached"),
    }
}

/**
 * does the evaluated topic contains the tested_topic match
 */
pub fn does_topic_match(tested_topic: &String, evaluated_topic: &String) -> bool {
    let mut tested = tested_topic.clone();
    if tested_topic.ends_with('#') {
        tested = (tested[0..tested.len() - 1]).to_string();
        evaluated_topic.starts_with(&tested)
    } else if tested_topic.eq("") {
        true
    } else {
        evaluated_topic.eq(&tested)
    }
}

/// Test does_topic_match function
#[test]
fn test_does_topic_match() {
    assert!(!does_topic_match(
        &"home".to_string(),
        &"home/toto".to_string()
    ));
    assert!(does_topic_match(
        &"home/#".to_string(),
        &"home/toto".to_string()
    ));
    assert!(!does_topic_match(&"toto".to_string(), &"tutu".to_string()));

    assert!(does_topic_match(&"".to_string(), &"tutu".to_string()));
    assert!(does_topic_match(&"#".to_string(), &"tutu".to_string()));
}
