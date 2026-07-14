#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use mqtt_async_client::client::{Client, KeepAlive, QoS};
use rustls::{ClientConfig, RootCertStore};
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
                rustls_pemfile::certs(&mut reader).map(|c| c.unwrap()),
            );

            let cc = ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth();

            Some(cc)
        } else if args.tls_mozilla_root_cas {
            let root_store = rustls::RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
            };
            
            let cc = tokio_rustls::rustls::ClientConfig::builder()             
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
            // add secure options for clients
            log::debug!("Adding secure options for client");
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

/// Whether `evaluated_topic` matches the MQTT filter `tested_topic`.
pub fn does_topic_match(tested_topic: &str, evaluated_topic: &str) -> bool {
    if let Some(prefix) = tested_topic.strip_suffix('#') {
        evaluated_topic.starts_with(prefix)
    } else if tested_topic.is_empty() {
        true
    } else {
        evaluated_topic == tested_topic
    }
}

#[test]
fn test_does_topic_match() {
    assert!(!does_topic_match("home", "home/toto"));
    assert!(does_topic_match("home/#", "home/toto"));
    assert!(!does_topic_match("toto", "tutu"));
    assert!(does_topic_match("", "tutu"));
    assert!(does_topic_match("#", "tutu"));
}
