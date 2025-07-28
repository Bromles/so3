use std::collections::HashMap;

use crate::{
    error::Error,
    signature::v4::{
        AWS4_HMAC_SHA256_NAME, EMPTY_BODY_SHA256, X_AMZ_ALGORITHM_NAME, X_AMZ_CONTENT_SHA256_NAME,
        X_AMZ_CREDENTIAL_NAME, X_AMZ_DATE_NAME, X_AMZ_EXPIRES_NAME, X_AMZ_SIGNATURE_NAME,
        X_AMZ_SIGNED_HEADERS_NAME,
    },
};
use axum::body::Body;
use chrono::{DateTime, Duration, NaiveDateTime, TimeZone, Utc};
use http::{
    HeaderMap, HeaderName, Request, Uri,
    header::{AUTHORIZATION, HOST},
};

pub fn parse_query(uri: &Uri) -> Result<HeaderMap<(String, String)>, Error> {
    let mut query = HeaderMap::with_capacity(0);

    if let Some(query_string) = uri.query() {
        let pairs = url::form_urlencoded::parse(query_string.as_bytes());

        for (key, val) in pairs {
            let name = HeaderName::from_bytes(key.as_bytes()).unwrap();

            let value = (key.to_string(), val.to_string());

            if query.insert(name, value).is_some() {
                return Err(Error::Default);
            }
        }
    }

    Ok(query)
}

pub fn check_payload_signature(req: &mut Request<Body>) -> Result<(), Error> {
    let query_data = parse_query(req.uri())?;

    if query_data.contains_key(&X_AMZ_ALGORITHM_NAME) {
        //check presigned
        Ok(())
    } else if req.headers().contains_key(&AUTHORIZATION) {
        // check standard
        Ok(())
    } else {
        let _ = req
            .headers()
            .get(&X_AMZ_CONTENT_SHA256_NAME)
            .map(|v| v.to_str())
            .transpose()
            .unwrap();

        Ok(())
    }
}

pub struct Auth {
    key_id: String,
    scope: String,
    signed_headers: String,
    signature: String,
    content_sha256: String,
    date: DateTime<Utc>,
}

impl Auth {
    fn parse_header(headers: &HeaderMap) -> Result<Self, Error> {
        let auth = headers.get(AUTHORIZATION).unwrap().to_str().unwrap();

        let (auth_kind, rest) = auth.split_once(' ').unwrap();

        if auth_kind != AWS4_HMAC_SHA256_NAME {
            return Err(Error::Default);
        }

        let mut auth_params = HashMap::new();

        for auth_part in rest.split(',') {
            let auth_part = auth_part.trim();
            let eq = auth_part.find('=').unwrap();

            let (key, value) = auth_part.split_at(eq);

            auth_params.insert(key.to_string(), value.trim_start_matches('=').to_string());
        }

        let cred = auth_params.get("Credential").unwrap();
        let signed_headers = auth_params.get("SignedHeaders").unwrap().to_string();
        let signature = auth_params.get("Signature").unwrap().to_string();

        let content_sha256 = headers
            .get(X_AMZ_CONTENT_SHA256_NAME)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let date = headers.get(X_AMZ_DATE_NAME).unwrap().to_str().unwrap();
        let date = parse_date(date).unwrap();

        if Utc::now() - date > Duration::hours(24) {
            return Err(Error::Default);
        }

        let (key_id, scope) = parse_credential(cred).unwrap();

        Ok(Self {
            key_id,
            scope,
            signed_headers,
            signature,
            content_sha256,
            date,
        })
    }

    fn parse_presigned(
        algorithm: &str,
        query_data: &HeaderMap<(String, String)>,
    ) -> Result<Self, Error> {
        if algorithm != AWS4_HMAC_SHA256_NAME {
            return Err(Error::Default);
        }

        let cred = query_data.get(&X_AMZ_CREDENTIAL_NAME).unwrap();
        let signed_headers = query_data.get(&X_AMZ_SIGNED_HEADERS_NAME).unwrap();
        let signature = query_data.get(&X_AMZ_SIGNATURE_NAME).unwrap();

        let duration = query_data
            .get(&X_AMZ_EXPIRES_NAME)
            .unwrap()
            .1
            .parse()
            .unwrap();

        if duration > 7 * 24 * 60 * 60 {
            return Err(Error::Default);
        }

        let date = query_data.get(&X_AMZ_DATE_NAME).unwrap();
        let date = parse_date(&date.1).unwrap();

        if Utc::now() - date > Duration::seconds(duration) {
            return Err(Error::Default);
        }

        let (key_id, scope) = parse_credential(&cred.1).unwrap();

        Ok(Self {
            key_id,
            scope,
            signed_headers: signed_headers.1.clone(),
            signature: signature.1.clone(),
            content_sha256: EMPTY_BODY_SHA256.to_string(),
            date,
        })
    }
}

pub const SHORT_DATE: &str = "%Y%m%d";
pub const LONG_DATETIME: &str = "%Y%m%dT%H%M%SZ";

fn parse_date(date: &str) -> Result<DateTime<Utc>, Error> {
    let date: NaiveDateTime = NaiveDateTime::parse_from_str(date, LONG_DATETIME).unwrap();
    Ok(Utc.from_utc_datetime(&date))
}

fn parse_credential(cred: &str) -> Result<(String, String), Error> {
    let first_slash = cred.find('/').unwrap();

    let (key_id, scope) = cred.split_at(first_slash);

    Ok((
        key_id.to_string(),
        scope.trim_start_matches('/').to_string(),
    ))
}

fn split_signed_headers(auth: &Auth) -> Result<Vec<HeaderName>, Error> {
    let mut signed_headers = auth
        .signed_headers
        .split(';')
        .map(HeaderName::try_from)
        .collect::<Result<Vec<HeaderName>, _>>()
        .unwrap();

    signed_headers.sort_by(|h1, h2| h1.as_str().cmp(h2.as_str()));

    Ok(signed_headers)
}

fn verify_signed_headers(headers: &HeaderMap, signed_headers: &[HeaderName]) -> Result<(), Error> {
    if !signed_headers.contains(&HOST) {
        return Err(Error::Default);
    }

    for (name, _) in headers.iter() {
        if name.as_str().starts_with("x-amz-") {
            if !signed_headers.contains(name) {
                return Err(Error::Default);
            }
        }
    }

    Ok(())
}

pub fn check_presigned(
    req: &mut Request<Body>,
    mut query_data: HeaderMap<(String, String)>,
) -> Result<(), Error> {
    let algorithm = query_data.get(&X_AMZ_ALGORITHM_NAME).unwrap();
    let auth = Auth::parse_presigned(&algorithm.1, &query_data).unwrap();

    let signed_headers = split_signed_headers(&auth).unwrap();
    verify_signed_headers(req.headers(), &signed_headers).unwrap();

    query_data.remove(&X_AMZ_SIGNATURE_NAME);
    
    
}
