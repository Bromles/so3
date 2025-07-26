use crate::{
    error::Error,
    signature::v4::{X_AMZ_ALGORITHM_NAME, X_AMZ_CONTENT_SHA256_NAME},
};
use axum::body::Body;
use http::{HeaderMap, HeaderName, Request, Uri, header::AUTHORIZATION};

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
