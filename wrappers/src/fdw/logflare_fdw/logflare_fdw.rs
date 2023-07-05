use pgrx::{
    pg_sys,
    prelude::{AnyNumeric, Date, PgSqlErrorCode, Timestamp},
};
use reqwest::{self, header, StatusCode, Url};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde_json::value::Value as JsonValue;
use std::collections::HashMap;
use std::str::FromStr;

use supabase_wrappers::prelude::*;

fn create_client(api_key: &str) -> ClientWithMiddleware {
    let mut headers = header::HeaderMap::new();
    let header_name = header::HeaderName::from_static("x-api-key");
    let mut auth_value = header::HeaderValue::from_str(api_key).unwrap();
    auth_value.set_sensitive(true);
    headers.insert(header_name, auth_value);
    let client = reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .unwrap();
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
    ClientBuilder::new(client)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build()
}

macro_rules! report_request_error {
    ($err:ident) => {{
        report_error(
            PgSqlErrorCode::ERRCODE_FDW_ERROR,
            &format!("request failed: {}", $err),
        );
        return;
    }};
}

macro_rules! column_type_mismatch {
    ($col:ident) => {
        panic!("column '{}' data type not match", $col.name)
    };
}

#[wrappers_fdw(
    version = "0.1.0",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/logflare_fdw"
)]
pub(crate) struct LogflareFdw {
    rt: Runtime,
    base_url: Url,
    client: Option<ClientWithMiddleware>,
    scan_result: Option<Vec<Row>>,
}

impl LogflareFdw {
    const BASE_URL: &str = "https://api.logflare.app/api/endpoints/query/";

    fn resp_to_rows(&mut self, body: &JsonValue, tgt_cols: &[Column]) {
        self.scan_result = body
            .as_object()
            .and_then(|v| v.get("result"))
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|record| {
                        let mut row = Row::new();
                        if let Some(r) = record.as_object() {
                            for tgt_col in tgt_cols {
                                let cell = if tgt_col.name == "_attrs" {
                                    Some(Cell::String(record.to_string()))
                                } else {
                                    r.get(&tgt_col.name).map(|v| {
                                        match tgt_col.type_oid {
                                            pg_sys::BOOLOID => Cell::Bool(
                                                v.as_bool().unwrap_or_else(|| column_type_mismatch!(tgt_col)),
                                            ),
                                            pg_sys::CHAROID => Cell::I8(
                                                v.as_i64()
                                                    .map(|s| {
                                                        i8::try_from(s)
                                                            .unwrap_or_else(|_| column_type_mismatch!(tgt_col))
                                                    })
                                                    .unwrap_or_else(|| column_type_mismatch!(tgt_col)),
                                            ),
                                            pg_sys::INT2OID => Cell::I16(
                                                v.as_i64()
                                                    .map(|s| {
                                                        i16::try_from(s)
                                                            .unwrap_or_else(|_| column_type_mismatch!(tgt_col))
                                                    })
                                                    .unwrap_or_else(|| column_type_mismatch!(tgt_col)),
                                            ),
                                            pg_sys::FLOAT4OID => Cell::F32(
                                                v.as_f64()
                                                    .map(|s| s as f32)
                                                    .unwrap_or_else(|| column_type_mismatch!(tgt_col)),
                                            ),
                                            pg_sys::INT4OID => Cell::I32(
                                                v.as_i64()
                                                    .map(|s| {
                                                        i32::try_from(s)
                                                            .unwrap_or_else(|_| column_type_mismatch!(tgt_col))
                                                    })
                                                    .unwrap_or_else(|| column_type_mismatch!(tgt_col)),
                                            ),
                                            pg_sys::FLOAT8OID => Cell::F64(
                                                v.as_f64().unwrap_or_else(|| column_type_mismatch!(tgt_col)),
                                            ),
                                            pg_sys::INT8OID => Cell::I64(
                                                v.as_i64().unwrap_or_else(|| column_type_mismatch!(tgt_col)),
                                            ),
                                            pg_sys::NUMERICOID => Cell::Numeric(
                                                v.as_f64()
                                                    .map(|s| {
                                                        AnyNumeric::try_from(s)
                                                            .unwrap_or_else(|_| column_type_mismatch!(tgt_col))
                                                    })
                                                    .unwrap_or_else(|| column_type_mismatch!(tgt_col)),
                                            ),
                                            pg_sys::TEXTOID => Cell::String(
                                                v.as_str()
                                                    .map(|s| s.to_owned())
                                                    .unwrap_or_else(|| column_type_mismatch!(tgt_col)),
                                            ),
                                            pg_sys::DATEOID => Cell::Date(
                                                v.as_str()
                                                    .map(|s| {
                                                        Date::from_str(s)
                                                            .unwrap_or_else(|_| column_type_mismatch!(tgt_col))
                                                    })
                                                    .unwrap_or_else(|| column_type_mismatch!(tgt_col)),
                                            ),
                                            pg_sys::TIMESTAMPOID => Cell::Timestamp(
                                                v.as_str()
                                                    .map(|s| {
                                                        Timestamp::from_str(s)
                                                            .unwrap_or_else(|_| column_type_mismatch!(tgt_col))
                                                    })
                                                    .unwrap_or_else(|| column_type_mismatch!(tgt_col)),
                                            ),
                                            _ => {
                                                // report error and return a dummy cell
                                                report_error(
                                                    PgSqlErrorCode::ERRCODE_FDW_INVALID_DATA_TYPE,
                                                    &format!(
                                                        "column '{}' data type oid '{}' is not supported",
                                                        tgt_col.name, tgt_col.type_oid
                                                    ),
                                                );
                                                Cell::Bool(false)
                                            }
                                        }
                                    })
                                };
                                row.push(&tgt_col.name, cell);
                            }
                        }
                        row
                    })
                    .collect::<Vec<Row>>()
            });
    }
}

impl ForeignDataWrapper for LogflareFdw {
    fn new(options: &HashMap<String, String>) -> Self {
        let base_url = options
            .get("api_url")
            .map(|t| t.to_owned())
            .map(|s| {
                if s.ends_with('/') {
                    s
                } else {
                    format!("{}/", s)
                }
            })
            .unwrap_or_else(|| LogflareFdw::BASE_URL.to_string());
        let client = match options.get("api_key") {
            Some(api_key) => Some(create_client(api_key)),
            None => require_option("api_key_id", options)
                .and_then(|key_id| get_vault_secret(&key_id))
                .map(|api_key| create_client(&api_key)),
        };

        LogflareFdw {
            rt: create_async_runtime(),
            base_url: Url::parse(&base_url).unwrap(),
            client,
            scan_result: None,
        }
    }

    fn begin_scan(
        &mut self,
        _quals: &[Qual],
        columns: &[Column],
        _sorts: &[Sort],
        _limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) {
        let endpoint = if let Some(name) = require_option("endpoint", options) {
            name
        } else {
            return;
        };

        if let Some(client) = &self.client {
            // build url
            let url = self.base_url.join(&endpoint).unwrap();

            // make api call
            match self.rt.block_on(client.get(url).send()) {
                Ok(resp) => {
                    if resp.status() == StatusCode::NOT_FOUND {
                        // if it is 404 error, we should treat it as an empty
                        // result rather than a request error
                        return;
                    }
                    match resp.error_for_status() {
                        Ok(resp) => {
                            let body: JsonValue = self.rt.block_on(resp.json()).unwrap();
                            self.resp_to_rows(&body, columns);
                        }
                        Err(err) => report_request_error!(err),
                    }
                }
                Err(err) => report_request_error!(err),
            }
        }
    }

    fn iter_scan(&mut self, row: &mut Row) -> Option<()> {
        if let Some(ref mut result) = self.scan_result {
            if !result.is_empty() {
                return result
                    .drain(0..1)
                    .last()
                    .map(|src_row| row.replace_with(src_row));
            }
        }
        None
    }

    fn end_scan(&mut self) {
        self.scan_result.take();
    }

    fn validator(options: Vec<Option<String>>, catalog: Option<pg_sys::Oid>) {
        if let Some(oid) = catalog {
            if oid == FOREIGN_TABLE_RELATION_ID {
                check_options_contain(&options, "endpoint");
            }
        }
    }
}
