#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn wasm_smoketest() {
        Spi::connect(|mut c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER wasm_wrapper
                     HANDLER wasm_fdw_handler VALIDATOR wasm_fdw_validator"#,
                None,
                None,
            )
            .unwrap();

            // Paddle FDW test
            c.update(
                r#"CREATE SERVER paddle_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_paddle_fdw_v0.1.1/paddle_fdw.wasm',
                       fdw_package_name 'supabase:paddle-fdw',
                       fdw_package_version '0.1.1',
                       fdw_package_checksum 'c5ac70bb2eef33693787b7d4efce9a83cde8d4fa40889d2037403a51263ba657',
                       api_url 'https://sandbox-api.paddle.com',
                       api_key '1234567890'
                     )"#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE paddle_customers (
                    id text,
                    name text,
                    email text,
                    status text,
                    custom_data jsonb,
                    created_at timestamp,
                    updated_at timestamp,
                    attrs jsonb
                  )
                  SERVER paddle_server
                  OPTIONS (
                    object 'customers',
                    rowid_column 'id'
                  )
             "#,
                None,
                None,
            )
            .unwrap();

            let results = c
                .select(
                    "SELECT * FROM paddle_customers WHERE id = 'ctm_01hymwgpkx639a6mkvg99563sp'",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("email").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["test@test.com"]);
        });
    }
}
