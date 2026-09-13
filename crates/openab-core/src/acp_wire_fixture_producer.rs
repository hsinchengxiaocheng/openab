use openab_core::acp::connection::test_support::canonical_acp_wire_fixture;
use std::io::{self, Write};

fn main() {
    match canonical_acp_wire_fixture() {
        Ok(fixture) => {
            if let Err(error) = io::stdout().write_all(&fixture) {
                eprintln!("failed to write ACP wire fixture: {error}");
                std::process::exit(1);
            }
        }
        Err(error) => {
            eprintln!("failed to generate ACP wire fixture: {error}");
            std::process::exit(1);
        }
    }
}
