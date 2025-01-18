use tracing::info;

pub mod actor;
pub mod carrier;
pub mod container;
pub mod factory;
pub mod messenger;

fn get_tables_present(all_tables: &[String], query: &str) -> Vec<String> {
    let tables_found = all_tables
        .iter()
        .filter_map(|table| query.find(table.as_str()).map(|_| table.clone()))
        .collect::<Vec<_>>();
    info!(query = query, tables_found = format!("{tables_found:?}"));
    tables_found
}
