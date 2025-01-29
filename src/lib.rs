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
    let query_for_info = query.replace("\"", "");
    let query_for_info = if query_for_info.len() > 500 {
        query_for_info[0..500].to_string()
    } else {
        query_for_info
    };

    info!(
        query = query_for_info,
        tables_found = format!("{tables_found:?}").replace("\"", "")
    );
    tables_found
}

pub trait ToActiveModel {
    type ActiveModel;
    fn dml_clone(&self) -> Self::ActiveModel;
    fn dml(self) -> Self::ActiveModel;
}

#[macro_export]
macro_rules! impl_to_active_model {
    ($type:ty, $dbtype:ty) => {
        impl $crate::ToActiveModel for $type {
            type ActiveModel = ActiveModel;
            fn dml_clone(&self) -> Self::ActiveModel {
                ActiveModel::from(<$dbtype as ::sqlx_projector::projectors::FromEntity<
                    $type,
                >>::from_entity(self.clone()))
            }
            fn dml(self) -> Self::ActiveModel {
                ActiveModel::from(<$dbtype as ::sqlx_projector::projectors::FromEntity<
                    $type,
                >>::from_entity(self))
            }
        }
    };
}
