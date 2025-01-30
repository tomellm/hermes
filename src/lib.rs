pub mod actor;
pub mod carrier;
pub mod container;
pub mod factory;
pub mod messenger;

fn get_tables_present(all_tables: &[String], query: &str) -> Vec<String> {
    all_tables
        .iter()
        .filter_map(|table| query.find(table.as_str()).map(|_| table.clone()))
        .collect::<Vec<_>>()
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
