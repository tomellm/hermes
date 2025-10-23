use std::collections::HashSet;

#[cfg(any(feature = "psql", feature = "mysql", feature = "sqlite"))]
use chrono::{DateTime, FixedOffset, Local};
#[cfg(any(feature = "psql", feature = "mysql", feature = "sqlite"))]
use sea_orm::{EntityTrait, QuerySelect, Select};

#[cfg(any(feature = "psql", feature = "mysql", feature = "sqlite"))]
use crate::consts::QUERY_BUILDER;

pub mod container;

#[cfg(any(feature = "psql", feature = "mysql", feature = "sqlite"))]
pub mod actor;
#[cfg(any(feature = "psql", feature = "mysql", feature = "sqlite"))]
pub mod carrier;
#[cfg(any(feature = "psql", feature = "mysql", feature = "sqlite"))]
mod consts;
#[cfg(any(feature = "psql", feature = "mysql", feature = "sqlite"))]
pub mod factory;
#[cfg(any(feature = "psql", feature = "mysql", feature = "sqlite"))]
pub mod messenger;

fn get_tables_present(all_tables: &[String], query: &str) -> Vec<String> {
    all_tables
        .iter()
        .filter_map(|table| query.find(table.as_str()).map(|_| table.clone()))
        .collect::<Vec<_>>()
}

#[cfg(any(feature = "psql", feature = "mysql", feature = "sqlite"))]
pub trait ContainsTables {
    fn and_find_tables(self, collector: &mut TablesCollector) -> Self;
}

#[cfg(any(feature = "psql", feature = "mysql", feature = "sqlite"))]
impl<T> ContainsTables for Select<T>
where
    T: EntityTrait,
{
    fn and_find_tables(mut self, collector: &mut TablesCollector) -> Self {
        collector.add(self.query().to_string(QUERY_BUILDER).as_str());
        self
    }
}

pub struct TablesCollector {
    #[cfg(any(feature = "psql", feature = "mysql", feature = "sqlite"))]
    time_started: DateTime<FixedOffset>,
    all_tables: Vec<String>,
    tables: HashSet<String>,
}

impl TablesCollector {
    pub fn new(all_tables: Vec<String>) -> Self {
        Self {
            #[cfg(any(feature = "psql", feature = "mysql", feature = "sqlite"))]
            time_started: Local::now().into(),
            all_tables,
            tables: HashSet::new(),
        }
    }

    pub fn add(&mut self, query: &str) {
        let found_tables = get_tables_present(&self.all_tables, query);
        self.tables.extend(found_tables);
    }
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
                ActiveModel::from(<$dbtype as FromEntity<$type>>::from_entity(self.clone()))
            }
            fn dml(self) -> Self::ActiveModel {
                ActiveModel::from(<$dbtype as FromEntity<$type>>::from_entity(self))
            }
        }
    };
}

pub trait ToEntity<Entity>
where
    Entity: ?Sized,
{
    fn to_entity(self) -> Entity;
}

pub trait FromEntity<Entity>
where
    Entity: ?Sized,
{
    fn from_entity(entity: Entity) -> Self;
}
