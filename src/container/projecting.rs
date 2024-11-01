use std::sync::{atomic::AtomicBool, Arc};

use tracing::error;

use diesel::{
    backend::Backend,
    query_builder::QueryFragment,
    query_dsl::methods::{ExecuteDsl, LoadQuery},
    r2d2::{ManageConnection, Pool},
    Connection, RunQueryDsl,
};
use tokio::sync::mpsc;

use crate::{carrier::Carrier, messenger::ContainerData};

pub struct ProjectingContainer<Value, DbValue, Database>
where
    Database: ManageConnection + 'static,
    Value: Send + 'static,
    DbValue: Send + 'static,
{
    from_db: Arc<dyn Fn(DbValue) -> Value + Send + 'static>,
    values: Vec<Value>,
    carrier: Carrier<Database, DbValue>,
}

impl<Value, DbValue, Database> ProjectingContainer<Value, DbValue, Database>
where
    Database: ManageConnection + 'static,
    Database::Connection: Connection,
    <Database::Connection as Connection>::Backend: Default,
    <<Database::Connection as Connection>::Backend as Backend>::QueryBuilder: Default,
    Value: Send + 'static,
    DbValue: Send + 'static,
{
    pub(crate) fn new(
        pool: Pool<Database>,
        all_tables: Vec<String>,
        from_db_fn: impl Fn(DbValue) -> Value + Send + 'static,
        tables_interested_sender: mpsc::Sender<Vec<String>>,
        tables_changed_sender: mpsc::Sender<Vec<String>>,
        should_update: Arc<AtomicBool>,
        new_register_sender: mpsc::Sender<ContainerData>,
    ) -> Self {
        Self {
            from_db: Arc::new(from_db_fn),
            values: vec![],
            carrier: Carrier::new(
                pool,
                all_tables,
                tables_interested_sender,
                tables_changed_sender,
                should_update,
                new_register_sender,
            ),
        }
    }

    pub(crate) fn from_carrier(
        from_db: Arc<dyn Fn(DbValue) -> Value + Send + 'static>,
        carrier: Carrier<Database, DbValue>,
    ) -> Self {
        Self {
            from_db,
            values: vec![],
            carrier,
        }
    }

    pub fn values(&self) -> &Vec<Value> {
        &self.values
    }

    pub fn state_update(&mut self) {
        if let Some(result) = self.carrier.try_resolve_query() {
            match result {
                Ok(values) => {
                    self.values = values.into_iter().map(|val| (self.from_db)(val)).collect();
                }
                Err(error) => error!("{error}"),
            }
        }
        self.carrier.try_resolve_executes();
    }

    pub fn should_refresh(&self) -> bool {
        self.carrier.should_refresh()
    }

    pub fn query<Query>(&mut self, query_fn: impl FnOnce() -> Query + Send + 'static)
    where
        for<'query> Query: RunQueryDsl<Database::Connection>
            + QueryFragment<<Database::Connection as Connection>::Backend>
            + LoadQuery<'query, Database::Connection, DbValue>,
    {
        self.carrier.query(query_fn);
    }
    pub fn execute<Execute>(&mut self, execute_fn: impl FnOnce() -> Execute + Send + 'static)
    where
        Execute: RunQueryDsl<Database::Connection>
            + QueryFragment<<Database::Connection as Connection>::Backend>
            + ExecuteDsl<Database::Connection>,
    {
        self.carrier.execute(execute_fn);
    }
}

impl<Value, DbValue, Database> Clone for ProjectingContainer<Value, DbValue, Database>
where
    Database: ManageConnection + 'static,
    Database::Connection: Connection,
    <Database::Connection as Connection>::Backend: Default,
    <<Database::Connection as Connection>::Backend as Backend>::QueryBuilder: Default,
    Value: Send + 'static,
    DbValue: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            from_db: Arc::clone(&self.from_db),
            values: vec![],
            carrier: self.carrier.clone(),
        }
    }
}
