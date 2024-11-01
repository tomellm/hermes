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

pub struct Container<Value, Database>
where
    Database: ManageConnection + 'static,
    Value: Send + 'static,
{
    values: Vec<Value>,
    carrier: Carrier<Database, Value>,
}

impl<Value, Database> Container<Value, Database>
where
    Database: ManageConnection + 'static,
    Database::Connection: Connection,
    <Database::Connection as Connection>::Backend: Default,
    <<Database::Connection as Connection>::Backend as Backend>::QueryBuilder: Default,
    Value: Send + 'static,
{
    pub(crate) fn new(
        pool: Pool<Database>,
        all_tables: Vec<String>,
        tables_interested_sender: mpsc::Sender<Vec<String>>,
        tables_changed_sender: mpsc::Sender<Vec<String>>,
        should_update: Arc<AtomicBool>,
        new_register_sender: mpsc::Sender<ContainerData>,
    ) -> Self {
        Self {
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

    pub fn from_carrier(carrier: Carrier<Database, Value>) -> Self {
        Self {
            values: vec![],
            carrier,
        }
    }

    pub fn state_update(&mut self) {
        if let Some(result) = self.carrier.try_resolve_query() {
            match result {
                Ok(values) => {
                    self.values = values;
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
            + LoadQuery<'query, Database::Connection, Value>,
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

impl<Value, Database> Clone for Container<Value, Database>
where
    Database: ManageConnection + 'static,
    Database::Connection: Connection,
    <Database::Connection as Connection>::Backend: Default,
    <<Database::Connection as Connection>::Backend as Backend>::QueryBuilder: Default,
    Value: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            values: vec![],
            carrier: self.carrier.clone(),
        }
    }
}
