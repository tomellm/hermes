
use tracing::error;

use diesel::{
    backend::Backend,
    query_builder::QueryFragment,
    query_dsl::methods::{ExecuteDsl, LoadQuery},
    r2d2::ManageConnection,
    Connection, RunQueryDsl,
};

use crate::
    carrier::{execute::ExecuteCarrier, query::QueryCarrier}
;

use super::ContainerBuilder;

pub struct Container<Value, Database>
where
    Database: ManageConnection + 'static,
    Value: Send + 'static,
{
    values: Vec<Value>,
    query_carrier: QueryCarrier<Database, Value>,
    execute_carrier: ExecuteCarrier<Database>,
}

impl<Value, Database> Container<Value, Database>
where
    Database: ManageConnection + 'static,
    Database::Connection: Connection,
    <Database::Connection as Connection>::Backend: Default,
    <<Database::Connection as Connection>::Backend as Backend>::QueryBuilder: Default,
    Value: Send + 'static,
{
    pub(crate) fn from_carriers(
        query_carrier: QueryCarrier<Database, Value>,
        execute_carrier: ExecuteCarrier<Database>,
    ) -> Self {
        Self {
            values: vec![],
            query_carrier,
            execute_carrier
        }
    }

    pub fn builder(&self) -> ContainerBuilder<Database> {
        self.query_carrier.builder()
    }

    pub fn state_update(&mut self) {
        if let Some(result) = self.query_carrier.try_resolve_query() {
            match result {
                Ok(values) => {
                    self.values = values;
                }
                Err(error) => error!("{error}"),
            }
        }
        self.execute_carrier.try_resolve_executes();
    }

    pub fn should_refresh(&self) -> bool {
        self.query_carrier.should_refresh()
    }

    pub fn query<Query>(&mut self, query_fn: impl FnOnce() -> Query + Send + 'static)
    where
        for<'query> Query: RunQueryDsl<Database::Connection>
            + QueryFragment<<Database::Connection as Connection>::Backend>
            + LoadQuery<'query, Database::Connection, Value>,
    {
        self.query_carrier.query(query_fn);
    }
    pub fn execute<Execute>(&mut self, execute_fn: impl FnOnce() -> Execute + Send + 'static)
    where
        Execute: RunQueryDsl<Database::Connection>
            + QueryFragment<<Database::Connection as Connection>::Backend>
            + ExecuteDsl<Database::Connection>,
    {
        self.execute_carrier.execute(execute_fn);
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
            query_carrier: self.query_carrier.clone(),
            execute_carrier: self.execute_carrier.clone()
        }
    }
}
