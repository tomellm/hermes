use std::sync::Arc;

use tracing::error;

use diesel::{
    backend::Backend,
    query_builder::QueryFragment,
    query_dsl::methods::{ExecuteDsl, LoadQuery},
    r2d2::ManageConnection,
    Connection, RunQueryDsl,
};

use crate::{actor::Actor, carrier::{execute::ExecuteCarrier, query::QueryCarrier}};

use super::ContainerBuilder;

pub struct ProjectingContainer<Value, DbValue, Database>
where
    Database: ManageConnection + 'static,
    Value: Send + 'static,
    DbValue: Send + 'static,
{
    from_db: Arc<dyn Fn(DbValue) -> Value + Sync + Send + 'static>,
    values: Vec<Value>,
    query_carrier: QueryCarrier<Database, DbValue>,
    execute_carrier: ExecuteCarrier<Database>,
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
    pub(crate) fn from_carriers(
        from_db: Arc<dyn Fn(DbValue) -> Value + Sync + Send + 'static>,
        query_carrier: QueryCarrier<Database, DbValue>,
        execute_carrier: ExecuteCarrier<Database>,
    ) -> Self {
        Self {
            from_db,
            values: vec![],
            query_carrier,
            execute_carrier,
        }
    }

    pub fn builder(&self) -> ContainerBuilder<Database> {
        self.query_carrier.builder()
    }

    pub fn values(&self) -> &Vec<Value> {
        &self.values
    }

    pub fn state_update(&mut self) {
        if let Some(result) = self.query_carrier.try_resolve_query() {
            match result {
                Ok(values) => {
                    self.values = values.into_iter().map(|val| (self.from_db)(val)).collect();
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
            + LoadQuery<'query, Database::Connection, DbValue>,
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

    pub fn actor(&self) -> Actor<Database> {
        Actor::new(self.execute_carrier.clone())
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
            query_carrier: self.query_carrier.clone(),
            execute_carrier: self.execute_carrier.clone(),
        }
    }
}
