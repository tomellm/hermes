
use diesel::{
    backend::Backend, query_builder::QueryFragment, query_dsl::methods::ExecuteDsl, r2d2::ManageConnection, Connection, RunQueryDsl
};

use crate::carrier::execute::ExecuteCarrier;

pub struct Actor<Database>
where
    Database: ManageConnection + 'static,
    Database::Connection: Connection,
    <Database::Connection as Connection>::Backend: Default,
    <<Database::Connection as Connection>::Backend as Backend>::QueryBuilder: Default,
{
    executor: ExecuteCarrier<Database>,
}

impl<Database> Actor<Database>
where
    Database: ManageConnection + 'static,
    Database::Connection: Connection,
    <Database::Connection as Connection>::Backend: Default,
    <<Database::Connection as Connection>::Backend as Backend>::QueryBuilder: Default,
{
    pub(crate) fn new(executor: ExecuteCarrier<Database>) -> Self {
        Self { executor }
    }

    pub fn execute<Execute>(&mut self, execute_fn: impl FnOnce() -> Execute + Send + 'static)
    where
        Execute: RunQueryDsl<Database::Connection>
            + QueryFragment<<Database::Connection as Connection>::Backend>
            + ExecuteDsl<Database::Connection>,
    {
        self.executor.execute(execute_fn);
    }
}
