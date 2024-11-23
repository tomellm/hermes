use sqlx::{Database, Executor, FromRow, IntoArguments, QueryBuilder};
use tracing::error;

use crate::carrier::{execute::ExecuteCarrier, query::QueryCarrier};

use super::ContainerBuilder;

pub struct Container<Value, DB>
where
    DB: Database,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    for<'row> Value: FromRow<'row, DB::Row> + Send + 'static,
{
    values: Vec<Value>,
    query_carrier: QueryCarrier<DB, Value>,
    execute_carrier: ExecuteCarrier<DB>,
}

impl<Value, DB> Container<Value, DB>
where
    DB: Database,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    for<'row> Value: FromRow<'row, DB::Row> + Send + 'static,
{
    pub(crate) fn from_carriers(
        query_carrier: QueryCarrier<DB, Value>,
        execute_carrier: ExecuteCarrier<DB>,
    ) -> Self {
        Self {
            values: vec![],
            query_carrier,
            execute_carrier,
        }
    }

    pub fn builder(&self) -> ContainerBuilder<DB> {
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

    pub fn query<BuildFn>(&mut self, create_query: BuildFn)
    where
        Value: Unpin,
        for<'args, 'intoargs> <DB as Database>::Arguments<'args>: IntoArguments<'intoargs, DB>,
        for<'builder> BuildFn: Fn(&mut QueryBuilder<'builder, DB>) + Clone + Send + 'static,
    {
        self.query_carrier.query(create_query);
    }
    pub fn execute<BuildFn>(&mut self, create_execute: BuildFn)
    where
        for<'args, 'intoargs> <DB as Database>::Arguments<'args>: IntoArguments<'intoargs, DB>,
        for<'builder> BuildFn: Fn(&mut QueryBuilder<'builder, DB>) + Clone + Send + 'static,
    {
        self.execute_carrier.execute(create_execute);
    }
    pub fn execute_many<'builder, Builders>(&mut self, executes: Builders)
    where
        for<'args, 'intoargs> <DB as Database>::Arguments<'args>: IntoArguments<'intoargs, DB>,
        Builders: Iterator<Item = QueryBuilder<'builder, DB>> + Send + 'static,
    {
        self.execute_carrier.execute_many(executes);
    }
}

impl<Value, DB> Clone for Container<Value, DB>
where
    DB: Database,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    for<'row> Value: FromRow<'row, DB::Row> + Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            values: vec![],
            query_carrier: self.query_carrier.clone(),
            execute_carrier: self.execute_carrier.clone(),
        }
    }
}
