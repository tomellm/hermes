use sqlx::{Database, Executor, FromRow};
use tracing::error;

use crate::carrier::{execute::{ExecuteCarrier, GetExecuteCarrier}, query::{GetQueryCarrier, QueryCarrier}};

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

}

impl<DbValue, DB> GetQueryCarrier<DB, DbValue> for Container<DbValue, DB>
where
    DB: Database,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    for<'row> DbValue: FromRow<'row, DB::Row> + Send + 'static,
{
    fn ref_mut_query_carrier(&mut self) -> &mut QueryCarrier<DB, DbValue> {
        &mut self.query_carrier
    }
}

impl<DbValue, DB> GetExecuteCarrier<DB> for Container<DbValue, DB>
where
    DB: Database,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    for<'row> DbValue: FromRow<'row, DB::Row> + Send + 'static,
{
    fn ref_mut_execute_carrier(&mut self) -> &mut ExecuteCarrier<DB> {
        &mut self.execute_carrier
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
