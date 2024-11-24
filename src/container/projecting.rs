use sqlx::{Database, Executor, FromRow};
use sqlx_projector::projectors::{FromEntity, ToEntity};
use tracing::error;

use crate::carrier::{
    execute::{ExecuteCarrier, HasExecuteCarrier},
    query::{HasQueryCarrier, QueryCarrier},
};

use super::{
    data::{Data, HasData},
    ContainerBuilder,
};

pub struct ProjectingContainer<Value, DbValue, DB>
where
    DB: Database,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    Value: Send + 'static,
    for<'row> DbValue: FromRow<'row, DB::Row> + Send + 'static,
{
    pub data: Data<Value>,
    query_carrier: QueryCarrier<DB, DbValue>,
    execute_carrier: ExecuteCarrier<DB>,
}

impl<Value, DbValue, DB> ProjectingContainer<Value, DbValue, DB>
where
    DB: Database,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    Value: Clone + Send + 'static,
    for<'row> DbValue:
        FromRow<'row, DB::Row> + FromEntity<Value> + ToEntity<Value> + Send + 'static,
{
    pub(crate) fn from_carriers(
        query_carrier: QueryCarrier<DB, DbValue>,
        execute_carrier: ExecuteCarrier<DB>,
    ) -> Self {
        Self {
            data: Data::default(),
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
                Ok(values) => self.data.set(values.into_iter().map(ToEntity::to_entity)),
                Err(error) => error!("{error}"),
            }
        }
        self.execute_carrier.try_resolve_executes();
    }
}

impl<Value, DbValue, DB> HasQueryCarrier<DB, DbValue> for ProjectingContainer<Value, DbValue, DB>
where
    DB: Database,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    for<'row> DbValue: FromRow<'row, DB::Row> + Send + 'static,
    Value: Send,
{
    fn ref_query_carrier(&self) -> &QueryCarrier<DB, DbValue> {
        &self.query_carrier
    }
    fn ref_mut_query_carrier(&mut self) -> &mut QueryCarrier<DB, DbValue> {
        &mut self.query_carrier
    }
}

impl<Value, DbValue, DB> HasExecuteCarrier<DB> for ProjectingContainer<Value, DbValue, DB>
where
    DB: Database,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    for<'row> DbValue: FromRow<'row, DB::Row> + Send + 'static,
    Value: Send,
{
    fn ref_execute_carrier(&self) -> &ExecuteCarrier<DB> {
        &self.execute_carrier
    }
    fn ref_mut_execute_carrier(&mut self) -> &mut ExecuteCarrier<DB> {
        &mut self.execute_carrier
    }
}

impl<Value, DbValue, DB> HasData<Value> for ProjectingContainer<Value, DbValue, DB>
where
    DB: Database,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    Value: Send + 'static,
    for<'row> DbValue: FromRow<'row, DB::Row> + Send + 'static,
{
    fn ref_data(&self) -> &Data<Value> {
        &self.data
    }
    fn ref_mut_data(&mut self) -> &mut Data<Value> {
        &mut self.data
    }
}

impl<Value, DbValue, DB> Clone for ProjectingContainer<Value, DbValue, DB>
where
    DB: Database,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    Value: Send + 'static,
    for<'row> DbValue: FromRow<'row, DB::Row> + Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            data: Data::default(),
            query_carrier: self.query_carrier.clone(),
            execute_carrier: self.execute_carrier.clone(),
        }
    }
}
