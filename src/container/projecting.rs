use sqlx::{Database, Executor, FromRow, IntoArguments, QueryBuilder};
use sqlx_projector::projectors::{FromEntity, ToEntity};
use tracing::error;

use crate::{
    actor::Actor,
    carrier::{execute::ExecuteCarrier, query::QueryCarrier},
};

use super::ContainerBuilder;

pub struct ProjectingContainer<Value, DbValue, DB>
where
    DB: Database,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    Value: Send + 'static,
    for<'row> DbValue: FromRow<'row, DB::Row> + Send + 'static,
{
    values: Vec<Value>,
    query_carrier: QueryCarrier<DB, DbValue>,
    execute_carrier: ExecuteCarrier<DB>,
}

impl<Value, DbValue, DB> ProjectingContainer<Value, DbValue, DB>
where
    DB: Database,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    Value: Send + 'static,
    for<'row> DbValue:
        FromRow<'row, DB::Row> + FromEntity<Value> + ToEntity<Value> + Send + 'static,
{
    pub(crate) fn from_carriers(
        query_carrier: QueryCarrier<DB, DbValue>,
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

    pub fn values(&self) -> &Vec<Value> {
        &self.values
    }

    pub fn state_update(&mut self) {
        if let Some(result) = self.query_carrier.try_resolve_query() {
            match result {
                Ok(values) => {
                    self.values = values.into_iter().map(ToEntity::to_entity).collect();
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
        DbValue: Unpin,
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

    pub fn actor(&self) -> Actor<DB> {
        Actor::new(self.execute_carrier.clone())
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
            values: vec![],
            query_carrier: self.query_carrier.clone(),
            execute_carrier: self.execute_carrier.clone(),
        }
    }
}
