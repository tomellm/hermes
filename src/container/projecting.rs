use sea_orm::EntityTrait;
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

pub struct ProjectingContainer<Value, DbValue>
where
    Value: Send + 'static,
    DbValue: EntityTrait + Send + 'static,
    <DbValue as EntityTrait>::Model: FromEntity<Value> + ToEntity<Value>,
{
    pub data: Data<Value>,
    query_carrier: QueryCarrier<DbValue>,
    execute_carrier: ExecuteCarrier,
}

impl<Value, DbValue> ProjectingContainer<Value, DbValue>
where
    Value: Clone + Send + 'static,
    DbValue: EntityTrait + Send + 'static,
    <DbValue as EntityTrait>::Model: FromEntity<Value> + ToEntity<Value>,
{
    pub(crate) fn from_carriers(
        query_carrier: QueryCarrier<DbValue>,
        execute_carrier: ExecuteCarrier,
    ) -> Self {
        Self {
            data: Data::default(),
            query_carrier,
            execute_carrier,
        }
    }

    pub fn builder(&self) -> ContainerBuilder {
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

impl<Value, DbValue> HasQueryCarrier<DbValue> for ProjectingContainer<Value, DbValue>
where
    Value: Send,
    DbValue: EntityTrait + Send + 'static,
    <DbValue as EntityTrait>::Model: FromEntity<Value> + ToEntity<Value>,
{
    fn ref_query_carrier(&self) -> &QueryCarrier<DbValue> {
        &self.query_carrier
    }
    fn ref_mut_query_carrier(&mut self) -> &mut QueryCarrier<DbValue> {
        &mut self.query_carrier
    }
}

impl<Value, DbValue> HasExecuteCarrier for ProjectingContainer<Value, DbValue>
where
    Value: Send,
    DbValue: EntityTrait + Send + 'static,
    <DbValue as EntityTrait>::Model: FromEntity<Value> + ToEntity<Value>,
{
    fn ref_execute_carrier(&self) -> &ExecuteCarrier {
        &self.execute_carrier
    }
    fn ref_mut_execute_carrier(&mut self) -> &mut ExecuteCarrier {
        &mut self.execute_carrier
    }
}

impl<Value, DbValue> HasData<Value> for ProjectingContainer<Value, DbValue>
where
    Value: Send + 'static,
    DbValue: EntityTrait + Send + 'static,
    <DbValue as EntityTrait>::Model: FromEntity<Value> + ToEntity<Value>,
{
    fn ref_data(&self) -> &Data<Value> {
        &self.data
    }
    fn ref_mut_data(&mut self) -> &mut Data<Value> {
        &mut self.data
    }
}

impl<Value, DbValue> Clone for ProjectingContainer<Value, DbValue>
where
    Value: Send + 'static,
    DbValue: EntityTrait + Send + 'static,
    <DbValue as EntityTrait>::Model: FromEntity<Value> + ToEntity<Value>,
{
    fn clone(&self) -> Self {
        Self {
            data: Data::default(),
            query_carrier: self.query_carrier.clone(),
            execute_carrier: self.execute_carrier.clone(),
        }
    }
}
