use sea_orm::EntityTrait;
use tracing::error;

use crate::carrier::{
    execute::{ExecuteCarrier, HasExecuteCarrier},
    query::ImplQueryCarrier,
    simple_query::{HasSimpleQueryCarrier, SimpleQueryCarrier},
};

use super::ContainerBuilder;

pub struct Container<DbValue>
where
    DbValue: EntityTrait + Send + 'static,
{
    values: Vec<DbValue::Model>,
    query_carrier: SimpleQueryCarrier<DbValue>,
    execute_carrier: ExecuteCarrier,
}

impl<DbValue> Container<DbValue>
where
    DbValue: EntityTrait + Send + 'static,
{
    pub(crate) fn from_carriers(
        query_carrier: SimpleQueryCarrier<DbValue>,
        execute_carrier: ExecuteCarrier,
    ) -> Self {
        Self {
            values: vec![],
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

impl<DbValue> HasSimpleQueryCarrier<DbValue> for Container<DbValue>
where
    DbValue: EntityTrait + Send + 'static,
{
    fn ref_simple_query_carrier(&self) -> &SimpleQueryCarrier<DbValue> {
        &self.query_carrier
    }
    fn ref_mut_simple_query_carrier(&mut self) -> &mut SimpleQueryCarrier<DbValue> {
        &mut self.query_carrier
    }
}

impl<DbValue> HasExecuteCarrier for Container<DbValue>
where
    DbValue: EntityTrait + Send + 'static,
{
    fn ref_execute_carrier(&self) -> &ExecuteCarrier {
        &self.execute_carrier
    }
    fn ref_mut_execute_carrier(&mut self) -> &mut ExecuteCarrier {
        &mut self.execute_carrier
    }
}

impl<Value> Clone for Container<Value>
where
    Value: EntityTrait + Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            values: vec![],
            query_carrier: self.query_carrier.clone(),
            execute_carrier: self.execute_carrier.clone(),
        }
    }
}
