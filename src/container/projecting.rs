use std::{future::Future, pin::Pin};

use sea_orm::{DbErr, EntityTrait, Select};
use tracing::error;

use crate::{
    carrier::{
        execute::{ExecuteCarrier, HasExecuteCarrier},
        query::ImplQueryCarrier,
        simple_query::{HasSimpleQueryCarrier, ImplSimpleQueryCarrier, SimpleQueryCarrier},
    },
    container::builder::ContainerBuilder,
    FromEntity, ToEntity,
};

use super::data::{Data, HasData};

pub struct ProjectingContainer<Value, DbValue>
where
    Value: Send + 'static,
    DbValue: EntityTrait + Send + 'static,
    <DbValue as EntityTrait>::Model: FromEntity<Value> + ToEntity<Value>,
{
    pub name: String,
    pub data: Data<Value>,
    query_carrier: SimpleQueryCarrier<DbValue>,
    execute_carrier: ExecuteCarrier,
}

impl<Value, DbValue> ProjectingContainer<Value, DbValue>
where
    Value: Clone + Send + 'static,
    DbValue: EntityTrait + Send + 'static,
    <DbValue as EntityTrait>::Model: FromEntity<Value> + ToEntity<Value>,
{
    pub(crate) fn from_carriers(
        name: String,
        query_carrier: SimpleQueryCarrier<DbValue>,
        execute_carrier: ExecuteCarrier,
    ) -> Self {
        Self {
            name,
            data: Data::default(),
            query_carrier,
            execute_carrier,
        }
    }

    pub fn builder(&self) -> ContainerBuilder {
        self.query_carrier.builder()
    }

    pub fn state_update(&mut self, automatic_requery: bool) {
        self.query_carrier.try_recive_should_update();
        if let Some(result) = self.query_carrier.try_resolve_query() {
            match result {
                Ok(values) => self.data.set(values.into_iter().map(ToEntity::to_entity)),
                Err(error) => error!(container = self.name, error = error.to_string()),
            }
        }
        self.execute_carrier.try_resolve_executes();

        if automatic_requery && self.should_refresh() {
            if let Some(select) = &self.query_carrier.stored_select {
                self.query(select.clone());
            }
        }
    }

    pub fn direct_proj_query<QValue, QDbValue>(
        &self,
        query: Select<QDbValue>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<QValue>, DbErr>> + Send + 'static>>
    where
        QDbValue: EntityTrait + Send + 'static,
        <QDbValue as EntityTrait>::Model: ToEntity<QValue>,
    {
        let future = self.direct_query(query);
        Box::pin(async move {
            future
                .await
                .map(|values| values.into_iter().map(|val| val.to_entity()).collect())
        })
    }
}

impl<Value, DbValue> HasSimpleQueryCarrier<DbValue> for ProjectingContainer<Value, DbValue>
where
    Value: Send,
    DbValue: EntityTrait + Send + 'static,
    <DbValue as EntityTrait>::Model: FromEntity<Value> + ToEntity<Value>,
{
    fn ref_simple_query_carrier(&self) -> &SimpleQueryCarrier<DbValue> {
        &self.query_carrier
    }
    fn ref_mut_simple_query_carrier(&mut self) -> &mut SimpleQueryCarrier<DbValue> {
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
            name: self.name.clone(),
            data: Data::default(),
            query_carrier: self.query_carrier.clone(),
            execute_carrier: self.execute_carrier.clone(),
        }
    }
}
