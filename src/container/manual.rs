use std::sync::Arc;

use crate::carrier::{
    execute::{ExecuteCarrier, HasExecuteCarrier},
    manual_query::{HasManualQueryCarrier, ImplManualQueryCarrier, ManualQueryCarrier},
    query::ImplQueryCarrier,
};

use tracing::error;

use super::{
    data::{Data, HasData},
    ContainerBuilder,
};

pub(crate) type StoredQuery<Value> =
    Option<Arc<dyn Fn(&mut Container<Value>) + Sync + Send + 'static>>;

pub struct Container<Value>
where
    Value: Send + 'static,
{
    pub name: String,
    pub data: Data<Value>,
    query_carrier: ManualQueryCarrier<Value>,
    execute_carrier: ExecuteCarrier,

    pub(crate) stored_query: StoredQuery<Value>,
}

impl<Value> Container<Value>
where
    Value: Send + 'static,
{
    pub(crate) fn from_carriers(
        name: String,
        query_carrier: ManualQueryCarrier<Value>,
        execute_carrier: ExecuteCarrier,
    ) -> Self {
        Self {
            name,
            data: Data::default(),
            query_carrier,
            execute_carrier,
            stored_query: None,
        }
    }

    pub fn builder(&self) -> ContainerBuilder {
        self.query_carrier.builder()
    }

    pub fn state_update(&mut self, automatic_requery: bool) {
        self.query_carrier.try_recive_should_update();
        if let Some(result) = self.query_carrier.try_resolve_query() {
            match result {
                Ok(values) => self.data.set(values.into_iter()),
                Err(error) => error!(container = self.name, error = error.to_string()),
            }
        }
        self.execute_carrier.try_resolve_executes();

        if automatic_requery && self.should_refresh() {
            if let Some(query) = self.stored_query.clone() {
                query(self)
            }
        }
    }

    pub fn stored_query(&mut self, query_action: impl Fn(&mut Self) + Sync + Send + 'static) {
        query_action(self);
        let _ = self.stored_query.insert(Arc::new(query_action));
    }
}

impl<Value> HasExecuteCarrier for Container<Value>
where
    Value: Send + 'static,
{
    fn ref_execute_carrier(&self) -> &ExecuteCarrier {
        &self.execute_carrier
    }

    fn ref_mut_execute_carrier(&mut self) -> &mut ExecuteCarrier {
        &mut self.execute_carrier
    }
}

impl<Value> HasManualQueryCarrier<Value> for Container<Value>
where
    Value: Send + 'static,
{
    fn ref_manual_query_carrier(&self) -> &ManualQueryCarrier<Value> {
        &self.query_carrier
    }

    fn ref_mut_manual_query_carrier(&mut self) -> &mut ManualQueryCarrier<Value> {
        &mut self.query_carrier
    }
}

impl<Value> HasData<Value> for Container<Value>
where
    Value: Send + 'static,
{
    fn ref_data(&self) -> &Data<Value> {
        &self.data
    }

    fn ref_mut_data(&mut self) -> &mut Data<Value> {
        &mut self.data
    }
}
