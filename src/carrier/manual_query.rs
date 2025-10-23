use std::future::Future;

use sea_orm::DatabaseConnection;
use tokio::{
    sync::{mpsc, oneshot},
    task,
};

use crate::{container::builder::ContainerBuilder, messenger::ContainerData, TablesCollector};

use super::query::{ExecutedQuery, HasQueryCarrier, ImplQueryCarrier, QueryCarrier};

pub struct ManualQueryCarrier<Value>
where
    Value: Send + 'static,
{
    carrier: QueryCarrier<Value>,
}

impl<Value> ManualQueryCarrier<Value>
where
    Value: Send + 'static,
{
    pub fn new(carrier: QueryCarrier<Value>) -> Self {
        Self { carrier }
    }

    pub fn register_new(
        name: String,
        pool: DatabaseConnection,
        all_tables: Vec<String>,
        tables_changed_sender: mpsc::Sender<Vec<String>>,
        new_register_sender: mpsc::Sender<ContainerData>,
    ) -> Self {
        Self::new(QueryCarrier::register_new(
            name,
            pool,
            all_tables,
            tables_changed_sender,
            new_register_sender,
        ))
    }

    pub fn manual_query<P, F>(&mut self, query_producer: P)
    where
        F: Future<Output = ExecutedQuery<Value>> + Send + 'static,
        P: FnOnce(DatabaseConnection, TablesCollector) -> F,
    {
        let (sender, reciever) = oneshot::channel();
        let collector = TablesCollector::new(self.carrier.all_tables.clone());
        self.carrier
            .should_update
            .set_updating(collector.time_started);

        let executing_query = query_producer(self.carrier.db.clone(), collector);
        task::spawn(async move {
            let query_result = executing_query.await;
            let _ = sender.send(query_result);
        });
        #[allow(clippy::let_underscore_future)]
        let _ = self.carrier.executing_query.insert(reciever);
    }

    pub fn builder(&self) -> ContainerBuilder {
        self.carrier.builder()
    }
}

impl<Value> HasQueryCarrier<Value> for ManualQueryCarrier<Value>
where
    Value: Send + 'static,
{
    fn ref_query_carrier(&self) -> &QueryCarrier<Value> {
        &self.carrier
    }

    fn ref_mut_query_carrier(&mut self) -> &mut QueryCarrier<Value> {
        &mut self.carrier
    }
}

pub trait ImplManualQueryCarrier<Value>
where
    Value: Send + 'static,
{
    fn should_refresh(&self) -> bool;

    fn manual_query<P, F>(&mut self, query_producer: P)
    where
        F: Future<Output = ExecutedQuery<Value>> + Send + 'static,
        P: FnOnce(DatabaseConnection, TablesCollector) -> F;
}

impl<T, Value> ImplManualQueryCarrier<Value> for T
where
    T: HasManualQueryCarrier<Value>,
    Value: Send + 'static,
{
    fn manual_query<P, F>(&mut self, query_producer: P)
    where
        F: Future<Output = ExecutedQuery<Value>> + Send + 'static,
        P: FnOnce(DatabaseConnection, TablesCollector) -> F,
    {
        self.ref_mut_manual_query_carrier()
            .manual_query(query_producer);
    }

    fn should_refresh(&self) -> bool {
        self.ref_manual_query_carrier().should_refresh()
    }
}

pub(crate) trait HasManualQueryCarrier<Value>
where
    Value: Send + 'static,
{
    fn ref_manual_query_carrier(&self) -> &ManualQueryCarrier<Value>;
    fn ref_mut_manual_query_carrier(&mut self) -> &mut ManualQueryCarrier<Value>;
}
