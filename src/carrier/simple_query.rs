use std::{future::Future, pin::Pin};

use chrono::Local;
use sea_orm::{DatabaseConnection, DbErr, EntityTrait, QueryTrait, Select};
use tokio::{
    sync::{mpsc, oneshot},
    task,
};
use tracing::info;

use crate::{get_tables_present, messenger::ContainerData, QUERY_BUILDER};

use super::query::{ExecutedQuery, HasQueryCarrier, ImplQueryCarrier, QueryCarrier};

#[derive(Clone)]
pub struct SimpleQueryCarrier<DbValue>
where
    DbValue: EntityTrait + Send + 'static,
{
    carrier: QueryCarrier<DbValue::Model>,

    pub(crate) stored_select: Option<Select<DbValue>>,
}

impl<DbValue> SimpleQueryCarrier<DbValue>
where
    DbValue: EntityTrait + Send + 'static,
{
    pub fn new(
        carrier: QueryCarrier<DbValue::Model>,
        stored_select: Option<Select<DbValue>>,
    ) -> Self {
        Self {
            carrier,
            stored_select,
        }
    }

    pub fn register_new(
        name: String,
        pool: DatabaseConnection,
        all_tables: Vec<String>,
        tables_changed_sender: mpsc::Sender<Vec<String>>,
        new_register_sender: mpsc::Sender<ContainerData>,
    ) -> Self {
        let carrier = QueryCarrier::register_new(
            name,
            pool,
            all_tables,
            tables_changed_sender,
            new_register_sender,
        );
        Self::new(carrier, None)
    }

    pub fn query(&mut self, mut query: Select<DbValue>) {
        let time_started = Local::now().into();
        self.carrier.should_update.set_updating(time_started);

        let db = self.carrier.db.clone();
        let (sender, reciever) = oneshot::channel();
        let query_string = query.query().to_string(QUERY_BUILDER);
        let tables = get_tables_present(&self.carrier.all_tables, &query_string);

        {
            const LIM: usize = 1000;
            let sub_str = if query_string.len() > LIM {
                &query_string[0..LIM]
            } else {
                &query_string
            };
            info!(
                "QueryCarrier: '{}' has queried for {}",
                self.carrier.name, sub_str
            );
        }

        task::spawn(async move {
            let result = query.into_model::<DbValue::Model>().all(&db).await;
            let _ = sender.send(ExecutedQuery::new(tables, result, time_started));
        });
        #[allow(unused_must_use)]
        self.carrier.executing_query.insert(reciever);
    }

    /// Does the action once and then stores it internally to redo later
    pub fn stored_query(&mut self, query: Select<DbValue>) {
        self.query(query.clone());
        let _ = self.stored_select.insert(query);
    }

    pub fn direct_query<OneTtimeValue>(
        &self,
        query: Select<OneTtimeValue>,
    ) -> DirectQueryFuture<<OneTtimeValue as EntityTrait>::Model>
    where
        OneTtimeValue: EntityTrait + Send + 'static,
    {
        let db = self.carrier.db.clone();
        Box::pin(async move { query.into_model::<OneTtimeValue::Model>().all(&db).await })
    }
}

pub trait ImplSimpleQueryCarrier<DbValue>
where
    DbValue: EntityTrait + Send + 'static,
{
    fn should_refresh(&self) -> bool;
    fn query(&mut self, query: Select<DbValue>);
    fn stored_query(&mut self, query: Select<DbValue>);
    fn direct_query<OneTtimeValue>(
        &self,
        query: Select<OneTtimeValue>,
    ) -> DirectQueryFuture<<OneTtimeValue as EntityTrait>::Model>
    where
        OneTtimeValue: EntityTrait + Send + 'static;
}

impl<T, DbValue> ImplSimpleQueryCarrier<DbValue> for T
where
    T: HasSimpleQueryCarrier<DbValue>,
    DbValue: EntityTrait + Send + 'static,
{
    fn should_refresh(&self) -> bool {
        self.ref_simple_query_carrier().should_refresh()
    }
    fn query(&mut self, query: Select<DbValue>) {
        self.ref_mut_simple_query_carrier().query(query);
    }
    fn stored_query(&mut self, query: Select<DbValue>) {
        self.ref_mut_simple_query_carrier().stored_query(query);
    }
    fn direct_query<OneTtimeValue>(
        &self,
        query: Select<OneTtimeValue>,
    ) -> DirectQueryFuture<<OneTtimeValue as EntityTrait>::Model>
    where
        OneTtimeValue: EntityTrait + Send + 'static,
    {
        Box::pin(self.ref_simple_query_carrier().direct_query(query))
    }
}

impl<Value> HasQueryCarrier<Value::Model> for SimpleQueryCarrier<Value>
where
    Value: EntityTrait + Send + 'static,
{
    fn ref_query_carrier(&self) -> &QueryCarrier<Value::Model> {
        &self.carrier
    }

    fn ref_mut_query_carrier(&mut self) -> &mut QueryCarrier<Value::Model> {
        &mut self.carrier
    }
}

pub(crate) trait HasSimpleQueryCarrier<DbValue>
where
    DbValue: EntityTrait + Send + 'static,
{
    fn ref_simple_query_carrier(&self) -> &SimpleQueryCarrier<DbValue>;
    fn ref_mut_simple_query_carrier(&mut self) -> &mut SimpleQueryCarrier<DbValue>;
}

pub type DirectQueryFuture<Type> =
    Pin<Box<dyn Future<Output = Result<Vec<Type>, DbErr>> + Send + 'static>>;
