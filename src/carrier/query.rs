use std::{cmp::Ordering, future::Future, mem, pin::Pin};

use crate::{container::ContainerBuilder, get_tables_present, messenger::ContainerData};
use chrono::{DateTime, FixedOffset, Local};
use sea_orm::{DatabaseConnection, DbErr, EntityTrait, QueryTrait, Select};
use sea_query::SqliteQueryBuilder;
use tokio::{
    sync::{
        mpsc,
        oneshot::{self, error::TryRecvError},
    },
    task,
};
use tracing::info;

pub struct QueryCarrier<DbValue>
where
    DbValue: EntityTrait + Send + 'static,
{
    name: String,

    db: DatabaseConnection,
    all_tables: Vec<String>,

    interesting_tables: Vec<String>,
    executing_query: Option<oneshot::Receiver<ExecutedQuery<DbValue::Model>>>,
    tables_interested_sender: mpsc::Sender<Vec<String>>,

    should_update: UpdateState,
    time_of_change_reciver: mpsc::Receiver<DateTime<FixedOffset>>,

    new_register_sender: mpsc::Sender<ContainerData>,
    tables_changed_sender: mpsc::Sender<Vec<String>>,

    pub(crate) stored_select: Option<Select<DbValue>>,
}

impl<DbValue> Clone for QueryCarrier<DbValue>
where
    DbValue: EntityTrait + Send + 'static,
{
    fn clone(&self) -> Self {
        Self::register_new(
            self.name.clone(),
            self.db.clone(),
            self.all_tables.clone(),
            self.tables_changed_sender.clone(),
            self.new_register_sender.clone(),
        )
    }
}

impl<DbValue> QueryCarrier<DbValue>
where
    DbValue: EntityTrait + Send,
{
    pub fn register_new(
        name: String,
        pool: DatabaseConnection,
        all_tables: Vec<String>,
        tables_changed_sender: mpsc::Sender<Vec<String>>,
        new_register_sender: mpsc::Sender<ContainerData>,
    ) -> Self {
        let (tables_interested_sender, tables_interested_reciever) = mpsc::channel(3);

        let sender = new_register_sender.clone();
        let (update_sender, update_reciver) = mpsc::channel(10);
        let data = ContainerData::new(tables_interested_reciever, update_sender);
        task::spawn(async move {
            let _ = sender.send(data).await;
        });

        Self::new(
            name,
            pool,
            all_tables,
            tables_interested_sender,
            tables_changed_sender,
            update_reciver,
            new_register_sender,
        )
    }

    fn new(
        name: String,
        pool: DatabaseConnection,
        all_tables: Vec<String>,
        tables_interested_sender: mpsc::Sender<Vec<String>>,
        tables_changed_sender: mpsc::Sender<Vec<String>>,
        update_reciver: mpsc::Receiver<DateTime<FixedOffset>>,
        new_register_sender: mpsc::Sender<ContainerData>,
    ) -> Self {
        Self {
            name,
            db: pool,
            all_tables,
            interesting_tables: vec![],
            executing_query: None,
            tables_interested_sender,
            tables_changed_sender,
            should_update: UpdateState::UpToDate,
            time_of_change_reciver: update_reciver,
            new_register_sender,
            stored_select: None,
        }
    }

    pub fn should_refresh(&self) -> bool {
        matches!(self.should_update, UpdateState::ShouldUpdate)
    }

    pub fn try_recive_should_update(&mut self) {
        if let Ok(time_of_change) = self.time_of_change_reciver.try_recv() {
            self.should_update.set_should_update(time_of_change);
        }
    }

    pub fn try_resolve_query(&mut self) -> Option<Result<Vec<DbValue::Model>, DbErr>> {
        let mut executed_query = Option::take(&mut self.executing_query)?;
        match executed_query.try_recv() {
            Ok(result) => {
                if let ExecutedQuery {
                    interested_tables,
                    query_result: Ok(_),
                    time_started,
                } = result
                {
                    self.should_update.check_and_set_done(time_started);
                    self.interesting_tables = interested_tables.clone();
                    let sender = self.tables_interested_sender.clone();
                    task::spawn(async move {
                        let _ = sender.send(interested_tables).await;
                    });
                }
                Some(result.query_result)
            }
            Err(TryRecvError::Closed) => None,
            Err(TryRecvError::Empty) => {
                #[allow(unused_must_use)]
                self.executing_query.insert(executed_query);
                None
            }
        }
    }

    pub fn query(&mut self, mut query: Select<DbValue>) {
        let time_started = Local::now().into();
        self.should_update.set_updating(time_started);

        let db = self.db.clone();
        let (sender, reciever) = oneshot::channel();
        let query_string = query.query().to_string(SqliteQueryBuilder);
        let tables = get_tables_present(&self.all_tables, &query_string);

        let sub_str = if query_string.len() > 500 {
            &query_string[0..500]
        } else {
            &query_string
        };
        info!("QueryCarrier: '{}' has queried for {}", self.name, sub_str);

        task::spawn(async move {
            let result = query.into_model::<DbValue::Model>().all(&db).await;
            let _ = sender.send(ExecutedQuery::new(tables, result, time_started));
        });
        #[allow(unused_must_use)]
        self.executing_query.insert(reciever);
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
        let db = self.db.clone();
        Box::pin(async move { query.into_model::<OneTtimeValue::Model>().all(&db).await })
    }

    pub fn builder(&self) -> ContainerBuilder {
        ContainerBuilder::new(
            self.db.clone(),
            self.all_tables.clone(),
            self.tables_changed_sender.clone(),
            self.new_register_sender.clone(),
        )
    }
}

pub type DirectQueryFuture<Type> =
    Pin<Box<dyn Future<Output = Result<Vec<Type>, DbErr>> + Send + 'static>>;

struct ExecutedQuery<DbValue>
where
    DbValue: Send + 'static,
{
    interested_tables: Vec<String>,
    query_result: Result<Vec<DbValue>, DbErr>,
    time_started: DateTime<FixedOffset>,
}

impl<DbValue> ExecutedQuery<DbValue>
where
    DbValue: Send + 'static,
{
    fn new(
        interested_tables: Vec<String>,
        values: Result<Vec<DbValue>, DbErr>,
        time_started: DateTime<FixedOffset>,
    ) -> Self {
        Self {
            interested_tables,
            query_result: values,
            time_started,
        }
    }
}

pub trait ImplQueryCarrier<DbValue>
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

impl<T, DbValue> ImplQueryCarrier<DbValue> for T
where
    T: HasQueryCarrier<DbValue>,
    DbValue: EntityTrait + Send + 'static,
{
    fn should_refresh(&self) -> bool {
        self.ref_query_carrier().should_refresh()
    }
    fn query(&mut self, query: Select<DbValue>) {
        self.ref_mut_query_carrier().query(query);
    }
    fn stored_query(&mut self, query: Select<DbValue>) {
        self.ref_mut_query_carrier().stored_query(query);
    }
    fn direct_query<OneTtimeValue>(
        &self,
        query: Select<OneTtimeValue>,
    ) -> DirectQueryFuture<<OneTtimeValue as EntityTrait>::Model>
    where
        OneTtimeValue: EntityTrait + Send + 'static,
    {
        Box::pin(self.ref_query_carrier().direct_query(query))
    }
}

pub(crate) trait HasQueryCarrier<DbValue>
where
    DbValue: EntityTrait + Send + 'static,
{
    fn ref_query_carrier(&self) -> &QueryCarrier<DbValue>;
    fn ref_mut_query_carrier(&mut self) -> &mut QueryCarrier<DbValue>;
}

#[derive(Copy, Clone)]
pub(crate) enum UpdateState {
    ShouldUpdate,
    Updating {
        time_started: DateTime<FixedOffset>,
        back_to_back: Option<DateTime<FixedOffset>>,
    },
    UpToDate,
}

impl UpdateState {
    pub(crate) fn set_updating(&mut self, time_started: DateTime<FixedOffset>) {
        let back_to_back = match self {
            Self::Updating { back_to_back, .. } => *back_to_back,
            _ => None,
        };
        let new_val = Self::Updating {
            time_started,
            back_to_back,
        };
        let _ = mem::replace(self, new_val);
    }
    pub(crate) fn check_and_set_done(&mut self, update_time_started: DateTime<FixedOffset>) {
        if let Self::Updating {
            time_started,
            back_to_back,
        } = &self
        {
            let date_cmp = update_time_started.cmp(time_started);
            let new_val = match (date_cmp, *back_to_back) {
                (Ordering::Less, _) => None,
                (_, None) => Some(Self::UpToDate),
                (Ordering::Equal, Some(_)) => Some(Self::ShouldUpdate),
                (Ordering::Greater, Some(back_to_back)) => {
                    match update_time_started.cmp(&back_to_back) {
                        Ordering::Greater => Some(Self::UpToDate),
                        _ => Some(Self::ShouldUpdate),
                    }
                }
            };
            if let Some(new_val) = new_val {
                let _ = mem::replace(self, new_val);
            }
        }
    }

    pub(crate) fn set_should_update(&mut self, time_of_change: DateTime<FixedOffset>) {
        let new_val = match &self {
            Self::Updating {
                time_started,
                back_to_back,
            } => match (time_started.cmp(&time_of_change), back_to_back) {
                (Ordering::Greater, _) => None,
                (_, None) => Some(Self::Updating {
                    time_started: *time_started,
                    back_to_back: Some(time_of_change),
                }),
                (_, Some(prev_time_of_change)) => {
                    let new_time_of_change =
                        if matches!(prev_time_of_change.cmp(&time_of_change), Ordering::Greater) {
                            prev_time_of_change
                        } else {
                            &time_of_change
                        };
                    Some(Self::Updating {
                        time_started: *time_started,
                        back_to_back: Some(*new_time_of_change),
                    })
                }
            },
            Self::ShouldUpdate => None,
            Self::UpToDate => Some(Self::ShouldUpdate),
        };
        if let Some(new_val) = new_val {
            let _ = mem::replace(self, new_val);
        }
    }
}
