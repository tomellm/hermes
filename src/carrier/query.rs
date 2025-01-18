use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use sea_orm::{DatabaseConnection, DbErr, EntityTrait, FromQueryResult, QueryTrait, Select};
use sea_query::SqliteQueryBuilder;
use tokio::{
    sync::{
        mpsc,
        oneshot::{self, error::TryRecvError},
    },
    task,
};

use crate::{container::ContainerBuilder, get_tables_present, messenger::ContainerData};

pub struct QueryCarrier<DbValue>
where
    DbValue: EntityTrait + FromQueryResult + Send + 'static,
{
    db: DatabaseConnection,
    all_tables: Vec<String>,

    interesting_tables: Vec<String>,
    executing_query: Option<oneshot::Receiver<ExecutedQuery<DbValue>>>,
    tables_interested_sender: mpsc::Sender<Vec<String>>,
    should_update: Arc<AtomicBool>,

    new_register_sender: mpsc::Sender<ContainerData>,
    tables_changed_sender: mpsc::Sender<Vec<String>>,
}

impl<DbValue> Clone for QueryCarrier<DbValue>
where
    DbValue: EntityTrait + FromQueryResult + Send + 'static,
{
    fn clone(&self) -> Self {
        Self::register_new(
            self.db.clone(),
            self.all_tables.clone(),
            self.tables_changed_sender.clone(),
            self.new_register_sender.clone(),
        )
    }
}

impl<DbValue> QueryCarrier<DbValue>
where
    DbValue: EntityTrait + FromQueryResult + Send,
{
    pub fn register_new(
        pool: DatabaseConnection,
        all_tables: Vec<String>,
        tables_changed_sender: mpsc::Sender<Vec<String>>,
        new_register_sender: mpsc::Sender<ContainerData>,
    ) -> Self {
        let (tables_interested_sender, tables_interested_reciever) = mpsc::channel(3);
        let should_update = Arc::new(AtomicBool::new(false));

        let sender = new_register_sender.clone();
        let data = ContainerData::new(tables_interested_reciever, Arc::clone(&should_update));
        task::spawn(async move {
            let _ = sender.send(data).await;
        });

        Self::new(
            pool,
            all_tables,
            tables_interested_sender,
            tables_changed_sender,
            should_update,
            new_register_sender,
        )
    }

    fn new(
        pool: DatabaseConnection,
        all_tables: Vec<String>,
        tables_interested_sender: mpsc::Sender<Vec<String>>,
        tables_changed_sender: mpsc::Sender<Vec<String>>,
        should_update: Arc<AtomicBool>,
        new_register_sender: mpsc::Sender<ContainerData>,
    ) -> Self {
        Self {
            db: pool,
            all_tables,
            interesting_tables: vec![],
            executing_query: None,
            tables_interested_sender,
            tables_changed_sender,
            should_update,
            new_register_sender,
        }
    }

    pub fn should_refresh(&self) -> bool {
        self.should_update.load(Ordering::Relaxed)
    }

    pub fn try_resolve_query(&mut self) -> Option<Result<Vec<DbValue>, DbErr>> {
        let mut executed_query = Option::take(&mut self.executing_query)?;
        match executed_query.try_recv() {
            Ok(result) => {
                if let ExecutedQuery {
                    interested_tables,
                    query_result: Ok(_),
                } = result
                {
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
        let db = self.db.clone();
        let (sender, reciever) = oneshot::channel();
        let tables = get_tables_present(
            &self.all_tables,
            &query.query().to_string(SqliteQueryBuilder),
        );

        task::spawn(async move {
            let result = query.into_model::<DbValue>().all(&db).await;
            let _ = sender.send(ExecutedQuery::new(tables, result));
        });
        #[allow(unused_must_use)]
        self.executing_query.insert(reciever);
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

struct ExecutedQuery<DbValue>
where
    DbValue: Send + 'static,
{
    interested_tables: Vec<String>,
    query_result: Result<Vec<DbValue>, DbErr>,
}

impl<DbValue> ExecutedQuery<DbValue>
where
    DbValue: Send + 'static,
{
    fn new(interested_tables: Vec<String>, values: Result<Vec<DbValue>, DbErr>) -> Self {
        Self {
            interested_tables,
            query_result: values,
        }
    }
}

pub trait ImplQueryCarrier<DbValue>
where
    DbValue: EntityTrait + FromQueryResult + Send + 'static,
{
    fn should_refresh(&self) -> bool;
    fn query(&mut self, query: Select<DbValue>);
}

impl<T, DbValue> ImplQueryCarrier<DbValue> for T
where
    T: HasQueryCarrier<DbValue>,
    DbValue: EntityTrait + FromQueryResult + Send + 'static,
{
    fn should_refresh(&self) -> bool {
        self.ref_query_carrier().should_refresh()
    }
    fn query(&mut self, query: Select<DbValue>) {
        self.ref_mut_query_carrier().query(query);
    }
}

pub(crate) trait HasQueryCarrier<DbValue>
where
    DbValue: EntityTrait + FromQueryResult + Send + 'static,
{
    fn ref_query_carrier(&self) -> &QueryCarrier<DbValue>;
    fn ref_mut_query_carrier(&mut self) -> &mut QueryCarrier<DbValue>;
}
