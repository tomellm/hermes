use std::
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    }
;

use sqlx::{Database, Execute, Executor, FromRow, IntoArguments, Pool, QueryBuilder};
use sqlx_projector::builder;
use tokio::{
    sync::{
        mpsc,
        oneshot::{self, error::TryRecvError},
    },
    task,
};

use crate::{container::ContainerBuilder, get_tables_present, messenger::ContainerData};

pub struct QueryCarrier<DB, DbValue>
where
    DB: Database,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    for<'row> DbValue: FromRow<'row, DB::Row> + Send + 'static,
{
    pool: Arc<Pool<DB>>,
    all_tables: Vec<String>,

    interesting_tables: Vec<String>,
    executing_query: Option<oneshot::Receiver<ExecutingQuery<DbValue>>>,
    tables_interested_sender: mpsc::Sender<Vec<String>>,
    should_update: Arc<AtomicBool>,

    new_register_sender: mpsc::Sender<ContainerData>,
    tables_changed_sender: mpsc::Sender<Vec<String>>,
}

impl<DB, DbValue> Clone for QueryCarrier<DB, DbValue>
where
    DB: Database,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    for<'row> DbValue: FromRow<'row, DB::Row> + Send + 'static,
{
    fn clone(&self) -> Self {
        Self::register_new(
            self.pool.clone(),
            self.all_tables.clone(),
            self.tables_changed_sender.clone(),
            self.new_register_sender.clone(),
        )
    }
}

impl<DB, DbValue> QueryCarrier<DB, DbValue>
where
    DB: Database,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    for<'row> DbValue: FromRow<'row, DB::Row> + Send,
{
    pub fn register_new(
        pool: Arc<Pool<DB>>,
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
        pool: Arc<Pool<DB>>,
        all_tables: Vec<String>,
        tables_interested_sender: mpsc::Sender<Vec<String>>,
        tables_changed_sender: mpsc::Sender<Vec<String>>,
        should_update: Arc<AtomicBool>,
        new_register_sender: mpsc::Sender<ContainerData>,
    ) -> Self {
        Self {
            pool,
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

    pub fn try_resolve_query(&mut self) -> Option<sqlx::Result<Vec<DbValue>>> {
        let mut executing_query = self.executing_query.take()?;
        match executing_query.try_recv() {
            Ok(result) => {
                if let ExecutingQuery {
                    interested_tables,
                    values: Ok(_),
                } = result
                {
                    self.interesting_tables = interested_tables.clone();
                    let sender = self.tables_interested_sender.clone();
                    task::spawn(async move {
                        let _ = sender.send(interested_tables).await;
                    });
                }
                Some(result.values)
            }
            Err(TryRecvError::Closed) => None,
            Err(TryRecvError::Empty) => {
                #[allow(unused_must_use)]
                self.executing_query.insert(executing_query);
                None
            }
        }
    }

    pub fn query<BuildFn>(&mut self, create_query: BuildFn)
    where
        DbValue: Unpin,
        for<'args, 'intoargs> <DB as Database>::Arguments<'args>: IntoArguments<'intoargs, DB>,
        for<'builder> BuildFn: Fn(&mut QueryBuilder<'builder, DB>) + Clone + Send + 'static,
    {
        let all_tables = self.all_tables.clone();
        let pool = self.pool.clone();
        let (sender, reciever) = oneshot::channel();

        task::spawn(async move {
            let mut builder = builder();
            create_query(&mut builder);
            let query = builder.build();

            let tables = get_tables_present(all_tables, query.sql());

            let result = query
                .map(|v| DbValue::from_row(&v).unwrap())
                .fetch_all(&*pool)
                .await;
            let _ = sender.send(ExecutingQuery::new(tables, result));
        });
        #[allow(unused_must_use)]
        self.executing_query.insert(reciever);
    }

    pub fn builder(&self) -> ContainerBuilder<DB> {
        ContainerBuilder::new(
            self.pool.clone(),
            self.all_tables.clone(),
            self.tables_changed_sender.clone(),
            self.new_register_sender.clone(),
        )
    }
}

struct ExecutingQuery<DbValue>
where
    DbValue: Send + 'static,
{
    interested_tables: Vec<String>,
    values: sqlx::Result<Vec<DbValue>>,
}

impl<DbValue> ExecutingQuery<DbValue>
where
    DbValue: Send + 'static,
{
    fn new(interested_tables: Vec<String>, values: sqlx::Result<Vec<DbValue>>) -> Self {
        Self {
            interested_tables,
            values,
        }
    }
}
