use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use diesel::{
    backend::Backend,
    debug_query,
    query_builder::QueryFragment,
    query_dsl::methods::{ExecuteDsl, LoadQuery},
    r2d2::{ManageConnection, Pool},
    Connection, QueryResult, RunQueryDsl,
};
use tokio::{
    sync::{
        mpsc,
        oneshot::{self, error::TryRecvError},
    },
    task,
};
use tracing::error;

use crate::messenger::ContainerData;

pub struct Carrier<Database, DbValue>
where
    Database: ManageConnection + 'static,
    DbValue: Send + 'static,
{
    pool: Pool<Database>,
    all_tables: Vec<String>,
    interesting_tables: Vec<String>,
    executing_query: Option<oneshot::Receiver<ExecutingQuery<DbValue>>>,
    tables_interested_sender: mpsc::Sender<Vec<String>>,
    executing_executes: Vec<oneshot::Receiver<ExecutingExecute>>,
    tables_changed_sender: mpsc::Sender<Vec<String>>,
    should_update: Arc<AtomicBool>,
    new_register_sender: mpsc::Sender<ContainerData>,
}

impl<Database, DbValue> Clone for Carrier<Database, DbValue>
where
    Database: ManageConnection + 'static,
    Database::Connection: Connection,
    <Database::Connection as Connection>::Backend: Default,
    <<Database::Connection as Connection>::Backend as Backend>::QueryBuilder: Default,
    DbValue: Send + 'static,
{
    fn clone(&self) -> Self {
        let (tables_interested_sender, tables_interested_reciever) = mpsc::channel(3);
        let should_update = Arc::new(AtomicBool::new(false));

        let sender = self.new_register_sender.clone();
        let data = ContainerData::new(tables_interested_reciever, Arc::clone(&should_update));
        task::spawn(async move {
            let _ = sender.send(data).await;
        });

        Carrier::new(
            self.pool.clone(),
            self.all_tables.clone(),
            tables_interested_sender,
            self.tables_changed_sender.clone(),
            should_update,
            self.new_register_sender.clone(),
        )
    }
}

impl<Database, DbValue> Carrier<Database, DbValue>
where
    Database: ManageConnection + 'static,
    Database::Connection: Connection,
    <Database::Connection as Connection>::Backend: Default,
    <<Database::Connection as Connection>::Backend as Backend>::QueryBuilder: Default,
    DbValue: Send + 'static,
{
    pub fn register_new(
        pool: Pool<Database>,
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

        Carrier::new(
            pool,
            all_tables,
            tables_interested_sender,
            tables_changed_sender,
            should_update,
            new_register_sender,
        )
    }

    pub fn new(
        pool: Pool<Database>,
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
            executing_executes: vec![],
            tables_changed_sender,
            should_update,
            new_register_sender,
        }
    }

    pub fn should_refresh(&self) -> bool {
        self.should_update.load(Ordering::Relaxed)
    }

    pub fn try_resolve_query(&mut self) -> Option<QueryResult<Vec<DbValue>>> {
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

    pub fn try_resolve_executes(&mut self) {
        self.executing_executes
            .retain_mut(|execute| match execute.try_recv() {
                Ok(ExecutingExecute {
                    affected_tables,
                    result: Ok(_),
                }) => {
                    let tables = affected_tables.clone();
                    let sender = self.tables_changed_sender.clone();
                    task::spawn(async move {
                        let _ = sender.send(tables).await;
                    });
                    false
                }
                Ok(ExecutingExecute {
                    result: Err(error), ..
                }) => {
                    error!("{error}");
                    false
                }
                Err(TryRecvError::Closed) => false,
                Err(TryRecvError::Empty) => true,
            });
    }

    pub fn query<Query>(&mut self, query_fn: impl FnOnce() -> Query + Send + 'static)
    where
        for<'query> Query: RunQueryDsl<Database::Connection>
            + QueryFragment<<Database::Connection as Connection>::Backend>
            + LoadQuery<'query, Database::Connection, DbValue>,
    {
        let all_tables = self.all_tables.clone();
        let pool = self.pool.clone();

        let (sender, reciver) = oneshot::channel();

        task::spawn(async move {
            let query = query_fn();
            let tables = Self::get_tables_present(all_tables, &query);
            let conn = &mut pool.get().unwrap();
            let result = query.load(conn);
            let _ = sender.send(ExecutingQuery::new(tables, result));
        });
        #[allow(unused_must_use)]
        self.executing_query.insert(reciver);
    }
    pub fn execute<Execute>(&mut self, execute_fn: impl FnOnce() -> Execute + Send + 'static)
    where
        Execute: RunQueryDsl<Database::Connection>
            + QueryFragment<<Database::Connection as Connection>::Backend>
            + ExecuteDsl<Database::Connection>,
    {
        let all_tables = self.all_tables.clone();
        let pool = self.pool.clone();

        let (sender, reciever) = oneshot::channel();

        task::spawn(async move {
            let execute = execute_fn();
            let tables = Self::get_tables_present(all_tables, &execute);
            let conn = &mut pool.get().unwrap();
            let result = execute.execute(conn);
            let _ = sender.send(ExecutingExecute::new(tables, result));
        });

        self.executing_executes.push(reciever);
    }

    fn get_tables_present<Query>(all_tables: Vec<String>, query: &Query) -> Vec<String>
    where
        Query: RunQueryDsl<Database::Connection>
            + QueryFragment<<Database::Connection as Connection>::Backend>,
    {
        let dbg = debug_query::<<Database::Connection as Connection>::Backend, Query>(query);
        let sql = format!("{dbg}");
        all_tables
            .into_iter()
            .filter_map(|table| sql.find(&format!("`{table}`")).map(|_| table))
            .collect::<Vec<_>>()
    }
}

struct ExecutingQuery<DbValue>
where
    DbValue: Send + 'static,
{
    interested_tables: Vec<String>,
    values: QueryResult<Vec<DbValue>>,
}

impl<DbValue> ExecutingQuery<DbValue>
where
    DbValue: Send + 'static,
{
    fn new(interested_tables: Vec<String>, values: QueryResult<Vec<DbValue>>) -> Self {
        Self {
            interested_tables,
            values,
        }
    }
}

struct ExecutingExecute {
    affected_tables: Vec<String>,
    result: QueryResult<usize>,
}

impl ExecutingExecute {
    fn new(affected_tables: Vec<String>, result: QueryResult<usize>) -> Self {
        Self {
            affected_tables,
            result,
        }
    }
}
