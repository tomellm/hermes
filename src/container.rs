use std::{
    fmt::Display,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use tracing::error;

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

use crate::messenger::ContainerData;

pub struct Container<Value, Database>
where
    Database: ManageConnection + 'static,
    Value: Send + 'static,
{
    pool: Pool<Database>,
    all_tables: Vec<String>,
    interesting_tables: Vec<String>,
    values: Vec<Value>,
    executing_query: Option<oneshot::Receiver<ExecutingQuery<Value>>>,
    tables_interested_sender: mpsc::Sender<Vec<String>>,
    executing_executes: Vec<oneshot::Receiver<ExecutingExecute>>,
    tables_changed_sender: mpsc::Sender<Vec<String>>,
    should_update: Arc<AtomicBool>,
    new_register_sender: mpsc::Sender<ContainerData>,
}

impl<Value, Database> Container<Value, Database>
where
    Database: ManageConnection + 'static,
    Database::Connection: Connection,
    <Database::Connection as Connection>::Backend: Default,
    <<Database::Connection as Connection>::Backend as Backend>::QueryBuilder: Default,
    Value: Send + 'static,
{
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
            values: vec![],
            executing_query: None,
            tables_interested_sender,
            executing_executes: vec![],
            tables_changed_sender,
            should_update,
            new_register_sender,
        }
    }

    pub fn state_update(&mut self) {
        let executing_query = self.executing_query.take();
        if let Some(mut executing_query) = executing_query {
            match executing_query.try_recv() {
                Ok(ExecutingQuery {
                    interested_tables,
                    values: Ok(values),
                }) => {
                    self.interesting_tables = interested_tables.clone();
                    let sender = self.tables_interested_sender.clone();
                    task::spawn(async move {
                        let _ = sender.send(interested_tables).await;
                    });
                    self.values = values;
                }
                Ok(ExecutingQuery {
                    values: Err(error), ..
                }) => {
                    error!("{error}");
                }
                Err(TryRecvError::Closed) => (),
                Err(TryRecvError::Empty) => {
                    let _ = self.executing_query.insert(executing_query);
                }
            }
        }

        self.executing_executes
            .retain_mut(|execute| match execute.try_recv() {
                Ok(ExecutingExecute { affected_tables, result: Ok(_) }) => {
                    let tables = affected_tables.clone();
                    let sender = self.tables_changed_sender.clone();
                    task::spawn(async move {
                        let _ = sender.send(tables).await;
                    });
                    false
                }
                Ok(ExecutingExecute { result: Err(error), .. }) => {
                    error!("{error}");
                    false
                }
                Err(TryRecvError::Closed) => false,
                Err(TryRecvError::Empty) => true,
            });
    }
    pub async fn register_new(&self) -> Self {
        let (tables_interested_sender, tables_interested_reciever) = mpsc::channel(3);
        let should_update = Arc::new(AtomicBool::new(false));

        let sender = self.new_register_sender.clone();
        let data = ContainerData::new(tables_interested_reciever, Arc::clone(&should_update));
        task::spawn(async move {
            let _ = sender.send(data).await;
        });

        Container::new(
            self.pool.clone(),
            self.all_tables.clone(),
            tables_interested_sender,
            self.tables_changed_sender.clone(),
            should_update,
            self.new_register_sender.clone(),
        )
    }
    pub fn should_refresh(&self) -> bool {
        self.should_update.load(Ordering::Relaxed)
    }
    pub fn query<Query>(&mut self, query_fn: impl FnOnce() -> Query + Send + 'static)
    where
        for<'query> Query: RunQueryDsl<Database::Connection>
            + QueryFragment<<Database::Connection as Connection>::Backend>
            + LoadQuery<'query, Database::Connection, Value>,
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
        let _ = self.executing_query.insert(reciver);
    }
    pub fn execute<Execute>(&mut self, execute_fn: impl FnOnce() -> Execute + Send + 'static)
    where
        Execute: RunQueryDsl<Database::Connection>
            + QueryFragment<<Database::Connection as Connection>::Backend>
            + ExecuteDsl<Database::Connection>
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
            .iter()
            .filter_map(|table| {
                sql.find(&format!("`{table}`"))
                    .map(|pos| sql[pos..table.len()].to_string())
            })
            .collect::<Vec<_>>()
    }
}

struct ExecutingQuery<Value>
where
    Value: Send + 'static,
{
    interested_tables: Vec<String>,
    values: QueryResult<Vec<Value>>,
}

impl<Value> ExecutingQuery<Value>
where
    Value: Send + 'static,
{
    fn new(interested_tables: Vec<String>, values: QueryResult<Vec<Value>>) -> Self {
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
