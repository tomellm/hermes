use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use diesel::{
    backend::Backend,
    debug_query,
    query_builder::{self, AsQuery, IntoBoxedClause, Query, QueryFragment, QueryId},
    query_dsl::methods::{ExecuteDsl, LoadQuery},
    Connection, QueryResult, RunQueryDsl,
};
use tokio::{
    sync::{
        mpsc,
        oneshot::{self, error::TryRecvError},
        Mutex,
    },
    task,
};
use tracing::error;

use crate::messenger::ContainerData;

pub struct Container<Value, Database>
where
    Database: Connection + 'static,
    Value: Send + 'static,
{
    pool: Arc<Mutex<Database>>,
    all_tables: Vec<String>,
    interesting_tables: Vec<String>,
    values: Vec<Value>,
    executing_query: Option<ExecutingQuery<Value>>,
    tables_interested_sender: mpsc::Sender<Vec<String>>,
    executing_executes: Vec<ExecutingExecute>,
    tables_changed_sender: mpsc::Sender<Vec<String>>,
    should_update: Arc<AtomicBool>,
    new_register_sender: mpsc::Sender<ContainerData>,
}

impl<Value, Database> Container<Value, Database>
where
    Database: Connection + 'static,
    Value: Send + 'static,
{
    pub fn new(
        pool: Arc<Mutex<Database>>,
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
            self.new_register_sender.clone()
        )
    }
    pub fn should_refresh(&self) -> bool {
        self.should_update.load(Ordering::Relaxed)
    }
    pub fn state_update(&mut self) {
        let executing_query = self.executing_query.take();
        if let Some(mut executing_query) = executing_query {
            match executing_query.reciever.try_recv() {
                Ok(Ok(values)) => {
                    let tables = executing_query.interested_tables;
                    self.interesting_tables = tables.clone();
                    let sender = self.tables_interested_sender.clone();
                    task::spawn(async move {
                        let _ = sender.send(tables).await;
                    });
                    self.values = values;
                }
                Ok(Err(error)) => {
                    error!("{error}");
                }
                Err(TryRecvError::Closed) => (),
                Err(TryRecvError::Empty) => {
                    let _ = self.executing_query.insert(executing_query);
                }
            }
        }

        self.executing_executes
            .retain_mut(|execute| match execute.reciever.try_recv() {
                Ok(Ok(_)) => {
                    let tables = execute.affected_tables.clone();
                    let sender = self.tables_changed_sender.clone();
                    task::spawn(async move {
                        let _ = sender.send(tables).await;
                    });
                    false
                }
                Ok(Err(error)) => {
                    error!("{error}");
                    false
                }
                Err(TryRecvError::Closed) => false,
                Err(TryRecvError::Empty) => true,
            });
    }
    pub fn query<Query>(&mut self, query: Query)
    where
        for<'query> Query: IntoBoxedClause<'query, Database::Backend>
            + LoadQuery<'query, Database, Value>
            + QueryFragment<<Database as Connection>::Backend>
            + 'static,
        for<'query> <Query as IntoBoxedClause<'query, Database::Backend>>::BoxedClause:
            LoadQuery<'query, Database, Value>
                + QueryFragment<<Database as Connection>::Backend>
                + query_builder::Query
                + Send,
        for<'query> <<Query as IntoBoxedClause<'query, Database::Backend>>::BoxedClause as AsQuery>::Query:
            QueryId,
        <Database as Connection>::Backend: Default,
        <<Database>::Backend as Backend>::QueryBuilder: Default,
    {
        let tables = self.get_tables_present(&query);
        let (sender, reciever) = oneshot::channel();
        let pool = self.pool.clone();
        let boxed_query = query.into_boxed();
        task::spawn(async move {
            let result = boxed_query.load(&mut *pool.lock().await);
            let _ = sender.send(result);
        });
        let _ = self
            .executing_query
            .insert(ExecutingQuery::new(tables, reciever));
    }
    pub fn execute<Execute>(&mut self, execute: Execute)
    where
        for<'query> Execute: IntoBoxedClause<'query, Database::Backend>
            + RunQueryDsl<Database>
            + ExecuteDsl<Database>
            + QueryFragment<<Database as Connection>::Backend>
            + 'static,
        for<'query> <Execute as IntoBoxedClause<'query, Database::Backend>>::BoxedClause:
            ExecuteDsl<Database>
                + RunQueryDsl<Database>
                + QueryFragment<<Database as Connection>::Backend>
                + Query
                + Send,
        for<'query> <<Execute as IntoBoxedClause<'query, Database::Backend>>::BoxedClause as AsQuery>::Query:
            QueryId,
        <Database as Connection>::Backend: Default,
        <<Database>::Backend as Backend>::QueryBuilder: Default,
    {
        let tables = self.get_tables_present(&execute);
        let (sender, reciever) = oneshot::channel();
        let pool = self.pool.clone();
        let boxed_execute = execute.into_boxed();
        tokio::spawn(async move {
            let result = boxed_execute.execute(&mut *pool.lock().await);
            let _ = sender.send(result);
        });
        self.executing_executes
            .push(ExecutingExecute::new(tables, reciever));
    }

    fn get_tables_present<T>(&self, query: &T) -> Vec<String>
    where
        T: QueryFragment<<Database as Connection>::Backend>,
        <Database>::Backend: Default,
        <<Database>::Backend as Backend>::QueryBuilder: Default,
    {
        let sql = format!("{}", debug_query::<Database::Backend, T>(query));
        self.all_tables
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
    reciever: oneshot::Receiver<QueryResult<Vec<Value>>>,
}

impl<Value> ExecutingQuery<Value>
where
    Value: Send + 'static,
{
    fn new(
        interested_tables: Vec<String>,
        reciever: oneshot::Receiver<QueryResult<Vec<Value>>>,
    ) -> Self {
        Self {
            interested_tables,
            reciever,
        }
    }
}

struct ExecutingExecute {
    affected_tables: Vec<String>,
    reciever: oneshot::Receiver<QueryResult<usize>>,
}

impl ExecutingExecute {
    fn new(affected_tables: Vec<String>, reciever: oneshot::Receiver<QueryResult<usize>>) -> Self {
        Self {
            affected_tables,
            reciever,
        }
    }
}
