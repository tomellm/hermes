use diesel::{
    backend::Backend,
    debug_query,
    query_builder::QueryFragment,
    query_dsl::methods::ExecuteDsl,
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

pub(crate) struct ExecuteCarrier<Database>
where
    Database: ManageConnection + 'static,
{
    pool: Pool<Database>,
    all_tables: Vec<String>,
    executing_executes: Vec<oneshot::Receiver<ExecutingExecute>>,
    tables_changed_sender: mpsc::Sender<Vec<String>>,
    new_register_sender: mpsc::Sender<ContainerData>,
}

impl<Database> Clone for ExecuteCarrier<Database>
where
    Database: ManageConnection + 'static,
    Database::Connection: Connection,
    <Database::Connection as Connection>::Backend: Default,
    <<Database::Connection as Connection>::Backend as Backend>::QueryBuilder: Default,
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

impl<Database> ExecuteCarrier<Database>
where
    Database: ManageConnection + 'static,
    Database::Connection: Connection,
    <Database::Connection as Connection>::Backend: Default,
    <<Database::Connection as Connection>::Backend as Backend>::QueryBuilder: Default,
{
    pub fn register_new(
        pool: Pool<Database>,
        all_tables: Vec<String>,
        tables_changed_sender: mpsc::Sender<Vec<String>>,
        new_register_sender: mpsc::Sender<ContainerData>,
    ) -> Self {
        Self::new(pool, all_tables, tables_changed_sender, new_register_sender)
    }

    fn new(
        pool: Pool<Database>,
        all_tables: Vec<String>,
        tables_changed_sender: mpsc::Sender<Vec<String>>,
        new_register_sender: mpsc::Sender<ContainerData>,
    ) -> Self {
        Self {
            pool,
            all_tables,
            executing_executes: vec![],
            tables_changed_sender,
            new_register_sender,
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
