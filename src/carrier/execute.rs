use std::sync::Arc;

use sqlx::{Database, Execute, Executor, IntoArguments, Pool, QueryBuilder};
use sqlx_projector::builder;
use tokio::{
    sync::{
        mpsc,
        oneshot::{self, error::TryRecvError},
    },
    task,
};
use tracing::error;

use crate::{get_tables_present, messenger::ContainerData};

pub(crate) struct ExecuteCarrier<DB>
where
    DB: Database,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
{
    pool: Arc<Pool<DB>>,
    all_tables: Vec<String>,
    executing_executes: Vec<oneshot::Receiver<ExecutingExecute<DB>>>,
    tables_changed_sender: mpsc::Sender<Vec<String>>,
    new_register_sender: mpsc::Sender<ContainerData>,
}

impl<DB> Clone for ExecuteCarrier<DB>
where
    DB: Database,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
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

impl<DB> ExecuteCarrier<DB>
where
    DB: Database,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
{
    pub fn register_new(
        pool: Arc<Pool<DB>>,
        all_tables: Vec<String>,
        tables_changed_sender: mpsc::Sender<Vec<String>>,
        new_register_sender: mpsc::Sender<ContainerData>,
    ) -> Self {
        Self::new(pool, all_tables, tables_changed_sender, new_register_sender)
    }

    fn new(
        pool: Arc<Pool<DB>>,
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

    pub fn execute<BuildFn>(&mut self, create_execute: BuildFn)
    where
        for<'args, 'intoargs> <DB as Database>::Arguments<'args>: IntoArguments<'intoargs, DB>,
        for<'builder> BuildFn: Fn(&mut QueryBuilder<'builder, DB>) + Clone + Send + 'static,
    {
        let all_tables = self.all_tables.clone();
        let pool = self.pool.clone();
        let (sender, reciever) = oneshot::channel();

        task::spawn(async move {
            let mut builder = builder();
            create_execute(&mut builder);
            let execute = builder.build();

            let tables = get_tables_present(all_tables, execute.sql());
            let query_result = execute.execute(&*pool).await;
            let _ = sender.send(ExecutingExecute {
                affected_tables: tables,
                result: query_result,
            });
        });

        self.executing_executes.push(reciever);
    }
}

struct ExecutingExecute<DB>
where
    DB: Database,
{
    affected_tables: Vec<String>,
    result: sqlx::Result<DB::QueryResult>,
}
