use std::{collections::HashSet, sync::Arc};

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
    executing_executes: Vec<oneshot::Receiver<ExecuteResult>>,
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
                Ok(Ok(affected_tables)) => {
                    let tables = affected_tables.clone();
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
            let execute_result = match execute.execute(&*pool).await {
                Ok(_) => Ok(tables),
                Err(error) => Err(error),
            };
            let _ = sender.send(execute_result);
        });

        self.executing_executes.push(reciever);
    }

    pub fn execute_many<'builder, Builders>(&mut self, executes: Builders)
    where
        for<'args, 'intoargs> <DB as Database>::Arguments<'args>: IntoArguments<'intoargs, DB>,
        Builders: Iterator<Item = QueryBuilder<'builder, DB>> + Send + 'static,
    {
        let all_tables = self.all_tables.clone();
        let pool = self.pool.clone();
        let (sender, reciever) = oneshot::channel();

        task::spawn(async move {
            let mut connection = pool.acquire().await.unwrap();

            sqlx::query("BEGIN TRANSACTION")
                .execute(&mut *connection)
                .await
                .unwrap();

            let mut all_tables_changed = HashSet::new();
            for mut builder in executes {
                let execute = builder.build();
                let tables = get_tables_present(all_tables.clone(), execute.sql());
                match execute.execute(&mut *connection).await {
                    Ok(_) => all_tables_changed.extend(tables),
                    Err(error) => {
                        sender.send(Err(error)).unwrap();
                        sqlx::query("ROLLBACK")
                            .execute(&mut *connection)
                            .await
                            .unwrap();
                        return;
                    }
                }
            }
            sqlx::query("COMMIT TRANSACTION")
                .execute(&mut *connection)
                .await
                .unwrap();

            sender
                .send(Ok(all_tables_changed.into_iter().collect::<Vec<_>>()))
                .unwrap();
        });

        self.executing_executes.push(reciever);
    }
}

type ExecuteResult = Result<Vec<String>, sqlx::error::Error>;

pub trait ImplExecuteCarrier<DB>
where
    DB: Database,
{
    fn execute<BuildFn>(&mut self, create_execute: BuildFn)
    where
        for<'args, 'intoargs> <DB as Database>::Arguments<'args>: IntoArguments<'intoargs, DB>,
        for<'builder> BuildFn: Fn(&mut QueryBuilder<'builder, DB>) + Clone + Send + 'static;

    fn execute_many<'builder, Builders>(&mut self, executes: Builders)
    where
        for<'args, 'intoargs> <DB as Database>::Arguments<'args>: IntoArguments<'intoargs, DB>,
        Builders: Iterator<Item = QueryBuilder<'builder, DB>> + Send + 'static;
}

impl<T, DB> ImplExecuteCarrier<DB> for T
where
    DB: Database,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    T: HasExecuteCarrier<DB>,
{
    fn execute<BuildFn>(&mut self, create_execute: BuildFn)
    where
        for<'args, 'intoargs> <DB as Database>::Arguments<'args>: IntoArguments<'intoargs, DB>,
        for<'builder> BuildFn: Fn(&mut QueryBuilder<'builder, DB>) + Clone + Send + 'static,
    {
        self.ref_mut_execute_carrier().execute(create_execute);
    }
    fn execute_many<'builder, Builders>(&mut self, executes: Builders)
    where
        for<'args, 'intoargs> <DB as Database>::Arguments<'args>: IntoArguments<'intoargs, DB>,
        Builders: Iterator<Item = QueryBuilder<'builder, DB>> + Send + 'static,
    {
        self.ref_mut_execute_carrier().execute_many(executes);
    }
}

pub(crate) trait HasExecuteCarrier<DB>
where
    DB: Database,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
{
    fn ref_mut_execute_carrier(&mut self) -> &mut ExecuteCarrier<DB>;
}
