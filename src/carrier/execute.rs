use std::collections::HashSet;

use sea_orm::{
    ConnectionTrait, DatabaseConnection, DbBackend, DbErr, Statement, TransactionTrait, Values,
};
use sea_query::{QueryStatementWriter, SqliteQueryBuilder};
use tokio::{
    sync::{
        mpsc,
        oneshot::{self, error::TryRecvError},
    },
    task,
};
use tracing::error;

use crate::{actor::Actor, get_tables_present, messenger::ContainerData};

pub(crate) struct ExecuteCarrier {
    db: DatabaseConnection,
    all_tables: Vec<String>,
    executing_executes: Vec<oneshot::Receiver<ExecuteResult>>,
    tables_changed_sender: mpsc::Sender<Vec<String>>,
    new_register_sender: mpsc::Sender<ContainerData>,
}

impl Clone for ExecuteCarrier {
    fn clone(&self) -> Self {
        Self::register_new(
            self.db.clone(),
            self.all_tables.clone(),
            self.tables_changed_sender.clone(),
            self.new_register_sender.clone(),
        )
    }
}

impl ExecuteCarrier {
    pub fn register_new(
        pool: DatabaseConnection,
        all_tables: Vec<String>,
        tables_changed_sender: mpsc::Sender<Vec<String>>,
        new_register_sender: mpsc::Sender<ContainerData>,
    ) -> Self {
        Self::new(pool, all_tables, tables_changed_sender, new_register_sender)
    }

    fn new(
        pool: DatabaseConnection,
        all_tables: Vec<String>,
        tables_changed_sender: mpsc::Sender<Vec<String>>,
        new_register_sender: mpsc::Sender<ContainerData>,
    ) -> Self {
        Self {
            db: pool,
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

    pub fn execute(&mut self, execute: impl QueryStatementWriter + Send + 'static) {
        let db = self.db.clone();
        let (sender, reciever) = oneshot::channel();

        let tables = get_tables_present(&self.all_tables, &execute.to_string(SqliteQueryBuilder));

        task::spawn(async move {
            let (execute, values) = execute.build(SqliteQueryBuilder);
            let result = db
                .execute(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    execute,
                    values,
                ))
                .await
                .map(|_| tables);
            let _ = sender.send(result);
        });

        self.executing_executes.push(reciever);
    }

    pub fn execute_many(&mut self, transaction_builder: impl Fn(&mut TransactionBuilder)) {
        let db = self.db.clone();
        let (sender, reciever) = oneshot::channel();

        let mut builder = TransactionBuilder::new(&self.all_tables);
        transaction_builder(&mut builder);
        let TransactionBuilder { executes, .. } = builder;

        task::spawn(async move {
            let transaction = async move || {
                let txn = db.begin().await?;

                let mut tables = HashSet::new();
                for TransactionExecute {
                    interested_tables,
                    execute,
                    parameters,
                } in executes
                {
                    txn.execute(Statement::from_sql_and_values(
                        DbBackend::Sqlite,
                        execute,
                        parameters,
                    ))
                    .await?;
                    tables.extend(interested_tables);
                }
                txn.commit().await?;
                Ok(tables.into_iter().collect::<Vec<_>>())
            };

            let _ = sender.send(transaction().await);
        });
        self.executing_executes.push(reciever);
    }
}

pub struct TransactionBuilder<'executor> {
    executes: Vec<TransactionExecute>,
    all_tables: &'executor [String],
}

impl<'executor> TransactionBuilder<'executor> {
    fn new(all_tables: &'executor [String]) -> Self {
        Self {
            executes: vec![],
            all_tables,
        }
    }

    pub fn execute(&mut self, execute: impl QueryStatementWriter + Send + 'static) -> &mut Self {
        let transaction_execute = TransactionExecute::from_execute(execute, self.all_tables);
        self.executes.push(transaction_execute);
        self
    }
}

struct TransactionExecute {
    interested_tables: Vec<String>,
    execute: String,
    parameters: Values,
}

impl TransactionExecute {
    pub fn from_execute(
        execute: impl QueryStatementWriter + Send + 'static,
        all_tables: &[String],
    ) -> Self {
        let interested_tables =
            get_tables_present(all_tables, &execute.to_string(SqliteQueryBuilder));
        let (execute, values) = execute.build(SqliteQueryBuilder);
        Self {
            interested_tables,
            execute,
            parameters: values,
        }
    }
}

type ExecuteResult = Result<Vec<String>, DbErr>;

pub trait ImplExecuteCarrier {
    fn actor(&self) -> Actor;
    fn execute(&mut self, execute: impl QueryStatementWriter + Send + 'static);
    fn execute_many(&mut self, transaction_builder: impl Fn(&mut TransactionBuilder));
}

impl<T> ImplExecuteCarrier for T
where
    T: HasExecuteCarrier,
{
    fn actor(&self) -> Actor {
        Actor::new(self.ref_execute_carrier().clone())
    }
    fn execute(&mut self, create_execute: impl QueryStatementWriter + Send + 'static) {
        self.ref_mut_execute_carrier().execute(create_execute);
    }
    fn execute_many(&mut self, transaction_builder: impl Fn(&mut TransactionBuilder)) {
        self.ref_mut_execute_carrier()
            .execute_many(transaction_builder);
    }
}

pub(crate) trait HasExecuteCarrier {
    fn ref_execute_carrier(&self) -> &ExecuteCarrier;
    fn ref_mut_execute_carrier(&mut self) -> &mut ExecuteCarrier;
}
