use std::collections::HashSet;

use sea_orm::{
    ConnectionTrait, DatabaseConnection, DbBackend, DbErr, QueryTrait, Statement, TransactionTrait,
};
use tokio::{sync::mpsc, task};
use tracing::error;

use crate::{actor::Actor, get_tables_present, messenger::ContainerData};

pub(crate) struct ExecuteCarrier {
    db: DatabaseConnection,
    all_tables: Vec<String>,

    executing_executes: mpsc::Receiver<ExecuteResult>,
    _bk_executing_sender: mpsc::Sender<ExecuteResult>,

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
        let (sender, reciver) = mpsc::channel(20);

        Self {
            db: pool,
            all_tables,
            executing_executes: reciver,
            _bk_executing_sender: sender,
            tables_changed_sender,
            new_register_sender,
        }
    }

    pub fn try_resolve_executes(&mut self) {
        loop {
            let recived = self.executing_executes.try_recv();
            match recived {
                Ok(Ok(affected_tables)) => {
                    let tables = affected_tables.clone();
                    let sender = self.tables_changed_sender.clone();
                    task::spawn(async move {
                        let _ = sender.send(tables).await;
                    });
                }
                Ok(Err(error)) => error!("{error}"),
                Err(mpsc::error::TryRecvError::Empty) => break,
                _ => unreachable!(),
            }
        }
    }

    pub fn execute(&mut self, execute: impl QueryTrait + Send + 'static) {
        let db = self.db.clone();
        let sender = self._bk_executing_sender.clone();

        let execute = execute.build(DbBackend::Sqlite);
        let tables = get_tables_present(&self.all_tables, &execute.to_string());

        task::spawn(async move {
            let result = db.execute(execute).await.map(|_| tables);
            sender.send(result).await.unwrap();
        });
    }

    pub fn action<E>(&self) -> impl Fn(E)
    where
        E: QueryTrait + Send + 'static,
    {
        let all_tables = self.all_tables.clone();
        let db = self.db.clone();
        let sender = self._bk_executing_sender.clone();

        move |execute: E| {
            let db = db.clone();
            let sender = sender.clone();

            let execute = execute.build(DbBackend::Sqlite);
            let tables = get_tables_present(&all_tables, &execute.to_string());

            task::spawn(async move {
                let result = db.execute(execute).await.map(|_| tables);
                sender.send(result).await.unwrap();
            });
        }
    }

    pub fn execute_many(&mut self, transaction_builder: impl FnOnce(&mut TransactionBuilder)) {
        let db = self.db.clone();
        let sender = self._bk_executing_sender.clone();

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
                } in executes
                {
                    txn.execute(execute).await?;
                    tables.extend(interested_tables);
                }
                txn.commit().await?;
                Ok(tables.into_iter().collect::<Vec<_>>())
            };

            sender.send(transaction().await).await.unwrap();
        });
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

    pub fn execute(&mut self, execute: impl QueryTrait + Send + 'static) -> &mut Self {
        let transaction_execute = TransactionExecute::from_execute(execute, self.all_tables);
        self.executes.push(transaction_execute);
        self
    }
}

struct TransactionExecute {
    interested_tables: Vec<String>,
    execute: Statement,
}

impl TransactionExecute {
    pub fn from_execute(execute: impl QueryTrait + Send + 'static, all_tables: &[String]) -> Self {
        let execute = execute.build(DbBackend::Sqlite);
        let interested_tables = get_tables_present(all_tables, &execute.to_string());
        Self {
            interested_tables,
            execute,
        }
    }
}

type ExecuteResult = Result<Vec<String>, DbErr>;

pub trait ImplExecuteCarrier {
    fn actor(&self) -> Actor;
    fn action<E>(&self) -> impl Fn(E)
    where
        E: QueryTrait + Send + 'static;
    fn execute(&mut self, execute: impl QueryTrait + Send + 'static);
    fn execute_many(&mut self, transaction_builder: impl FnOnce(&mut TransactionBuilder));
}

impl<T> ImplExecuteCarrier for T
where
    T: HasExecuteCarrier,
{
    fn actor(&self) -> Actor {
        Actor::new(self.ref_execute_carrier().clone())
    }
    fn action<E>(&self) -> impl Fn(E)
    where
        E: QueryTrait + Send + 'static,
    {
        self.ref_execute_carrier().action()
    }
    fn execute(&mut self, create_execute: impl QueryTrait + Send + 'static) {
        self.ref_mut_execute_carrier().execute(create_execute);
    }
    fn execute_many(&mut self, transaction_builder: impl FnOnce(&mut TransactionBuilder)) {
        self.ref_mut_execute_carrier()
            .execute_many(transaction_builder);
    }
}

pub(crate) trait HasExecuteCarrier {
    fn ref_execute_carrier(&self) -> &ExecuteCarrier;
    fn ref_mut_execute_carrier(&mut self) -> &mut ExecuteCarrier;
}
