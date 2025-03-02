use sea_orm::{DatabaseConnection, QueryTrait};
use tokio::sync::mpsc;

use crate::carrier::execute::{
    ExecuteCarrier, ExecuteResult, HasExecuteCarrier, ImplExecuteCarrier,
};

#[derive(Clone)]
pub struct Actor {
    db: DatabaseConnection,
    all_tables: Vec<String>,

    _bk_executing_sender: mpsc::Sender<ExecuteResult>,
}

impl Actor {
    pub fn new(
        db: DatabaseConnection,
        all_tables: &[String],
        _bk_executing_sender: mpsc::Sender<ExecuteResult>,
    ) -> Self {
        Self {
            db,
            all_tables: all_tables.to_vec(),
            _bk_executing_sender,
        }
    }
}

impl ImplExecuteCarrier for Actor {
    fn actor(&self) -> Actor {
        self.clone()
    }

    fn action<E>(&self) -> impl Fn(E)
    where
        E: QueryTrait + Send + 'static,
    {
        let all_tables = self.all_tables.clone();
        let db = self.db.clone();
        let sender = self._bk_executing_sender.clone();

        move |execute: E| {
            let db = db.clone();
            let sender = sender.clone();

            ExecuteCarrier::execute_static(db, sender, &all_tables, execute);
        }
    }

    fn execute(&mut self, execute: impl QueryTrait + Send + 'static) {
        ExecuteCarrier::execute_static(
            self.db.clone(),
            self._bk_executing_sender.clone(),
            &self.all_tables,
            execute,
        );
    }

    fn execute_many(
        &mut self,
        transaction_builder: impl FnOnce(&mut crate::carrier::execute::TransactionBuilder),
    ) {
        ExecuteCarrier::execute_many_static(
            self.db.clone(),
            self._bk_executing_sender.clone(),
            &self.all_tables,
            transaction_builder,
        );
    }

    fn many_action<B>(&self) -> impl Fn(B)
    where
        B: FnOnce(&mut crate::carrier::execute::TransactionBuilder),
    {
        let db = self.db.clone();
        let sender = self._bk_executing_sender.clone();
        let all_tables = self.all_tables.clone();

        move |transaction_builder| {
            let db = db.clone();
            let sender = sender.clone();

            ExecuteCarrier::execute_many_static(db, sender, &all_tables, transaction_builder);
        }
    }
}
