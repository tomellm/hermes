use sqlx::{Database, Executor};

use crate::carrier::execute::{ExecuteCarrier, HasExecuteCarrier};

pub struct Actor<DB>
where
    DB: Database,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
{
    executor: ExecuteCarrier<DB>,
}

impl<DB> Actor<DB>
where
    DB: Database,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
{
    pub(crate) fn new(executor: ExecuteCarrier<DB>) -> Self {
        Self { executor }
    }
}

impl<DB> HasExecuteCarrier<DB> for Actor<DB>
where
    DB: Database,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
{
    fn ref_execute_carrier(&self) -> &ExecuteCarrier<DB> {
        &self.executor
    }
    fn ref_mut_execute_carrier(&mut self) -> &mut ExecuteCarrier<DB> {
        &mut self.executor
    }
}
