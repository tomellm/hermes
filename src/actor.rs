use sqlx::{Database, Executor, IntoArguments, QueryBuilder};

use crate::carrier::execute::ExecuteCarrier;

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

    pub fn execute<BuildFn>(&mut self, create_execute: BuildFn)
    where
        for<'args, 'intoargs> <DB as Database>::Arguments<'args>: IntoArguments<'intoargs, DB>,
        for<'builder> BuildFn: Fn(&mut QueryBuilder<'builder, DB>) + Clone + Send + 'static,
    {
        self.executor.execute(create_execute);
    }
}
