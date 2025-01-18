use crate::carrier::execute::{ExecuteCarrier, HasExecuteCarrier};

pub struct Actor {
    executor: ExecuteCarrier,
}

impl Actor {
    pub(crate) fn new(executor: ExecuteCarrier) -> Self {
        Self { executor }
    }
}

impl HasExecuteCarrier for Actor {
    fn ref_execute_carrier(&self) -> &ExecuteCarrier {
        &self.executor
    }
    fn ref_mut_execute_carrier(&mut self) -> &mut ExecuteCarrier {
        &mut self.executor
    }
}
