use chrono::{DateTime, Duration, Local};
use tokio::sync::oneshot::{self, error::TryRecvError};

pub struct AwaitingTask<O>
where
    O: Send + 'static,
{
    result_reciver: oneshot::Receiver<Vec<O>>,
    time_started: DateTime<Local>,
}

pub enum AwaitingResult<O>
where
    O: Send + 'static,
{
    Recived(Vec<O>),
    Waiting(AwaitingTask<O>),
    Closed,
}

impl<O> AwaitingTask<O>
where
    O: Send + 'static,
{
    pub fn new(result_reciver: oneshot::Receiver<Vec<O>>) -> Self {
        Self {
            result_reciver,
            time_started: Local::now(),
        }
    }

    pub fn try_resolve(mut self) -> AwaitingResult<O> {
        match self.result_reciver.try_recv() {
            Ok(val) => AwaitingResult::Recived(val),
            Err(TryRecvError::Empty) => AwaitingResult::Waiting(self),
            Err(TryRecvError::Closed) => AwaitingResult::Closed,
        }
    }

    pub fn has_timed_out(&self, period: Duration) -> bool {
        (Local::now() - self.time_started) > period
    }
}
