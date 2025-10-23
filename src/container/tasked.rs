use std::future::Future;

use chrono::{DateTime, Duration, Local};
use tokio::{
    sync::oneshot::{self, error::TryRecvError},
    task,
};
use tracing::warn;

use crate::container::data::Data;

pub struct Container<P, Fut, O>
where
    P: Clone + Send + 'static,
    Fut: Future<Output = Vec<O>> + Send + 'static,
    O: Send + 'static,
{
    name: String,
    param: P,
    task_func: fn(P) -> Fut,
    executing: Option<AwaitingTask<O>>,
    data: Data<O>,
    timeout: Duration,
}

impl<P, Fut, O> Container<P, Fut, O>
where
    P: Clone + Send + 'static,
    Fut: Future<Output = Vec<O>> + Send + 'static,
    O: Send + 'static,
{
    pub fn new(name: String, task_func: fn(P) -> Fut, init_param: P) -> Self {
        Self {
            name,
            param: init_param.clone(),
            task_func,
            executing: Some(Self::request(task_func, init_param)),
            data: Data::default(),
            timeout: Duration::seconds(5),
        }
    }

    pub fn state_update(&mut self) {
        self.executing = self
            .executing
            .take()
            .map(|awaiter| match awaiter.try_resolve() {
                AwaitingResult::Recived(values) => {
                    self.data.set(values.into_iter());
                    None
                }
                AwaitingResult::Waiting(awaiting_task) => Some(awaiting_task),
                AwaitingResult::Closed => None,
            })
            .flatten();

        if self
            .executing
            .as_ref()
            .map(|executing| executing.has_timed_out(self.timeout))
            .unwrap_or_default()
        {
            let _ = self.executing.take();
        }
    }

    pub fn refresh(&mut self) {
        if self.executing.is_some() {
            warn!("{} is already executing, can't refresh.", self.name);
        }
        let task = Self::request(self.task_func, self.param.clone());
        let _ = self.executing.insert(task);
    }

    fn request(task_func: fn(P) -> Fut, param: P) -> AwaitingTask<O> {
        let (sender, receiver) = oneshot::channel();

        task::spawn(async move {
            let result = task_func(param).await;
            sender.send(result)
        });

        AwaitingTask::new(receiver)
    }
}

struct AwaitingTask<O>
where
    O: Send + 'static,
{
    result_reciver: oneshot::Receiver<Vec<O>>,
    time_started: DateTime<Local>,
}

enum AwaitingResult<O>
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
    fn new(result_reciver: oneshot::Receiver<Vec<O>>) -> Self {
        Self {
            result_reciver,
            time_started: Local::now(),
        }
    }

    fn try_resolve(mut self) -> AwaitingResult<O> {
        match self.result_reciver.try_recv() {
            Ok(val) => AwaitingResult::Recived(val),
            Err(TryRecvError::Empty) => AwaitingResult::Waiting(self),
            Err(TryRecvError::Closed) => AwaitingResult::Closed,
        }
    }

    fn has_timed_out(&self, period: Duration) -> bool {
        (Local::now() - self.time_started) > period
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;

    use crate::container::tasked::Container;

    pub fn test(_: String) -> impl Future<Output = Vec<()>> + Send + 'static {
        #[allow(clippy::unused_unit)]
        async move {
            vec![()]
        }
    }

    #[test]
    fn can_construct() {
        let val = Container::new(String::from("some_val"), test, String::new());
        assert_eq!(String::from("some_val"), val.name);
    }
}
