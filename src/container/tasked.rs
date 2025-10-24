mod awaiting_execute;
mod awaiting_result;

use std::future::Future;

use chrono::Duration;
use tokio::{
    sync::oneshot::{self},
    task,
};
use tracing::warn;

use crate::{
    container::{
        create_name,
        data::{Data, HasData},
        tasked::{
            awaiting_execute::AwaitingExecute,
            awaiting_result::{AwaitingResult, AwaitingTask},
        },
    },
    LogDebugErr, LogErr,
};

/// This [Container] can be used to run async tasks in the background
/// and automantically have the data refresh upon finishing of the task.
///
/// The principle is simple. You pass it a function pointer and a init parameter
/// which then instantly starts fetching the data. Whenever you wan't, you can
/// refetch that data or change the parameter
///
/// ```rust
/// use std::future::Future;
/// use tokio::time::{Duration, sleep};
/// use hermes::container::{data::ImplData, tasked::Container};
///
/// pub fn my_fetcher_function(
///     parameter: String
/// ) -> impl Future<Output = Vec<String>> + Send + 'static {
///     async move {
///         sleep(Duration::from_millis(100)).await;
///         vec![ parameter.clone(), parameter.clone(), parameter.clone() ]
///     }
/// }
///
/// async fn tokio_context() {
///     let mut container = Container::new_default_name(
///         my_fetcher_function,
///         String::from("content")
///     );
///     container.set_viewed();
///     assert!(container.data().is_empty());
///     
///     while !container.has_changed() {
///         container.state_update();
///         sleep(Duration::from_millis(100)).await;
///     }
///     
///     assert_eq!(
///         vec![String::from("content"), String::from("content"), String::from("content")],
///         container.data().to_vec()
///     );
/// }
/// ```
pub struct Container<P, Fut, O>
where
    P: Clone + Send + 'static,
    Fut: Future<Output = Vec<O>> + Send + 'static,
    O: Send + 'static,
{
    name: String,
    param: P,
    task_func: fn(P) -> Fut,
    task: Option<AwaitingTask<O>>,
    executes: Vec<AwaitingExecute>,

    data: Data<O>,
    timeout: Duration,
}

impl<P, Fut, O> Container<P, Fut, O>
where
    P: Clone + Send + 'static,
    Fut: Future<Output = Vec<O>> + Send + 'static,
    O: Send + 'static,
{
    /// Creates a new [Container] instance with a default name. Will
    /// instantly start executing the first request with the initial parameters.
    pub fn new_default_name(task_func: fn(P) -> Fut, init_param: P) -> Self {
        Self::new(create_name::<Self, O>(), task_func, init_param)
    }

    /// Creates a new [Container] instance with the defined parameters. Will
    /// instantly start executing the first request with the initial parameters.
    pub fn new(name: String, task_func: fn(P) -> Fut, init_param: P) -> Self {
        Self {
            name,
            param: init_param.clone(),
            task_func,
            task: Some(Self::request(task_func, init_param)),
            executes: vec![],
            data: Data::default(),
            timeout: Duration::seconds(5),
        }
    }

    /// This function is what keeps everything running meaning it always needs
    /// to be called in direct render contexts.
    ///
    /// Specifically it takes care of checking if tasks completed and updating
    /// the Data if they did.
    pub fn state_update(&mut self) {
        self.task = self
            .task
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
            .task
            .as_ref()
            .map(|executing| executing.has_timed_out(self.timeout))
            .unwrap_or_default()
        {
            let _ = self.task.take();
        }
    }

    /// Returns the name of this container
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Sets a new value for the stored parameters and directly starts a new
    /// [Container::refresh] with the new parameters
    pub fn new_params(&mut self, new_params: P) {
        self.param = new_params;
        self.refresh();
    }

    /// Starts a new fetch of data throught the function pointer and the
    /// internally stored parameters. The parameters can be changed with
    /// [Container::new_params]
    pub fn refresh(&mut self) {
        if self.task.is_some() {
            warn!("{} is already executing, can't refresh.", self.name);
        }
        let task = Self::request(self.task_func, self.param.clone());
        let _ = self.task.insert(task);
    }

    /// Allows you to execute some async function. Will automatically requery
    /// data with the stored functionpointer after completion. This means that
    /// the function executed is expected to change the data.
    ///
    /// If you don't intend to requery after execution use [Container::execute_any]
    pub fn execute_changes<ExF>(&mut self, exec_fn: impl FnOnce() -> ExF + Send + 'static)
    where
        ExF: Future<Output = ()> + Send + 'static,
    {
        let execute = Self::execute(exec_fn, true);
        self.executes.push(execute);
    }

    /// Allows you to execute some async function. Will not requery the internal
    /// data automatically after completion.
    ///
    /// If you intend to requery the internal data after completion use [Container::execute_changes]
    pub fn execute_any<ExF>(&mut self, exec_fn: impl FnOnce() -> ExF + Send + 'static)
    where
        ExF: Future<Output = ()> + Send + 'static,
    {
        let execute = Self::execute(exec_fn, false);
        self.executes.push(execute);
    }

    fn execute<ExF>(
        exec_fn: impl FnOnce() -> ExF + Send + 'static,
        changes_data: bool,
    ) -> AwaitingExecute
    where
        ExF: Future<Output = ()> + Send + 'static,
    {
        let (sender, reciver) = oneshot::channel();

        task::spawn(async move {
            let _ = exec_fn().await;
            sender.send(()).log_err();
        });

        AwaitingExecute::new(changes_data, reciver)
    }

    fn request(task_func: fn(P) -> Fut, param: P) -> AwaitingTask<O> {
        let (sender, receiver) = oneshot::channel();

        task::spawn(async move {
            let result = task_func(param).await;
            sender
                .send(result)
                .log_msg("was unable to send new fetched data");
        });

        AwaitingTask::new(receiver)
    }
}

impl<P, Fut, O> HasData<O> for Container<P, Fut, O>
where
    P: Clone + Send + 'static,
    Fut: Future<Output = Vec<O>> + Send + 'static,
    O: Send + 'static,
{
    fn ref_data(&self) -> &Data<O> {
        &self.data
    }

    fn ref_mut_data(&mut self) -> &mut Data<O> {
        &mut self.data
    }
}

#[cfg(test)]
mod tests {
    use crate::container::{data::ImplData, tasked::Container};
    use std::future::Future;

    use tokio::time::{sleep, Duration};

    pub fn test(_: String) -> impl Future<Output = Vec<()>> + Send + 'static {
        #[allow(clippy::unused_unit)]
        async move {
            sleep(Duration::from_millis(10)).await;
            vec![()]
        }
    }

    #[tokio::test]
    async fn default_name_contains_types() {
        let val = Container::new_default_name(test, String::new());

        println!("{}", val.name);
        assert!(val.name.contains("Container"));
        assert!(val.name.contains("()"));
    }

    #[tokio::test]
    async fn can_construct() {
        let val = Container::new(String::from("some_val"), test, String::new());
        assert_eq!(String::from("some_val"), val.name);
    }

    #[tokio::test]
    async fn can_await() {
        const CONTENT: &str = "some very random content that";

        pub fn my_fn(parameter: String) -> impl Future<Output = Vec<String>> + Send + 'static {
            async move {
                sleep(Duration::from_millis(10)).await;
                vec![parameter.clone(), parameter.clone(), parameter.clone()]
            }
        }

        let mut container = Container::new_default_name(my_fn, String::from(CONTENT));
        assert!(container.data().is_empty());
        await_data_change(&mut container).await;

        assert_eq!(
            [CONTENT, CONTENT, CONTENT]
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>(),
            container.data().to_vec()
        );
    }

    async fn await_data_change<P, Fut, O>(cont: &mut Container<P, Fut, O>)
    where
        P: Clone + Send + 'static,
        Fut: Future<Output = Vec<O>> + Send + 'static,
        O: Send + 'static,
    {
        cont.set_viewed();
        while !cont.has_changed() {
            cont.state_update();
            sleep(Duration::from_millis(10)).await;
        }
    }
}
