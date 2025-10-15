use std::{cmp::Ordering, mem};

use crate::{container::ContainerBuilder, messenger::ContainerData, TablesCollector};
use chrono::{DateTime, FixedOffset};
use sea_orm::{DatabaseConnection, DbErr};
use tokio::{
    sync::{
        mpsc,
        oneshot::{self, error::TryRecvError},
    },
    task,
};
use tracing::trace;

pub struct QueryCarrier<Value>
where
    Value: Send + 'static,
{
    pub(super) name: String,

    pub(super) db: DatabaseConnection,
    pub(super) all_tables: Vec<String>,

    interesting_tables: Vec<String>,
    pub(super) executing_query: Option<oneshot::Receiver<ExecutedQuery<Value>>>,
    tables_interested_sender: mpsc::Sender<Vec<String>>,

    pub(super) should_update: UpdateState,
    time_of_change_reciver: mpsc::Receiver<DateTime<FixedOffset>>,

    new_register_sender: mpsc::Sender<ContainerData>,
    tables_changed_sender: mpsc::Sender<Vec<String>>,
}

impl<Value> Clone for QueryCarrier<Value>
where
    Value: Send + 'static,
{
    fn clone(&self) -> Self {
        Self::register_new(
            self.name.clone(),
            self.db.clone(),
            self.all_tables.clone(),
            self.tables_changed_sender.clone(),
            self.new_register_sender.clone(),
        )
    }
}

impl<Value> QueryCarrier<Value>
where
    Value: Send,
{
    pub fn register_new(
        name: String,
        pool: DatabaseConnection,
        all_tables: Vec<String>,
        tables_changed_sender: mpsc::Sender<Vec<String>>,
        new_register_sender: mpsc::Sender<ContainerData>,
    ) -> Self {
        let (tables_interested_sender, tables_interested_reciever) = mpsc::channel(3);

        let sender = new_register_sender.clone();
        let (update_sender, update_reciver) = mpsc::channel(10);
        let data = ContainerData::new(tables_interested_reciever, update_sender);
        task::spawn(async move {
            let _ = sender.send(data).await;
        });

        Self::new(
            name,
            pool,
            all_tables,
            tables_interested_sender,
            tables_changed_sender,
            update_reciver,
            new_register_sender,
        )
    }

    fn new(
        name: String,
        pool: DatabaseConnection,
        all_tables: Vec<String>,
        tables_interested_sender: mpsc::Sender<Vec<String>>,
        tables_changed_sender: mpsc::Sender<Vec<String>>,
        update_reciver: mpsc::Receiver<DateTime<FixedOffset>>,
        new_register_sender: mpsc::Sender<ContainerData>,
    ) -> Self {
        Self {
            name,
            db: pool,
            all_tables,
            interesting_tables: vec![],
            executing_query: None,
            tables_interested_sender,
            tables_changed_sender,
            should_update: UpdateState::UpToDate,
            time_of_change_reciver: update_reciver,
            new_register_sender,
        }
    }

    pub fn should_refresh(&self) -> bool {
        matches!(self.should_update, UpdateState::ShouldUpdate)
    }

    pub fn try_recive_should_update(&mut self) {
        if let Ok(time_of_change) = self.time_of_change_reciver.try_recv() {
            self.should_update.set_should_update(time_of_change);
        }
    }

    pub fn try_resolve_query(&mut self) -> Option<Result<Vec<Value>, DbErr>> {
        let mut executed_query = Option::take(&mut self.executing_query)?;
        match executed_query.try_recv() {
            Ok(result) => {
                if let ExecutedQuery {
                    interested_tables,
                    query_result: Ok(_),
                    time_started,
                } = result
                {
                    self.should_update.check_and_set_done(time_started);
                    self.interesting_tables = interested_tables.clone();
                    let sender = self.tables_interested_sender.clone();
                    task::spawn(async move {
                        let _ = sender.send(interested_tables).await;
                    });
                }
                Some(result.query_result)
            }
            Err(TryRecvError::Closed) => None,
            Err(TryRecvError::Empty) => {
                #[allow(unused_must_use)]
                self.executing_query.insert(executed_query);
                None
            }
        }
    }

    pub fn builder(&self) -> ContainerBuilder {
        ContainerBuilder::new(
            self.db.clone(),
            self.all_tables.clone(),
            self.tables_changed_sender.clone(),
            self.new_register_sender.clone(),
        )
    }
}

pub trait ImplQueryCarrier<Value>
where
    Value: Send + 'static,
{
    fn should_refresh(&self) -> bool;
    fn try_recive_should_update(&mut self);
    fn try_resolve_query(&mut self) -> Option<Result<Vec<Value>, DbErr>>;
    fn builder(&self) -> crate::container::ContainerBuilder;
}

impl<T, Value> ImplQueryCarrier<Value> for T
where
    T: HasQueryCarrier<Value>,
    Value: Send + 'static,
{
    fn should_refresh(&self) -> bool {
        self.ref_query_carrier().should_refresh()
    }

    fn try_recive_should_update(&mut self) {
        self.ref_mut_query_carrier().try_recive_should_update();
    }

    fn try_resolve_query(&mut self) -> Option<Result<Vec<Value>, DbErr>> {
        self.ref_mut_query_carrier().try_resolve_query()
    }

    fn builder(&self) -> crate::container::ContainerBuilder {
        self.ref_query_carrier().builder()
    }
}

pub(crate) trait HasQueryCarrier<Value>
where
    Value: Send + 'static,
{
    fn ref_query_carrier(&self) -> &QueryCarrier<Value>;
    fn ref_mut_query_carrier(&mut self) -> &mut QueryCarrier<Value>;
}

pub struct ExecutedQuery<Value>
where
    Value: Send + 'static,
{
    interested_tables: Vec<String>,
    query_result: Result<Vec<Value>, DbErr>,
    time_started: DateTime<FixedOffset>,
}

impl<Value> ExecutedQuery<Value>
where
    Value: Send + 'static,
{
    pub fn new(
        interested_tables: Vec<String>,
        values: Result<Vec<Value>, DbErr>,
        time_started: DateTime<FixedOffset>,
    ) -> Self {
        Self {
            interested_tables,
            query_result: values,
            time_started,
        }
    }

    pub fn new_collector(collector: TablesCollector, values: Result<Vec<Value>, DbErr>) -> Self {
        Self {
            interested_tables: collector.tables.into_iter().collect(),
            query_result: values,
            time_started: collector.time_started,
        }
    }
}

#[derive(Copy, Clone)]
pub(crate) enum UpdateState {
    ShouldUpdate,
    Updating {
        time_started: DateTime<FixedOffset>,
        back_to_back: Option<DateTime<FixedOffset>>,
    },
    UpToDate,
}

impl UpdateState {
    pub fn display(&self) -> &'static str {
        match self {
            UpdateState::ShouldUpdate => "ShouldUpdate",
            UpdateState::Updating { .. } => "Updating",
            UpdateState::UpToDate => "UpToDate",
        }
    }

    pub fn log_state_change(&self, new_val: &Self) {
        trace!(
            "Internal query UpdateState is being changed from {} to {}",
            self.display(),
            new_val.display()
        )
    }

    /// The the enum that we have started updating, with the starttime of `time_started`
    ///
    /// * `time_started` - The time at which the Update started, also acts as an
    ///   identifier for the update.
    pub(crate) fn set_updating(&mut self, time_started: DateTime<FixedOffset>) {
        let time_started = match self {
            Self::Updating {
                back_to_back: Some(back_to_back),
                ..
            } => match time_started.cmp(back_to_back) {
                Ordering::Less => *back_to_back,
                _ => time_started,
            },
            _ => time_started,
        };
        let new_val = Self::Updating {
            time_started,
            back_to_back: None,
        };
        self.log_state_change(&new_val);
        let _ = mem::replace(self, new_val);
    }

    /// When an update is done then the state needs to be set to `UpToDate`
    /// but as there can be multiple states beeing updated at the same time
    /// the `update_time_state` is used to check which update is the one beeing
    /// resolved
    ///
    /// * `update_time_state` - Time at which the update trying to be resolved
    ///   has been started
    pub(crate) fn check_and_set_done(&mut self, update_time_started: DateTime<FixedOffset>) {
        if let Self::Updating {
            time_started,
            back_to_back,
        } = &self
        {
            trace!(
                "Checking to see if update is done, comparing expected '{}' with '{}'",
                update_time_started.to_string(),
                time_started.to_string()
            );
            // compare the expected time and the actual time started
            let date_cmp = update_time_started.cmp(time_started);
            let new_val = match (date_cmp, *back_to_back) {
                // if the current time started is more recent do noething
                (Ordering::Less, _) => None,
                // if the current update is Equal of Greater then we are
                // UpToDate and done
                (_, None) => Some(Self::UpToDate),
                // if there is a back to back then compare its time and in case
                // schedule a new update
                (_, Some(back_to_back)) => match update_time_started.cmp(&back_to_back) {
                    Ordering::Greater => Some(Self::UpToDate),
                    _ => Some(Self::ShouldUpdate),
                },
            };
            if let Some(new_val) = new_val {
                self.log_state_change(&new_val);
                let _ = mem::replace(self, new_val);
            }
        }
    }

    /// Sets this enums value anew depending on when a specific change
    /// happend. Meaning that if we think we are up to date we now we need to
    /// update for a change that happend at `time_of_change` time. But if we are
    /// already updating for a change after `time_of_change` then we can ignore
    /// the state. If on the other hand we are updating for a change that happend
    /// before the `time_of_change` we have to update back to back.
    ///
    /// * `time_of_change` - The time at which a change happend, used to consider
    ///   if a we are currently not already working on a newer change
    pub(crate) fn set_should_update(&mut self, time_of_change: DateTime<FixedOffset>) {
        let new_val = match &self {
            // we are currently already updating
            Self::Updating {
                time_started,
                back_to_back,
            } => match (time_started.cmp(&time_of_change), back_to_back) {
                // we are already updating for newer change
                (Ordering::Greater, _) => None,
                // we are updating for same or older change and not already back to back
                (_, None) => Some(Self::Updating {
                    time_started: *time_started,
                    back_to_back: Some(time_of_change),
                }),
                // we are updating for same or older change and its already back to back
                (_, Some(prev_time_of_change)) => {
                    // get more recent time
                    let new_time_of_change = match prev_time_of_change.cmp(&time_of_change) {
                        Ordering::Greater => prev_time_of_change,
                        _ => &time_of_change,
                    };
                    Some(Self::Updating {
                        time_started: *time_started,
                        back_to_back: Some(*new_time_of_change),
                    })
                }
            },
            // we already know an update should happen so no change in state
            Self::ShouldUpdate => None,
            // we think we are up to date, so change to communicate imminent update needed
            Self::UpToDate => Some(Self::ShouldUpdate),
        };
        if let Some(new_val) = new_val {
            self.log_state_change(&new_val);
            let _ = mem::replace(self, new_val);
        }
    }
}
