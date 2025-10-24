use tokio::sync::oneshot;

pub struct AwaitingExecute {
    changes_data: bool,
    reciver: oneshot::Receiver<()>,
}

impl AwaitingExecute {
    pub fn new(changes_data: bool, reciver: oneshot::Receiver<()>) -> Self {
        Self {
            changes_data,
            reciver,
        }
    }
}
