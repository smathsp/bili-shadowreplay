pub mod account;
pub mod config;
pub mod macros;
pub mod message;
pub mod migrate;
pub mod recorder;
pub mod summary;
pub mod task;
pub mod utils;
pub mod video;
pub mod video_editing;

use crate::database::account::AccountRow;

#[derive(serde::Serialize)]
pub struct AccountInfo {
    pub accounts: Vec<AccountRow>,
}
