use crate::prelude::*;
use crate::{self as myko_rs};
use partially::Partial;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[myko_item]
pub struct Client {
	#[belongs_to(Server)]
    pub server_id: Arc<str>,
}
