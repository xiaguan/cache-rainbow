use serde::de::DeserializeOwned;
use serde::Serialize;

pub trait Value: Serialize + DeserializeOwned {}
