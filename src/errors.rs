use thiserror::Error;

#[derive(Debug, Error)]
pub enum GLErr {
    #[error("sqlite error")]
    SqliteErr(#[from] rusqlite::Error),
    #[error("io error")]
    IOErr(#[from] std::io::Error),
    #[error("system time error")]
    TimeErr(#[from] std::time::SystemTimeError),
    #[error("parse int error")]
    ParseIntErr(#[from] std::num::ParseIntError),
    #[error("env var not found")]
    MissingEnvVar(#[from] std::env::VarError),
    #[error("reqwest error")]
    ReqwestErr(#[from] reqwest::Error),
    #[error("zip error")]
    ZipErr(#[from] zip::result::ZipError),
    #[error("csv errored")]
    CSVErr(#[from] csv::Error),
    #[error("tokio join error")]
    TaskJoinErr(#[from] tokio::task::JoinError),
    #[error("serde-json error")]
    SerdeJSONErr(#[from] serde_json::Error),
    #[error("dir not found. zip extraction may have failed")]
    ZipExtractErr,
    #[error("sqlite cursor.next failed")]
    CursorNextErr,
    #[error("returned osstring")]
    OSStringErr,
}
