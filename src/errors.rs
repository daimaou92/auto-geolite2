use thiserror::Error;

#[derive(Debug, Error)]
pub enum GLErr {
    #[error("sqlite error")]
    SqliteErr(#[from] rusqlite::Error),
    #[error("io error")]
    IOErr(#[from] std::io::Error),
    #[error("system time error")]
    TimeError(#[from] std::time::SystemTimeError),
    #[error("parse int error")]
    ParseIntErr(#[from] std::num::ParseIntError),
    #[error("env var not found")]
    MissingEnvVar(#[from] std::env::VarError),
    #[error("reqwest error")]
    ReqwestErr(#[from] reqwest::Error),
    #[error("zip error")]
    ZipError(#[from] zip::result::ZipError),
    #[error("csv errored")]
    CSVError(#[from] csv::Error),
    #[error("tokio join error")]
    TaskJoinError(#[from] tokio::task::JoinError),
    #[error("dir not founs. zip extraction may have failed")]
    ZipExtractError,
    #[error("sqlite cursor.next failed")]
    CursorNextErr,
}
