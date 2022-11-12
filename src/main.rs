use geolite::db;
use geolite::errors::GLErr;

#[tokio::main]
async fn main() -> Result<(), GLErr> {
    let _pcodes = db::phone_codes()?;
    db::update_db().await?;
    Ok(())
}
