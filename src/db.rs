use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Write},
};

use crate::errors::GLErr;
use rusqlite::ToSql;
use serde::Deserialize;

fn build_table_counties(conn: &rusqlite::Connection) -> Result<(), GLErr> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS countries (
        geoname_id INT PRIMARY KEY,
        continent_code TEXT,
        continent_name TEXT,
        country_iso_code TEXT,
        country_name TEXT,
        is_in_eu INT);",
        (),
    )?;
    Ok(())
}

fn build_table_cities(conn: &rusqlite::Connection) -> Result<(), GLErr> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS cities (
        geoname_id INT PRIMARY KEY,
        continent_code TEXT,
        continent_name TEXT,
        country_iso_code TEXT,
        country_name TEXT,
        subdivision_1_iso_code TEXT,
        subdivision_1_name TEXT,
        subdivision_2_iso_code TEXT,
        subdivision_2_name TEXT,
        city_name TEXT,
        metro_code TEXT,
        time_zone TEXT,
        is_in_eu INT);",
        (),
    )?;
    Ok(())
}

fn build_table_countries4(conn: &rusqlite::Connection) -> Result<(), GLErr> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS countries4(
        network TEXT PRIMARY KEY,
        geoname_id INT,
        registered_country_geoname_id INT,
        is_anonymous_proxy INT,
        is_satellite_provider INT)",
        (),
    )?;
    Ok(())
}

fn build_table_countries6(conn: &rusqlite::Connection) -> Result<(), GLErr> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS countries6(
        network TEXT PRIMARY KEY,
        geoname_id INT,
        registered_country_geoname_id INT,
        is_anonymous_proxy INT,
        is_satellite_provider INT)",
        (),
    )?;
    Ok(())
}

fn build_table_cities4(conn: &rusqlite::Connection) -> Result<(), GLErr> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS cities4(
        network TEXT PRIMARY KEY,
        geoname_id INT,
        registered_country_geoname_id INT,
        is_anonymous_proxy INT,
        is_satellite_provider INT,
        postal_code TEXT,
        latitude REAL,
        longitude REAL,
        accuracy_radius_km INT)",
        (),
    )?;
    Ok(())
}

fn build_table_cities6(conn: &rusqlite::Connection) -> Result<(), GLErr> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS cities6(
        network TEXT PRIMARY KEY,
        geoname_id INT,
        registered_country_geoname_id INT,
        is_anonymous_proxy INT,
        is_satellite_provider INT,
        postal_code TEXT,
        latitude REAL,
        longitude REAL,
        accuracy_radius_km INT)",
        (),
    )?;
    Ok(())
}

fn build_table_asn4(conn: &rusqlite::Connection) -> Result<(), GLErr> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS asn4 (
        network TEXT PRIMARY KEY,
        autonomous_system_number INT,
        autonomous_system_org TEXT);",
        (),
    )?;
    Ok(())
}

fn build_table_asn6(conn: &rusqlite::Connection) -> Result<(), GLErr> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS asn6 (
        network TEXT PRIMARY KEY,
        autonomous_system_number INT,
        autonomous_system_org TEXT);",
        (),
    )?;
    Ok(())
}

pub fn build_tables(conn: &rusqlite::Connection) -> Result<(), GLErr> {
    build_table_counties(conn)?;
    build_table_cities(conn)?;
    build_table_countries4(conn)?;
    build_table_countries6(conn)?;
    build_table_cities4(conn)?;
    build_table_cities6(conn)?;
    build_table_asn4(conn)?;
    build_table_asn6(conn)?;
    Ok(())
}

fn update_needed<P: AsRef<std::path::Path>>(dbdir: P) -> bool {
    let p = dbdir.as_ref().join("version");
    let mut f = match File::open(p) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("{:?}", e);
            return true;
        }
    };
    let mut s = String::new();
    match f.read_to_string(&mut s) {
        Ok(_) => {}
        Err(e) => {
            eprintln!("{:?}", e);
            return true;
        }
    }
    let secs = match s.parse::<u64>() {
        Ok(d) => d,
        Err(e) => {
            eprintln!("{:?}", e);
            return true;
        }
    };
    let last = std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs);
    let ds = match std::time::SystemTime::now().duration_since(last) {
        Ok(ds) => ds,
        Err(e) => {
            eprintln!("{:?}", e);
            return true;
        }
    };
    if ds < std::time::Duration::from_secs(3600 * 24 * 7) {
        eprintln!("It has been less than 7 days since last download");
        return false;
    }
    true
}

async fn download<P: AsRef<std::path::Path>>(url: &str, path: P) -> Result<(), GLErr> {
    println!("Downloading to: {:?}", path.as_ref());
    let res = reqwest::get(url).await?.bytes().await?;
    let mut f = File::create(path.as_ref())?;
    f.write_all(res.as_ref())?;
    Ok(())
}

async fn get_db_files<P: AsRef<std::path::Path>>(dbf_path: P) -> Result<(), GLErr> {
    let dbf_path = dbf_path.as_ref();
    std::fs::create_dir_all(dbf_path)?;
    let key = std::env::var("MAXMIND_KEY")?;
    // Countries
    let perma = format!(
        "https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-Country-CSV&license_key={}&suffix=zip",
        key,
    );
    let countriesf = dbf_path.join("countries.zip");
    download(perma.as_str(), &countriesf).await?;
    // unzip
    let f = File::open(&countriesf)?;
    let mut z = zip::ZipArchive::new(f)?;
    z.extract(&dbf_path.join("countries"))?;

    // Cities
    let perma = format!(
        "https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-City-CSV&license_key={}&suffix=zip",
        key,
    );
    let citiesf = dbf_path.join("cities.zip");
    download(perma.as_str(), &citiesf).await?;
    let f = File::open(&citiesf)?;
    let mut z = zip::ZipArchive::new(f)?;
    z.extract(&dbf_path.join("cities"))?;

    // ASN
    let perma = format!(
        "https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-ASN-CSV&license_key={}&suffix=zip",
        key,
    );
    let asnf = dbf_path.join("asn.zip");
    download(perma.as_str(), &asnf).await?;
    let f = File::open(&asnf)?;
    let mut z = zip::ZipArchive::new(f)?;
    z.extract(&dbf_path.join("asn"))?;

    // Unzip
    Ok(())
}

fn execute_query(db: String, q: &str, params: &[&dyn ToSql]) -> Result<(), GLErr> {
    let conn = rusqlite::Connection::open(&db)?;
    conn.execute(q, params)?;
    Ok(())
}

#[derive(Debug, Deserialize)]
struct Country {
    geoname_id: i64,
    continent_code: String,
    continent_name: String,
    country_iso_code: String,
    country_name: String,
    is_in_european_union: i64,
}

#[derive(Deserialize, Debug)]
struct CountryIPv4 {
    network: String,
    geoname_id: Option<i64>,
    registered_country_geoname_id: Option<i64>,
    is_anonymous_proxy: i64,
    is_satellite_provider: i64,
}

#[derive(Deserialize, Debug)]
struct CountryIPv6 {
    network: String,
    geoname_id: Option<i64>,
    registered_country_geoname_id: Option<i64>,
    is_anonymous_proxy: i64,
    is_satellite_provider: i64,
}

async fn countries_from_csv<P: AsRef<std::path::Path>>(
    dbfiles: P,
    db: String,
) -> Result<(), GLErr> {
    let cdir = dbfiles.as_ref().join(std::path::Path::new("countries"));
    let mut path = if let Some(v) = (std::fs::read_dir(&cdir)?).next() {
        match v {
            Ok(p) => p.path(),
            Err(e) => return Err(GLErr::IOErr(e)),
        }
    } else {
        return Err(GLErr::ZipExtractErr);
    };

    // Countries
    path.push("GeoLite2-Country-Locations-en.csv");
    let mut reader = csv::Reader::from_path(&path)?;
    println!("Populating Country Locations");
    let started = std::time::Instant::now();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Option<Country>>(10);
    let db1 = db.clone();
    // tokio::sync::Pin
    let jh = tokio::spawn(async move {
        let mut countries = Vec::<Country>::new();
        let mut count: usize = 0;
        let batch_size: usize = 5000;
        let db = db1;
        fn execute(cities: &[Country], db: String) {
            let mut values = Vec::<String>::new();
            let mut params = Vec::<&dyn rusqlite::ToSql>::new();
            let mut q = String::from(
                "INSERT INTO cities(
                    geoname_id, continent_code, continent_name, country_iso_code,
                    country_name, is_in_eu
            ) VALUES",
            );
            for c in cities.iter() {
                values.push(String::from("(?,?,?,?,?,?)"));
                params.push(&c.geoname_id);
                params.push(&c.continent_code);
                params.push(&c.continent_name);
                params.push(&c.country_iso_code);
                params.push(&c.country_name);
                params.push(&c.is_in_european_union);
            }
            q = format!("{} {}", q, values.join(","));
            match execute_query(db, &q, params.as_slice()) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("{:?}", e);
                }
            }
        }
        while let Some(country) = rx.recv().await {
            if count == batch_size {
                execute(&countries, db.clone());
                countries = Vec::<Country>::new();
                count = 0;
            }
            match country {
                Some(v) => {
                    countries.push(v);
                    count += 1;
                }
                None => {
                    execute(&countries, db.clone());
                    break;
                }
            };
        }
    });
    for result in reader.deserialize() {
        let c: Country = result?;
        let _ = tx.send(Some(c)).await;
    }
    let _ = tx.send(None).await;
    jh.await?;
    println!("Done!! Took: {:?}", started.elapsed());
    path.pop();

    // ipv4
    path.push("GeoLite2-Country-Blocks-IPv4.csv");
    let mut reader = csv::Reader::from_path(&path)?;
    println!("Populating Country Blocks IPv4");
    let started = std::time::Instant::now();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Option<CountryIPv4>>(10);
    let db1 = db.clone();
    // tokio::sync::Pin
    let jh = tokio::spawn(async move {
        let mut countries = Vec::<CountryIPv4>::new();
        let mut count: usize = 0;
        let batch_size: usize = 20000;
        let db = db1;
        fn execute(countries: &[CountryIPv4], db: String) {
            let mut values = Vec::<String>::new();
            let mut params = Vec::<&dyn rusqlite::ToSql>::new();
            let mut q = String::from(
                "INSERT INTO countries4(
                    network, geoname_id, registered_country_geoname_id, is_anonymous_proxy, is_satellite_provider
            ) VALUES",
            );
            for c in countries.iter() {
                values.push(String::from("(?,?,?,?,?)"));
                params.push(&c.network);
                params.push(&c.geoname_id);
                params.push(&c.registered_country_geoname_id);
                params.push(&c.is_anonymous_proxy);
                params.push(&c.is_satellite_provider);
            }
            q = format!("{} {}", q, values.join(","));
            match execute_query(db, &q, params.as_slice()) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("{:?}", e);
                }
            }
        }
        while let Some(country4) = rx.recv().await {
            if count == batch_size {
                execute(&countries, db.clone());
                countries = Vec::<CountryIPv4>::new();
                count = 0;
            }
            match country4 {
                Some(v) => {
                    countries.push(v);
                    count += 1;
                }
                None => {
                    execute(&countries, db.clone());
                    break;
                }
            };
        }
    });
    for result in reader.deserialize() {
        let c: CountryIPv4 = result?;
        let _ = tx.send(Some(c)).await;
    }
    let _ = tx.send(None).await;
    jh.await?;
    println!("Done!! Took: {:?}", started.elapsed());
    path.pop();

    // ipv6
    path.push("GeoLite2-Country-Blocks-IPv6.csv");
    let mut reader = csv::Reader::from_path(&path)?;
    println!("Populating Country Blocks IPv6");
    let started = std::time::Instant::now();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Option<CountryIPv6>>(10);
    let db1 = db.clone();
    // tokio::sync::Pin
    let jh = tokio::spawn(async move {
        let mut countries = Vec::<CountryIPv6>::new();
        let mut count: usize = 0;
        let batch_size: usize = 20000;
        let db = db1;
        fn execute(countries: &[CountryIPv6], db: String) {
            let mut values = Vec::<String>::new();
            let mut params = Vec::<&dyn rusqlite::ToSql>::new();
            let mut q = String::from(
                "INSERT INTO countries6(
                    network, geoname_id, registered_country_geoname_id, is_anonymous_proxy, is_satellite_provider
            ) VALUES",
            );
            for c in countries.iter() {
                values.push(String::from("(?,?,?,?,?)"));
                params.push(&c.network);
                params.push(&c.geoname_id);
                params.push(&c.registered_country_geoname_id);
                params.push(&c.is_anonymous_proxy);
                params.push(&c.is_satellite_provider);
            }
            q = format!("{} {}", q, values.join(","));
            match execute_query(db, &q, params.as_slice()) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("{:?}", e);
                }
            };
        }
        while let Some(country4) = rx.recv().await {
            if count == batch_size {
                execute(&countries, db.clone());
                countries = Vec::<CountryIPv6>::new();
                count = 0;
            }
            match country4 {
                Some(v) => {
                    countries.push(v);
                    count += 1;
                }
                None => {
                    execute(&countries, db.clone());
                    break;
                }
            };
        }
    });
    for result in reader.deserialize() {
        let c: CountryIPv6 = result?;
        let _ = tx.send(Some(c)).await;
    }
    let _ = tx.send(None).await;
    jh.await?;
    println!("Done!! Took: {:?}", started.elapsed());
    path.pop();
    Ok(())
}

#[derive(Debug, Deserialize)]
struct City {
    geoname_id: i64,
    continent_code: String,
    continent_name: String,
    country_iso_code: String,
    country_name: String,
    subdivision_1_iso_code: String,
    subdivision_1_name: String,
    subdivision_2_iso_code: String,
    subdivision_2_name: String,
    city_name: String,
    metro_code: String,
    time_zone: String,
    is_in_european_union: String,
}

#[derive(Debug, Deserialize)]
struct CityIPv4 {
    network: String,
    geoname_id: Option<i64>,
    registered_country_geoname_id: Option<i64>,
    is_anonymous_proxy: i64,
    is_satellite_provider: i64,
    postal_code: String,
    latitude: Option<f64>,
    longitude: Option<f64>,
    accuracy_radius: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct CityIPv6 {
    network: String,
    geoname_id: Option<i64>,
    registered_country_geoname_id: Option<i64>,
    is_anonymous_proxy: i64,
    is_satellite_provider: i64,
    postal_code: String,
    latitude: Option<f64>,
    longitude: Option<f64>,
    accuracy_radius: Option<i64>,
}

async fn cities_from_csv<P: AsRef<std::path::Path>>(dbfiles: P, db: String) -> Result<(), GLErr> {
    let cdir = dbfiles.as_ref().join(std::path::Path::new("cities"));
    let mut path = if let Some(v) = (std::fs::read_dir(&cdir)?).next() {
        match v {
            Ok(p) => p.path(),
            Err(e) => return Err(GLErr::IOErr(e)),
        }
    } else {
        return Err(GLErr::ZipExtractErr);
    };

    // City
    path.push("GeoLite2-City-Locations-en.csv");
    let mut reader = csv::Reader::from_path(&path)?;
    println!("Populating City Locations");
    let started = std::time::Instant::now();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Option<City>>(10);
    let db1 = db.clone();
    // tokio::sync::Pin
    let jh = tokio::spawn(async move {
        let mut cities = Vec::<City>::new();
        let mut count: usize = 0;
        let batch_size: usize = 5000;
        let db = db1;
        fn execute(cities: &[City], db: String) {
            let mut values = Vec::<String>::new();
            let mut params = Vec::<&dyn rusqlite::ToSql>::new();
            let mut q = String::from(
                "INSERT INTO cities(
                geoname_id, continent_code, continent_name, country_iso_code,
                country_name, subdivision_1_iso_code, subdivision_1_name,
                subdivision_2_iso_code, subdivision_2_name, city_name,
                metro_code, time_zone, is_in_eu
            ) VALUES",
            );
            for c in cities.iter() {
                values.push(String::from("(?,?,?,?,?,?,?,?,?,?,?,?,?)"));
                params.push(&c.geoname_id);
                params.push(&c.continent_code);
                params.push(&c.continent_name);
                params.push(&c.country_iso_code);
                params.push(&c.country_name);
                params.push(&c.subdivision_1_iso_code);
                params.push(&c.subdivision_1_name);
                params.push(&c.subdivision_2_iso_code);
                params.push(&c.subdivision_2_name);
                params.push(&c.city_name);
                params.push(&c.metro_code);
                params.push(&c.time_zone);
                params.push(&c.is_in_european_union);
            }
            q = format!("{} {}", q, values.join(","));
            match execute_query(db, &q, params.as_slice()) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("{:?}", e);
                }
            }
        }
        while let Some(city) = rx.recv().await {
            if count == batch_size {
                execute(&cities, db.clone());
                cities = Vec::<City>::new();
                count = 0;
            }
            match city {
                Some(v) => {
                    cities.push(v);
                    count += 1;
                }
                None => {
                    execute(&cities, db.clone());
                    break;
                }
            };
        }
    });
    for result in reader.deserialize() {
        let c: City = result?;
        let _ = tx.send(Some(c)).await;
    }
    let _ = tx.send(None).await;
    jh.await?;
    println!("Done!! Took: {:?}", started.elapsed());
    path.pop();

    // ipv4
    path.push("GeoLite2-City-Blocks-IPv4.csv");
    let mut reader = csv::Reader::from_path(&path)?;
    println!("Populating City Blocks IPv4");
    let started = std::time::Instant::now();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Option<CityIPv4>>(10);
    let db1 = db.clone();
    // tokio::sync::Pin
    let jh = tokio::spawn(async move {
        let mut cities = Vec::<CityIPv4>::new();
        let mut count: usize = 0;
        let batch_size: usize = 100000;
        let db = db1;
        fn execute(cities: &[CityIPv4], db: String) {
            let mut values = Vec::<String>::new();
            let mut params = Vec::<&dyn rusqlite::ToSql>::new();
            let mut q = String::from(
                "INSERT INTO cities4(
                    network, geoname_id, registered_country_geoname_id, is_anonymous_proxy, is_satellite_provider,
                    postal_code, latitude, longitude, accuracy_radius_km
            ) VALUES",
            );
            for c in cities.iter() {
                values.push(String::from("(?,?,?,?,?,?,?,?,?)"));
                params.push(&c.network);
                params.push(&c.geoname_id);
                params.push(&c.registered_country_geoname_id);
                params.push(&c.is_anonymous_proxy);
                params.push(&c.is_satellite_provider);
                params.push(&c.postal_code);
                params.push(&c.latitude);
                params.push(&c.longitude);
                params.push(&c.accuracy_radius);
            }
            q = format!("{} {}", q, values.join(","));
            match execute_query(db, &q, params.as_slice()) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("{:?}", e);
                }
            }
        }
        while let Some(city4) = rx.recv().await {
            if count == batch_size {
                execute(&cities, db.clone());
                cities = Vec::<CityIPv4>::new();
                count = 0;
            }
            match city4 {
                Some(v) => {
                    cities.push(v);
                    count += 1;
                }
                None => {
                    execute(&cities, db.clone());
                    break;
                }
            };
        }
    });
    for result in reader.deserialize() {
        let c: CityIPv4 = result?;
        let _ = tx.send(Some(c)).await;
    }
    let _ = tx.send(None).await;
    jh.await?;
    println!("Done!! Took: {:?}", started.elapsed());
    path.pop();

    // ipv6
    path.push("GeoLite2-City-Blocks-IPv6.csv");
    let mut reader = csv::Reader::from_path(&path)?;
    println!("Populating City Blocks IPv6");
    let started = std::time::Instant::now();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Option<CityIPv6>>(10);
    let db1 = db.clone();
    // tokio::sync::Pin
    let jh = tokio::spawn(async move {
        let mut cities = Vec::<CityIPv6>::new();
        let mut count: usize = 0;
        let batch_size: usize = 100000;
        let db = db1;
        fn execute(cities: &[CityIPv6], db: String) {
            let mut values = Vec::<String>::new();
            let mut params = Vec::<&dyn rusqlite::ToSql>::new();
            let mut q = String::from(
                "INSERT INTO cities6(
                    network, geoname_id, registered_country_geoname_id, is_anonymous_proxy, is_satellite_provider,
                    postal_code, latitude, longitude, accuracy_radius_km
            ) VALUES",
            );
            for c in cities.iter() {
                values.push(String::from("(?,?,?,?,?,?,?,?,?)"));
                params.push(&c.network);
                params.push(&c.geoname_id);
                params.push(&c.registered_country_geoname_id);
                params.push(&c.is_anonymous_proxy);
                params.push(&c.is_satellite_provider);
                params.push(&c.postal_code);
                params.push(&c.latitude);
                params.push(&c.longitude);
                params.push(&c.accuracy_radius);
            }
            q = format!("{} {}", q, values.join(","));
            match execute_query(db, &q, params.as_slice()) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("{:?}", e);
                }
            }
        }
        while let Some(city) = rx.recv().await {
            if count == batch_size {
                execute(&cities, db.clone());
                cities = Vec::<CityIPv6>::new();
                count = 0;
            }
            match city {
                Some(v) => {
                    cities.push(v);
                    count += 1;
                }
                None => {
                    execute(&cities, db.clone());
                    break;
                }
            };
        }
    });
    for result in reader.deserialize() {
        let c: CityIPv6 = result?;
        let _ = tx.send(Some(c)).await;
    }
    let _ = tx.send(None).await;
    jh.await?;
    println!("Done!! Took: {:?}", started.elapsed());
    path.pop();

    Ok(())
}

#[derive(Debug, Deserialize)]
struct ASN4 {
    network: String,
    autonomous_system_number: i64,
    autonomous_system_organization: String,
}

#[derive(Debug, Deserialize)]
struct ASN6 {
    network: String,
    autonomous_system_number: i64,
    autonomous_system_organization: String,
}

async fn asn_from_csv<P: AsRef<std::path::Path>>(dbfiles: P, db: String) -> Result<(), GLErr> {
    let cdir = dbfiles.as_ref().join(std::path::Path::new("asn"));
    let mut path = if let Some(v) = (std::fs::read_dir(&cdir)?).next() {
        match v {
            Ok(p) => p.path(),
            Err(e) => return Err(GLErr::IOErr(e)),
        }
    } else {
        return Err(GLErr::ZipExtractErr);
    };

    // ipv4
    path.push("GeoLite2-ASN-Blocks-IPv4.csv");
    let mut reader = csv::Reader::from_path(&path)?;
    println!("Populating ASN Blocks IPv4");
    let started = std::time::Instant::now();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Option<ASN4>>(10);
    let db1 = db.clone();
    // tokio::sync::Pin
    let jh = tokio::spawn(async move {
        let mut asns = Vec::<ASN4>::new();
        let mut count: usize = 0;
        let batch_size: usize = 100000;
        let db = db1;
        fn execute(asns: &[ASN4], db: String) {
            let mut values = Vec::<String>::new();
            let mut params = Vec::<&dyn rusqlite::ToSql>::new();
            let mut q = String::from(
                "INSERT INTO cities4(
                    network,autonomous_system_number,autonomous_system_org
            ) VALUES",
            );
            for a in asns.iter() {
                values.push(String::from("(?,?,?)"));
                params.push(&a.network);
                params.push(&a.autonomous_system_number);
                params.push(&a.autonomous_system_organization);
            }
            q = format!("{} {}", q, values.join(","));
            match execute_query(db, &q, params.as_slice()) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("{:?}", e);
                }
            }
        }
        while let Some(asn) = rx.recv().await {
            if count == batch_size {
                execute(&asns, db.clone());
                asns = Vec::<ASN4>::new();
                count = 0;
            }
            match asn {
                Some(v) => {
                    asns.push(v);
                    count += 1;
                }
                None => {
                    execute(&asns, db.clone());
                    break;
                }
            };
        }
    });
    for result in reader.deserialize() {
        let a: ASN4 = result?;
        let _ = tx.send(Some(a)).await;
    }
    let _ = tx.send(None).await;
    jh.await?;
    println!("Done!! Took: {:?}", started.elapsed());
    path.pop();

    // ipv6
    path.push("GeoLite2-ASN-Blocks-IPv6.csv");
    let mut reader = csv::Reader::from_path(&path)?;
    println!("Populating ASN Blocks IPv6");
    let started = std::time::Instant::now();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Option<ASN6>>(10);
    let db1 = db.clone();
    // tokio::sync::Pin
    let jh = tokio::spawn(async move {
        let mut asns = Vec::<ASN6>::new();
        let mut count: usize = 0;
        let batch_size: usize = 100000;
        let db = db1;
        fn execute(asns: &[ASN6], db: String) {
            let mut values = Vec::<String>::new();
            let mut params = Vec::<&dyn rusqlite::ToSql>::new();
            let mut q = String::from(
                "INSERT INTO cities4(
                    network,autonomous_system_number,autonomous_system_org
            ) VALUES",
            );
            for a in asns.iter() {
                values.push(String::from("(?,?,?)"));
                params.push(&a.network);
                params.push(&a.autonomous_system_number);
                params.push(&a.autonomous_system_organization);
            }
            q = format!("{} {}", q, values.join(","));
            match execute_query(db, &q, params.as_slice()) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("{:?}", e);
                }
            }
        }
        while let Some(asn) = rx.recv().await {
            if count == batch_size {
                execute(&asns, db.clone());
                asns = Vec::<ASN6>::new();
                count = 0;
            }
            match asn {
                Some(v) => {
                    asns.push(v);
                    count += 1;
                }
                None => {
                    execute(&asns, db.clone());
                    break;
                }
            };
        }
    });
    for result in reader.deserialize() {
        let a: ASN6 = result?;
        let _ = tx.send(Some(a)).await;
    }
    let _ = tx.send(None).await;
    jh.await?;
    println!("Done!! Took: {:?}", started.elapsed());
    path.pop();

    Ok(())
}

async fn new_db<P: AsRef<std::path::Path>>(db_dir: P) -> Result<(), GLErr> {
    let db_dir = db_dir.as_ref();
    let version_file = db_dir.join("version.new");
    let mut f = File::create(version_file)?;
    f.write_all(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs()
            .to_string()
            .as_bytes(),
    )?;

    let dbfiles = db_dir.join(std::path::Path::new("dbfiles"));
    get_db_files(&dbfiles).await?;

    let dbfile = db_dir.join("geolite2.db.new");
    let conn = rusqlite::Connection::open(&dbfile)?;
    let db = if let Ok(s) = dbfile.into_os_string().into_string() {
        s
    } else {
        return Err(GLErr::OSStringErr);
    };
    build_tables(&conn)?;
    countries_from_csv(&dbfiles, db.clone()).await?;
    cities_from_csv(&dbfiles, db.clone()).await?;
    asn_from_csv(&dbfiles, db).await?;
    Ok(())
}

pub async fn update_db() -> Result<(), GLErr> {
    // Update new version
    let dbd = std::env::var("GL2_DBDIR")?;
    let db_dir = std::path::Path::new(&dbd);
    if !update_needed(db_dir) {
        eprintln!("No update needed");
        return Ok(());
    }
    new_db(db_dir).await?;

    // Update file and version names
    let mut del_old = false;
    if db_dir.join("version").exists() {
        std::fs::rename(db_dir.join("version"), db_dir.join("version.old"))?;
        del_old = true;
    }
    std::fs::rename(db_dir.join("version.new"), db_dir.join("version"))?;
    if del_old {
        std::fs::remove_file(db_dir.join("version.old"))?;
    }
    let mut del_old = false;
    if db_dir.join("geolite2.db").exists() {
        std::fs::rename(db_dir.join("geolite2.db"), db_dir.join("geolite2.db.old"))?;
        del_old = true
    }
    std::fs::rename(db_dir.join("geolite2.db.new"), db_dir.join("geolite2.db"))?;
    if del_old {
        std::fs::remove_file(db_dir.join("geolite2.db.old"))?;
    }
    if db_dir.join("dbfiles").exists() {
        std::fs::remove_dir_all(db_dir.join("dbfiles"))?;
    }
    Ok(())
}

pub fn phone_codes() -> Result<HashMap<String, String>, GLErr> {
    let var = std::env::var("PHONE_JSON_FILE")?;
    let json_string = std::fs::read_to_string(&var)?;
    let h: HashMap<String, String> = serde_json::from_str(&json_string)?;
    Ok(h)
}
