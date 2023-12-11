use pyo3::prelude::*;

use odbc_api::{
    buffers::TextRowSet, 
    Cursor, 
    Environment, 
    ConnectionOptions, 
    ResultSetMetadata
};
use lazy_static::lazy_static;
use uuid::Uuid;
use std::error::Error;
use std::fmt;

use csv::StringRecord;
use serde::{Deserialize, Serialize};

// https://doc.rust-lang.org/rust-by-example/error/multiple_error_types/define_error_type.html
#[derive(Debug)]
struct CustomError(String);
impl fmt::Display for CustomError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Error: {}", self.0)
    }
}
impl Error for CustomError {}

pub struct Params {
    pub conn: String,
    pub query: String,
    pub bach_size: Option<usize>,
    pub fname: Option<String>
}
impl Params {
    pub fn new(args: &[String]) -> Result<Params, &str> {
        if args.len() < 2 {
            return Err("Not enough arguments!")
        }
        let conn = args[0].clone();
        let query = args[1].clone();
        let bach_size = match args.get(2) {
            Some(batch) => {
                match batch.parse() {
                    Ok(b) => Some(b),
                    Err(_) => None
                }
            },
            None => None
        };
        let fname = match args.get(3) {
            Some(fname) => Some(fname.clone()),
            None => None
        };
        Ok(Params {conn, query, bach_size, fname})
    }
}

fn connection(odbc_conn_string: &str) -> Result<odbc_api::Connection<'_>, odbc_api::Error> {
    lazy_static! {
        // static ref ENV: Environment = unsafe { Environment::new().unwrap() };
        static ref ENV: Environment = Environment::new().unwrap();
    }
    let conn_str = odbc_conn_string;
    let conn = ENV.connect_with_connection_string(
        conn_str,
        ConnectionOptions::default()
    )?;
    Ok(conn)
}

pub fn run(params: Params) -> Result<String, Box<dyn Error>> {  // Result<String, Box<dyn std::error::Error>>  
    // CREATING THE ODBC CONNECTION 
    let conn = connection(&params.conn)?;
    let query = params.query;
    //Maximum number of rows fetched with one row set. Fetching batches of rows is usually much
    //faster than fetching individual rows.
    let bach_size = match params.bach_size {
        Some(batch) => batch,
        None => 5_000
    };
    // println!("bach_size: {}", bach_size);
    // Execute a one of query without any parameters.    
    // let file_id: String = Uuid::new_v4().hyphenated().encode_lower(&mut Uuid::encode_buffer()).to_owned();
    let file_id = match params.fname {
        Some(fname) => fname,
        None => Uuid::new_v4().hyphenated().encode_lower(&mut Uuid::encode_buffer()).to_owned()
    };
    let temp_dir = std::env::temp_dir().display().to_string();
    let fname = format!("{temp_dir}/{file_id}.csv");
    match conn.execute(&query, ())? {
        Some(mut cursor) => {
            let mut writer = csv::Writer::from_path(&fname)?;
            // Write the column names to stdout
            let headline : Vec<String> = cursor.column_names()?.collect::<Result<_,_>>()?;
            writer.write_record(headline)?;
            // Use schema in cursor to initialize a text buffer large enough to hold the largest
            // possible strings for each column up to an upper limit of 4KiB.
            let mut buffers = TextRowSet::for_cursor(bach_size, &mut cursor, Some(4096))?;
            // Bind the buffer to the cursor. It is now being filled with every call to fetch.
            let mut row_set_cursor = cursor.bind_buffer(&mut buffers)?;
            // Iterate over batches
            while let Some(batch) = row_set_cursor.fetch()? {
                // Within a batch, iterate over every row
                for row_index in 0..batch.num_rows() {
                    // Within a row iterate over every column
                    let record = (0..batch.num_cols()).map(|col_index| {
                        batch
                            .at(col_index, row_index)
                            .unwrap_or(&[])
                    });
                    // let rec: Vec<_> = record.clone().collect();
                    // let rec: Vec<String> = record.clone().map(|x| String::from_utf8_lossy(x));
                    // println!("{:?}", rec);
                    // IN CASE WE HAVE INVALID UTF-8 BYTES IN OUR DATASET
                    let record = StringRecord::from_byte_record_lossy(record.collect());
                    // Writes row as csv
                    writer.write_record(record.as_byte_record())?;
                    //writer.write_record(csv::StringRecord(&record))?;
                }
            }
            writer.flush()?;
        }
        None => {
            return Err(Box::new(CustomError("Query came back empty. No output has been created.".into())))
        }
    }
    Ok(fname)
}


#[derive(Serialize, Deserialize)]
struct Response {
    success: bool,
    msg: String,
    fname: Option<String>
}
// --conn "Driver={iSeries Access ODBC Driver};System=172.21.11.1;Uid=USR;Pwd=PWD"
// --query "SELECT * FROM \"BIAPRT\".\"GGR12\""
//  rust-odbc-csv "Driver={iSeries Access ODBC Driver};System=172.21.11.1;Uid=USR;Pwd=PWD" "SELECT * FROM \"BIAPRT\".\"GGR12\""
#[pyfunction]
fn odbc_csv(args: Vec<String>) -> String {
    // COLLECTING THE ARGUMENTS
    //let args: Vec<String> = env::args().collect();
    // PARSING THE ARGUMENTS
    let params = match Params::new(&args) {
        Ok(params) => params,
        Err(err) => {
            let msg = Response {
                success: false,
                msg: format!("Problem parsing argumants: {}", err),
                fname: None
            };
            let j = serde_json::to_string(&msg).unwrap();
            return j.to_string()
            // process::exit(0);
        }
    };
    let fname = match run(params) {
        Ok(_file) => _file,
        Err(err) => {
            let msg = Response {
                success: false,
                msg: format!("Error: {}", err),
                fname: None
            };
            let json_msg = serde_json::to_string(&msg).unwrap();
            return json_msg.to_string()
            // process::exit(0);
        }
    };
    let msg = Response {
        success: true,
        msg: format!("Success: {fname}"),
        fname: Some(fname)
    };
    let json_msg = serde_json::to_string(&msg).unwrap();
    return json_msg.to_string()
}

/// A Python module implemented in Rust.
#[pymodule]
fn py_rust_odbc_csv(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(odbc_csv, m)?)?;
    Ok(())
}