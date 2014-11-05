#![feature(phase)]

extern crate csv;
extern crate serialize;
#[phase(plugin, link)] extern crate log;


use std::path::Path;
use std::os;

const SPLIT_BY_FIELD : &'static str = "Email";
const RECORDS_PER_FILE: uint = 1000;

#[deriving(Show)]
struct Stats {
    total_records: uint,
    number_of_files: uint,
}

fn split_file(csv_file_path: &Path, split_by_field: &str, records_per_file: uint) -> Stats {
    let csv_file_name = csv_file_path.as_str().unwrap();
    let mut reader = csv::Reader::from_file(csv_file_path).delimiter(b'|');

    let headers = reader.headers().unwrap();
    let split_record_index = headers.iter().position(|header| header.as_slice() == split_by_field).expect("Can't find split_by_field field");

    info!("Loading...");
    let mut sorted_records : Vec<Vec<_>> = reader.byte_records().map(|record| record.unwrap()).collect();

    info!("Sorting...");
    sorted_records.sort_by(|rec1, rec2| rec1[split_record_index].cmp(&rec2[split_record_index]));
    info!("Sorted, writing...");

    let mut current_file_records = 0u;
    let mut file_number = 0u;
    let mut last_split_field_value = None;
    let mut writer = csv::Writer::from_file(&Path::new(format!("{}-p{}.csv", csv_file_name, file_number))).delimiter(b'|');
    writer.encode(headers.clone()).unwrap();

    for record in sorted_records.iter() {
        if (current_file_records >= records_per_file) && (last_split_field_value != Some(record[split_record_index].clone())) {
            file_number += 1;
            current_file_records = 0;
            writer = csv::Writer::from_file(&Path::new(format!("{}-p{}.csv", csv_file_name, file_number))).delimiter(b'|');
            writer.encode(headers.clone()).unwrap();
        }
        last_split_field_value = Some(record[split_record_index].clone());
        writer.write_bytes(record.clone().into_iter()).unwrap();
        current_file_records += 1;
    }
    Stats { total_records: sorted_records.len(), number_of_files:  file_number+1 }
}

fn main() {
    let args = os::args();
    if args.len() < 2{
        panic!("usage: {} CSV_FILE_PATH", args[0]);
    }
    let csv_file_name = os::args()[1].clone();
    let stats = split_file(&Path::new(csv_file_name), SPLIT_BY_FIELD, RECORDS_PER_FILE);
    info!("{}", stats);
}
