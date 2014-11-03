extern crate csv;
extern crate serialize;

use std::path::Path;
use std::os;
use std::collections::TreeMap;

const SPLIT_BY_FIELD : &'static str = "Email";
const RECORDS_PER_FILE: uint = 1000;

fn main() {
    let args = os::args();
    if args.len() < 2{
        panic!("usage: {} CSV_FILE_PATH", args[0]);
    }
    let csv_file_name = os::args()[1].clone();
    let fp = &Path::new(csv_file_name.clone());
    let mut reader = csv::Reader::from_file(fp).delimiter(b'|');

    let headers = reader.headers().unwrap();
    let split_record_index = headers.iter().position(|header| header.as_slice() == SPLIT_BY_FIELD).expect("Can't find split_by_field field");
    let mut sorted_record : TreeMap<String, Vec<Vec<String>>> = TreeMap::new();

    println!("Loading...");
    for record in reader.records() {
        let record = record.unwrap();
        let split_field_value = record[split_record_index].clone();
        if sorted_record.contains_key(&split_field_value){
            sorted_record.index_mut(&split_field_value).push(record);
        }else{
            sorted_record.insert(split_field_value, vec!(record));
        }
    }

    println!("Writing...");
    let mut current_file_records = 0u;
    let mut file_number = 0u;
    let mut records_count = 0u;
    let mut writer = csv::Writer::from_file(&Path::new(format!("{}-p{}.csv", csv_file_name, file_number))).delimiter(b'|');
    writer.encode(headers).unwrap();

    for record_set in sorted_record.values() {
        if current_file_records >= RECORDS_PER_FILE {
            file_number += 1;
            current_file_records = 0;
            writer = csv::Writer::from_file(&Path::new(format!("{}-p{}.csv", csv_file_name, file_number))).delimiter(b'|');
        }
        for record in record_set.iter(){
            writer.encode(record).unwrap();
            current_file_records += 1;
            records_count += 1;
        }
    }
    println!("Total records: {}. Total uniq emails: {}. Files created: {}", records_count, sorted_record.len(), file_number+1);
}
