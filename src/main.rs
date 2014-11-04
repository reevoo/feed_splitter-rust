extern crate csv;
extern crate serialize;

use std::path::Path;
use std::os;

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
    let mut sorted_record = vec!();

    println!("Loading...");
    for record in reader.records() {
        let record = record.unwrap();
        sorted_record.push(record);
    }

    println!("Sorting...");
    sorted_record.sort_by(|rec1, rec2| rec1[split_record_index].cmp(&rec2[split_record_index]));
    println!("Sorted, writing...");

    let mut current_file_records = 0u;
    let mut file_number = 0u;
    let mut last_split_field_value = "".to_string();
    let mut writer = csv::Writer::from_file(&Path::new(format!("{}-p{}.csv", csv_file_name, file_number))).delimiter(b'|');
    writer.encode(headers.clone()).unwrap();

    for record in sorted_record.iter() {
        if (current_file_records >= RECORDS_PER_FILE) && (last_split_field_value != record[split_record_index]) {
            file_number += 1;
            current_file_records = 0;
            writer = csv::Writer::from_file(&Path::new(format!("{}-p{}.csv", csv_file_name, file_number))).delimiter(b'|');
            writer.encode(headers.clone()).unwrap();
        }
        last_split_field_value = record[split_record_index].clone();
        writer.encode(record).unwrap();
        current_file_records += 1;
    }
    println!("Total records: {}. Files created: {}.", sorted_record.len(), file_number+1);
}
