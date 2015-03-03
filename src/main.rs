#![feature(old_io)]
#![feature(old_path)]

extern crate csv;
extern crate getopts;
#[macro_use]
extern crate log;


use std::old_path::Path;
use std::old_io::File;
use getopts::Options;
use std::ascii::AsciiExt;

const RECORDS_PER_FILE: usize = 1000;
const DELIMETERS: [u8; 3] = [b'|', b';', b'\t'];

#[derive(Debug)]
struct Stats {
    total_records: usize,
    number_of_files: usize,
}

enum SplitByField {
    FieldName(String),
    FieldIndex(usize)
}

// Simple algorithm, copying what we have in ruby right now.
fn detect_delimiter(data: &[u8]) -> u8 {
    let mut delimiters_frequency : Vec<(u8, usize)> = DELIMETERS.iter().map(|&del| (del, data.iter().filter(|&&x| x == del).count())).collect();
    delimiters_frequency.sort_by(|&(_, count), &(_, count2)| count2.cmp(&count));
    if delimiters_frequency[0].1 > 2 {
        delimiters_frequency[0].0
    }else{
        b','
    }
}

fn detect_csv_file_delimiter(path: &Path) -> u8{
    let mut f = File::open(path).ok().expect("Can't open file");
    let mut buf : [u8; 100] = [0; 100];
    let read_bytes = f.read_at_least(100, &mut buf).unwrap(); //trying to read 100 bytes to the buf.
    let delimiter = detect_delimiter(&buf[0 .. read_bytes]); //slicing the actual number of bytes read.
    info!("Detected delimiter: 0x{:x}", delimiter);
    delimiter
}

fn split_records<T: Clone + Ord>(mut records: Vec<Vec<T>>, records_per_file: usize, split_record_index: usize) -> Vec<Vec<Vec<T>>> {
    let mut splitted_records = vec!();
    let mut current_vec = vec!();
    let mut current_file_records = 0usize;
    let mut last_split_field_value = None;
    info!("Sorting...");
    records.sort_by(|rec1, rec2| rec1[split_record_index].cmp(&rec2[split_record_index]));
    info!("Splitting...");
    for record in records.into_iter() {
        if (current_file_records >= records_per_file) && (last_split_field_value != Some(record[split_record_index].clone())) {
            splitted_records.push(current_vec);
            current_file_records = 0;
            current_vec = vec!();
        }
        last_split_field_value = Some(record[split_record_index].clone());
        current_vec.push(record);
        current_file_records += 1;
    }
    splitted_records.push(current_vec);
    splitted_records
}

fn split_file(csv_file_path: &Path, split_by_field: SplitByField, records_per_file: usize, delimiter: u8) -> Stats {
    let csv_file_name = csv_file_path.as_str().unwrap();
    let mut reader = csv::Reader::from_file(csv_file_path).delimiter(delimiter);

    let mut headers = None;
    let split_record_index = match split_by_field {
        SplitByField::FieldName(field_name) => {
            let lower_field_name = field_name.to_ascii_uppercase();
            let headers_tmp = reader.headers().ok().expect("Can't read headers");
            let pos =  headers_tmp.iter().position(|header| header.to_ascii_uppercase() == lower_field_name).expect(&*format!("Can't find header '{}' in the file", field_name));
            headers  = Some(headers_tmp);
            pos
        },
        SplitByField::FieldIndex(idx) => {
            reader = reader.has_headers(false);
            idx
        }
    };

    info!("Loading...");
    let records : Vec<Vec<_>> = reader.byte_records().map(|record| record.unwrap()).collect();
    let total_records = records.len();

    let splitted_records = split_records(records, records_per_file, split_record_index);

    info!("Writing...");
    let mut file_number = 0usize;
    for records_set in splitted_records.into_iter() {
        let mut writer = csv::Writer::from_file(&Path::new(format!("{}-p{}.csv", csv_file_name, file_number))).delimiter(delimiter);
        if headers.is_some(){
            writer.encode(headers.clone()).unwrap();
        }
        for record in records_set.into_iter() {
            writer.write(record.into_iter()).unwrap();
        }
        file_number += 1;
    }
    Stats { total_records: total_records, number_of_files: file_number+1 }
}

fn print_usage(opts: &Options){
    println!("{}", opts.usage("feed_splitter-rust [OPTIONS] CSV_FILE"));
}

#[allow(dead_code)]
fn main() {
    let args : Vec<_> = std::env::args().collect();

    let mut opts = Options::new();
    opts.optopt("i", "index", "use column index", "INDEX");
    opts.optopt("c", "column", "use column name", "NAME");
    opts.optopt("f", "file", "csv file", "FILE");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => {
            print_usage(&opts);
            panic!(f.to_string())
        }
    };

    let csv_file_name = matches.opt_str("f").unwrap();

    let split_field = match (matches.opt_present("i"), matches.opt_present("c")) {
        (true, true) | (false, false) => {
            print_usage(&opts);
            panic!("Please provide either -c or -i");
        },
        (true, _) => {
            SplitByField::FieldIndex(matches.opt_str("i").unwrap().parse().unwrap())
        },
        (_, true) => {
            SplitByField::FieldName(matches.opt_str("c").unwrap())
        }
    };

    let csv_file_path = &Path::new(csv_file_name);
    let delimiter = detect_csv_file_delimiter(csv_file_path);
    let stats = split_file(csv_file_path, split_field, RECORDS_PER_FILE, delimiter);
    info!("{:?}", stats);
}

#[cfg(test)]
mod test{
    use std::old_io::fs::{File, mkdir, rmdir_recursive};
    use std::old_io;
    use std::old_io::fs::PathExtensions;

    #[test]
    fn test_detect_delimiter(){
        assert_eq!(::detect_delimiter(b"a,b,c\nb,c,e"), b',');
        assert_eq!(::detect_delimiter(b"a|b|c\nb|c|e"), b'|');
        assert_eq!(::detect_delimiter(b"a\tb\tc\nb\tc\te"), b'\t');
        assert_eq!(::detect_delimiter(b"a;b;c\nb;c;b"), b';');
    }

    #[test]
    fn test_split_records(){
        let records = vec!(
            vec!("a", "b", "c"),
            vec!("a25", "b3", "c25"),
            vec!("a26", "b3", "c26"),
            vec!("a1", "b5", "c1"),
            vec!("a2", "b6", "c2"),
            vec!("a3", "b7", "c3"),
            vec!("a4", "b5", "c4"),
            vec!("a5", "b3", "c5"),
            vec!("a6", "b6", "c6"),
            vec!("a7", "b1", "c7"),
            vec!("a8", "b2", "c8"),
            vec!("a9", "b3", "c9")
        );
        assert_eq!(::split_records(records, 3, 1),
            vec!(
                vec!(
                    vec!("a", "b", "c"),
                    vec!("a7", "b1", "c7"),
                    vec!("a8", "b2", "c8")
                ),
                vec!(
                    vec!("a25", "b3", "c25"),
                    vec!("a26", "b3", "c26"),
                    vec!("a5", "b3", "c5"),
                    vec!("a9", "b3", "c9")
                ),
                vec!(
                    vec!("a1", "b5", "c1"),
                    vec!("a4", "b5", "c4"),
                    vec!("a2", "b6", "c2"),
                    vec!("a6", "b6", "c6")
                ),
                vec!(
                    vec!("a3", "b7", "c3")
                )
            )
        );
    }

    #[test]
    fn test_split_file(){
        let data = "f1|f2|f3\n\
                    a|b|c\n\
                    a25|b3|c25\n\
                    a26|b3|c26\n\
                    a1|b5|c1\n\
                    a2|b6|c2\n\
                    a3|b7|c3\n\
                    a4|b5|c4\n\
                    a5|b3|c5\n\
                    a6|b6|c6\n\
                    a7|b1|c7\n\
                    a8|b2|c8\n\
                    a9|b3|c9\n";
        rmdir_recursive(&Path::new("tmp_test")).unwrap_or(());
        mkdir(&Path::new("tmp_test"), old_io::USER_RWX).unwrap();
        let tmp_csv = Path::new("tmp_test/tmp_test.csv");
        {
            let mut f = File::create(&tmp_csv);
            f.write_str(data).unwrap();
        }
        let delimiter = ::detect_csv_file_delimiter(&tmp_csv);
        assert_eq!(delimiter, b'|');
        ::split_file(&tmp_csv, ::SplitByField::FieldName("f2".to_string()), 2, delimiter);
        assert!(Path::new("tmp_test/tmp_test.csv-p0.csv").exists());
        assert!(Path::new("tmp_test/tmp_test.csv-p1.csv").exists());
        assert!(Path::new("tmp_test/tmp_test.csv-p2.csv").exists());
        assert!(Path::new("tmp_test/tmp_test.csv-p3.csv").exists());
        assert!(Path::new("tmp_test/tmp_test.csv-p4.csv").exists());
        assert_eq!(File::open(&Path::new("tmp_test/tmp_test.csv-p0.csv")).read_to_string().unwrap(), "f1|f2|f3\na|b|c\na7|b1|c7\n");
        assert_eq!(File::open(&Path::new("tmp_test/tmp_test.csv-p1.csv")).read_to_string().unwrap(), "f1|f2|f3\na8|b2|c8\na25|b3|c25\na26|b3|c26\na5|b3|c5\na9|b3|c9\n");
        assert_eq!(File::open(&Path::new("tmp_test/tmp_test.csv-p2.csv")).read_to_string().unwrap(), "f1|f2|f3\na1|b5|c1\na4|b5|c4\n");
        assert_eq!(File::open(&Path::new("tmp_test/tmp_test.csv-p3.csv")).read_to_string().unwrap(), "f1|f2|f3\na2|b6|c2\na6|b6|c6\n");
        assert_eq!(File::open(&Path::new("tmp_test/tmp_test.csv-p4.csv")).read_to_string().unwrap(), "f1|f2|f3\na3|b7|c3\n");
        rmdir_recursive(&Path::new("tmp_test")).unwrap();
    }
}
