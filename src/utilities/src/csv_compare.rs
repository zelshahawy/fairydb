use csv::StringRecord;
use std::collections::hash_map::DefaultHasher;
use std::collections::BinaryHeap;
use std::hash::{Hash, Hasher};

/// Compares two CSV files for equality considering order.
///
/// # Returns
///
/// A `csv::Result<bool>` that is `Ok(true)` if the CSV files are equal,
/// and `Ok(false)` or `Err(_)` otherwise.
pub fn csvs_equal_ordered<R1, R2>(
    mut rdr1: csv::Reader<R1>,
    mut rdr2: csv::Reader<R2>,
    num_fields: Option<usize>,
) -> csv::Result<bool>
where
    R1: std::io::Read,
    R2: std::io::Read,
{
    let mut iter1 = rdr1.records();
    let mut iter2 = rdr2.records();

    loop {
        let rec1 = iter1.next();
        let rec2 = iter2.next();

        match (rec1, rec2) {
            (None, None) => break,
            (Some(rec1), Some(rec2)) => {
                if let Some(num_fields) = num_fields {
                    let rec1_parsed = rec1?.iter().take(num_fields).collect::<StringRecord>();

                    let rec2_parsed = rec2?.iter().take(num_fields).collect::<StringRecord>();

                    if rec1_parsed != rec2_parsed {
                        return Ok(false);
                    }
                } else if rec1? != rec2? {
                    return Ok(false);
                }
            }
            _ => return Ok(false),
        }
    }

    Ok(true)
}

/// Compares two CSV files for equality without considering order.
/// # Returns
///
/// A `csv::Result<bool>` that is `Ok(true)` if the CSV files are equal,
/// and `Ok(false)` or `Err(_)` otherwise.
pub fn csvs_equal_unordered<R1, R2>(
    mut rdr1: csv::Reader<R1>,
    mut rdr2: csv::Reader<R2>,
    num_fields: Option<usize>,
) -> csv::Result<bool>
where
    R1: std::io::Read,
    R2: std::io::Read,
{
    let mut heap1 = BinaryHeap::new();
    for rec in rdr1.records() {
        let mut hasher1 = DefaultHasher::new();
        let rec = rec?;
        if let Some(num_fields) = num_fields {
            rec.iter()
                .take(num_fields)
                .collect::<Vec<_>>()
                .hash(&mut hasher1);
        } else {
            rec.iter().collect::<Vec<_>>().hash(&mut hasher1);
        }
        heap1.push(hasher1.finish());
    }

    let mut heap2 = BinaryHeap::new();
    for rec in rdr2.records() {
        let mut hasher2 = DefaultHasher::new();
        let rec = rec?;
        if let Some(num_fields) = num_fields {
            rec.iter()
                .take(num_fields)
                .collect::<Vec<_>>()
                .hash(&mut hasher2);
        } else {
            rec.iter().collect::<Vec<_>>().hash(&mut hasher2);
        }
        heap2.push(hasher2.finish());
    }

    let mut final_hasher1 = DefaultHasher::new();
    while let Some(hash) = heap1.pop() {
        hash.hash(&mut final_hasher1);
    }
    let hash1 = final_hasher1.finish();

    let mut final_hasher2 = DefaultHasher::new();
    while let Some(hash) = heap2.pop() {
        hash.hash(&mut final_hasher2);
    }
    let hash2 = final_hasher2.finish();

    Ok(hash1 == hash2)
}

/// Test if one CSV file is a subset of another CSV file.
///
/// # Arguments
///
/// * `rdr1` - A CSV reader for the first CSV file.
/// * `rdr2` - A CSV reader for the second CSV file.
/// * `num_fields` - The number of fields to consider when comparing records.
///
///
/// # Returns
///
/// Check if the first CSV file is a subset of the second CSV file.
/// If the first CSV file is a subset of the second CSV file, then return `Ok(true)`.
/// If the first CSV file is not a subset of the second CSV file, then return `Ok(false)`.
/// If there is an error, then return `Err(_)`.
///
pub fn csvs_subset<R1, R2>(
    mut rdr1: csv::Reader<R1>,
    mut rdr2: csv::Reader<R2>,
    num_fields: Option<usize>,
) -> csv::Result<bool>
where
    R1: std::io::Read,
    R2: std::io::Read,
{
    let mut heap1 = BinaryHeap::new();
    for rec in rdr1.records() {
        let mut hasher1 = DefaultHasher::new();
        if let Some(num_fields) = num_fields {
            rec?.iter()
                .take(num_fields)
                .collect::<Vec<_>>()
                .hash(&mut hasher1);
        } else {
            rec?.iter().collect::<Vec<_>>().hash(&mut hasher1);
        }
        heap1.push(hasher1.finish());
    }

    let mut heap2 = BinaryHeap::new();
    for rec in rdr2.records() {
        let mut hasher2 = DefaultHasher::new();
        if let Some(num_fields) = num_fields {
            rec?.iter()
                .take(num_fields)
                .collect::<Vec<_>>()
                .hash(&mut hasher2);
        } else {
            rec?.iter().collect::<Vec<_>>().hash(&mut hasher2);
        }
        heap2.push(hasher2.finish());
    }

    while let Some(hash1) = heap1.pop() {
        loop {
            match heap2.pop() {
                Some(hash2) => {
                    if hash1 == hash2 {
                        break;
                    }
                }
                None => return Ok(false),
            }
        }
    }

    Ok(true)
}

#[cfg(test)]
mod test {
    use super::*;
    use csv::ReaderBuilder;

    #[test]
    fn test_csvs_equal_ordered() {
        let data1 = "name,age,city\nAlice,30,New York\nBob,40,San Francisco\n";
        let data2 = "name;age;city\nAlice;30;New York\nBob;40;San Francisco\n";

        let rdr1 = ReaderBuilder::new()
            .delimiter(b',')
            .from_reader(data1.as_bytes());

        let rdr2 = ReaderBuilder::new()
            .delimiter(b';')
            .from_reader(data2.as_bytes());

        assert!(csvs_equal_ordered(rdr1, rdr2, None).unwrap());

        let rdr1 = ReaderBuilder::new()
            .delimiter(b',')
            .from_reader(data1.as_bytes());

        let rdr2 = ReaderBuilder::new()
            .delimiter(b';')
            .from_reader(data2.as_bytes());

        assert!(csvs_equal_unordered(rdr1, rdr2, None).unwrap());

        let rdr1 = ReaderBuilder::new()
            .delimiter(b',')
            .from_reader(data1.as_bytes());

        let rdr2 = ReaderBuilder::new()
            .delimiter(b';')
            .from_reader(data2.as_bytes());

        assert!(csvs_subset(rdr1, rdr2, None).unwrap());

        let rdr1 = ReaderBuilder::new()
            .delimiter(b',')
            .from_reader(data1.as_bytes());

        let rdr2 = ReaderBuilder::new()
            .delimiter(b';')
            .from_reader(data2.as_bytes());

        assert!(csvs_subset(rdr2, rdr1, None).unwrap());
    }

    #[test]
    fn test_csvs_equal_unordered() {
        let data1 = "name,age,city\nAlice,30,New York\nBob,40,San Francisco\n";
        let data2 = "name;age;city\nBob;40;San Francisco\nAlice;30;New York\n";

        let rdr1 = ReaderBuilder::new()
            .delimiter(b',')
            .from_reader(data1.as_bytes());

        let rdr2 = ReaderBuilder::new()
            .delimiter(b';')
            .from_reader(data2.as_bytes());

        assert!(!csvs_equal_ordered(rdr1, rdr2, None).unwrap()); // Should be false

        let rdr1 = ReaderBuilder::new()
            .delimiter(b',')
            .from_reader(data1.as_bytes());
        let rdr2 = ReaderBuilder::new()
            .delimiter(b';')
            .from_reader(data2.as_bytes());

        assert!(csvs_equal_unordered(rdr1, rdr2, None).unwrap());

        let rdr1 = ReaderBuilder::new()
            .delimiter(b',')
            .from_reader(data1.as_bytes());
        let rdr2 = ReaderBuilder::new()
            .delimiter(b';')
            .from_reader(data2.as_bytes());

        assert!(csvs_subset(rdr1, rdr2, None).unwrap());

        let rdr1 = ReaderBuilder::new()
            .delimiter(b',')
            .from_reader(data1.as_bytes());
        let rdr2 = ReaderBuilder::new()
            .delimiter(b';')
            .from_reader(data2.as_bytes());

        assert!(csvs_subset(rdr2, rdr1, None).unwrap());
    }

    #[test]
    fn test_csvs_subset() {
        let data1 = "name,age,city\nAlice,30,New York\nBob,40,San Francisco\n";
        let data2 = "name;age;city\nBob;40;San Francisco\n";

        let rdr1 = ReaderBuilder::new()
            .delimiter(b',')
            .from_reader(data1.as_bytes());
        let rdr2 = ReaderBuilder::new()
            .delimiter(b';')
            .from_reader(data2.as_bytes());

        assert!(!csvs_equal_ordered(rdr1, rdr2, None).unwrap()); // Should be false

        let rdr1 = ReaderBuilder::new()
            .delimiter(b',')
            .from_reader(data1.as_bytes());
        let rdr2 = ReaderBuilder::new()
            .delimiter(b';')
            .from_reader(data2.as_bytes());

        assert!(!csvs_equal_unordered(rdr1, rdr2, None).unwrap()); // Should be false

        let rdr1 = ReaderBuilder::new()
            .delimiter(b',')
            .from_reader(data1.as_bytes());
        let rdr2 = ReaderBuilder::new()
            .delimiter(b';')
            .from_reader(data2.as_bytes());

        assert!(!csvs_subset(rdr1, rdr2, None).unwrap()); // Should be false

        let rdr1 = ReaderBuilder::new()
            .delimiter(b',')
            .from_reader(data1.as_bytes());
        let rdr2 = ReaderBuilder::new()
            .delimiter(b';')
            .from_reader(data2.as_bytes());

        assert!(csvs_subset(rdr2, rdr1, None).unwrap()); // Should be true
    }
}
