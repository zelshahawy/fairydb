# Write up

Ziad Elshahawy

## Query Life-Cycle Question
1. server::run_server() loop -> accept() -> make a new thread
   - handle_client_request(...)
     - read_command() deserializes SQL into `CommandWithArgs`
     - handle_command(..., `CommandWithArgs::Query(sql)`, ...)

2. SQL -> Logical -> Physical
   - handler::handle_command calls
     `Conductor::run_sql_from_string(sql, &server_state)`
   - `Conductor::run_sql_from_string` does:
     * IDK if we need explanation but it translate to `PhysicalRelExpr`

3. Physical plan -> OpIterator tree
   - `Conductor::run_sql_from_string` (or `run_physical_plan`) ->
     `Executor::run_opiterator(Box<dyn OpIterator>)`
   - `Executor::run_opiterator` roughly does:
     1. `iter.configure(true)`
     2. `iter.open()?`
     3. `while let Some(t) = iter.next()? { collect tuples }`
     4. `iter.close()?`

4. `open()` chain
   - `FilterOpIterator::open()` -> `child.open()`
   - `SeqScanOpIterator::open()` -> `StorageManager::open_table(table_id)` (initializes page cursor)

5. `next()` chain
   - `Executor` calls `iter.next()`
   - `FilterOpIterator::next()` -> calls `child.next()` until `a > 10` holds
   - `SeqScanOpIterator::next()`:
     1. if there is no page in memory:
        - call `StorageManager::read_page(table_id, page_no)`
        - underlying `HeapFile::read_page(...)` performs the first disk I/O
     2. scan tuples in the page and return the next tuple
     3. when the page is exhausted, increment `page_no` and repeat

So first actual disk IO occures when
		- `SeqScanOpIterator::next()` when its in-memory buffer is empty and it needs to fetch page 0.

## Design

In a sense, I did the opposite of a lazy approach when doing aggregation. When I call `open`, I immediately do everything instead of only doing the specific work concerning each `next`. I haven't tested through my benchmark, but this approach could
have detrimental effect on speed. That is CONTRARY TO MY `NEOVIM` set up that utlizes Lazy approach that you should definitely
star on github [here](https://github.com/zelshahawy/dotfiles) - shameless plug. If your looking for undergrad researchers, please dm me. Would love to work with y'all.
I can do the dirty work, if needed.

## Time Estimate / Reflection

Very short, ~7hours, but I couldv've done it in shorter time, I think. All the fucntions were very straightforward
compared to last milestone.

## Incomplete

Nah, I'd win.

## References

Rust documentation and the notes were extremely helpful this milestone.
