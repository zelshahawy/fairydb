# CrustyDB: Page Debugging Guide

Note this guide is a bit stale, so parts of the code / references might be out of 
date, but the key ideas should still be useful.

## Overview
This milestone focuses on implementing a **page**, a fixed size data structure
that holds records of varying sizes. You will construct this data structure,
handle reads, writes, and deletes, serialize and deserialize the page, and manage
free space with a header.

As a result, you will likely over the course of this milestone want to view the
contents of a page to determine if records have been inserted, updated, or
deleted properly and to see if your process of serialization and deserialization
work. To this end, we have implemented a HEX Viewer tool, the code for which is
in `page.rs` already. This way, you can print the contents of a page and it will
display in an organized, readable way to aid debugging. Next we will describe
this tool, how you can use it, how it works, and how you may want to modify itj.

## HEX Viewer

### How do you use it?
Using it is extremely easy: you simply need to use any of the logging macros,
such as `info!` or `debug!`, on a page object. For instance, with some record
`x`:

```rust
let mut p = Page::new(0);
p.add_value(x);
info!("{:?}", p);
```

The tool is built as a custom formatting implementation for `Page`, so all you
need to do to print the contents is call `println!`.

### How do you read results?
Now we will show a sample output of calling `println!` on a page object after
inserting some records. We print all
the bytes of the page, 40 bytes printed per line. If all bytes in a line are
0x0, then we omit those and instead print the number of sequential lines
ommitted in this way: i.e. `89 empty lines were hidden`. We print this for
sequential sets of empty lines, so if there were another set of continuous empty
lines that occur later in the page, they would appear with a separate `empty lines
were hidden` message.

For each nonempty line, we first print position in the page that the line begins
on. For instance, the first line will always start at byte 0. The second line
starts at byte 40--since we set the number of bytes per line to be 40--or 0x28.
In the actual line, we print each byte in HEX or base 16 separated by a space. If the byte is 0x0 we
print simply `. ` and if the byte is 0xff we print `##`, otherwise we simply
print the two digit hex code for that byte. 40 bytes does not evenly divide the
page size of 4096 bytes, so the last line does not have 40 bytes to print, hence
it finishes early: this is different than the remaining bytes being 0x0, as this
would be represented by `. `. Instead, the page simply ends after the final 0x01
in the last line.

```
[   0] .  .  14 .  ec 0f 14 .  d8 0f 14 .  c4 0f 14 .  b0 0f 14 .  9c 0f 14 .  88 0f 14 .  74 0f 14 .  60 0f 14 .  4c 0f 14 .
[  40] 38 0f 14 .  24 0f 14 .  10 0f 14 .  fc 0e 14 .  e8 0e 14 .  d4 0e 14 .  c0 0e 14 .  ac 0e 14 .  98 0e 14 .  84 0e 14 .
[  80] 70 0e 14 .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .
89 empty lines were hidden
[3680] .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## 13 13 13 13
[3720] 13 13 13 13 13 13 13 13 13 13 13 13 13 13 13 13 12 12 12 12 12 12 12 12 12 12 12 12 12 12 12 12 12 12 12 12 11 11 11 11
[3760] 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 10 10 10 10 10 10 10 10 10 10 10 10 10 10 10 10 10 10 10 10 0f 0f 0f 0f
[3800] 0f 0f 0f 0f 0f 0f 0f 0f 0f 0f 0f 0f 0f 0f 0f 0f 0e 0e 0e 0e 0e 0e 0e 0e 0e 0e 0e 0e 0e 0e 0e 0e 0e 0e 0e 0e 0d 0d 0d 0d
[3840] 0d 0d 0d 0d 0d 0d 0d 0d 0d 0d 0d 0d 0d 0d 0d 0d 0c 0c 0c 0c 0c 0c 0c 0c 0c 0c 0c 0c 0c 0c 0c 0c 0c 0c 0c 0c 0b 0b 0b 0b
[3880] 0b 0b 0b 0b 0b 0b 0b 0b 0b 0b 0b 0b 0b 0b 0b 0b 0a 0a 0a 0a 0a 0a 0a 0a 0a 0a 0a 0a 0a 0a 0a 0a 0a 0a 0a 0a 09 09 09 09
[3920] 09 09 09 09 09 09 09 09 09 09 09 09 09 09 09 09 08 08 08 08 08 08 08 08 08 08 08 08 08 08 08 08 08 08 08 08 07 07 07 07
[3960] 07 07 07 07 07 07 07 07 07 07 07 07 07 07 07 07 06 06 06 06 06 06 06 06 06 06 06 06 06 06 06 06 06 06 06 06 05 05 05 05
[4000] 05 05 05 05 05 05 05 05 05 05 05 05 05 05 05 05 04 04 04 04 04 04 04 04 04 04 04 04 04 04 04 04 04 04 04 04 03 03 03 03
[4040] 03 03 03 03 03 03 03 03 03 03 03 03 03 03 03 03 02 02 02 02 02 02 02 02 02 02 02 02 02 02 02 02 02 02 02 02 01 01 01 01
[4080] 01 01 01 01 01 01 01 01 01 01 01 01 01 01 01 01
```

### How does it work?

We'll now parse in detail what each section of the code for this function does,
both so you can understand and potentially use it again in other contexts in
CrustyDB and so you can modify if you wish. The function that produces this
starts on line 193 in `page.rs`. 

You can read more about how Rust formatting works for types [here](https://doc.rust-lang.org/std/fmt/index.html#formatting-traits), 
but to format an object using the `?` operator, we must implement the `Debug`
trait, hence: `impl fmt::Debug for Page`. To implement this trait, we must
implement the `fmt` function.

After creating a buffer, we then create a position pointer `pos` and loop while
`pos` has not reached the end of the page. We use `comp` to easily tell what
lines are empty. If at least `BYTES_PER_LINE` remain in the page, we grab those
many bytes, check if they are empty, and if not print each byte in the line,
using `. ` or `##` for 0x00 and 0xff. Finally, the else handles the final line,
which may have fewer than `BYTES_PER_LINE` bytes. Finally, we call `write!` to
finish the `fmt` function.

### How can you modify or extend it and how may you want to do so?

If you wish to either shorten or extend lines, you can simply vary
`BYTES_PER_LINE` on line 13 of `page.rs`. If you wish to change the way the
positions are printed at the beginnings of lines or how 0x0 or 0xff are printed,
please do--you can find how these are done within the loop described above. All
of these changes would be for personal preference: you do not have to modify
these values at all and will still be able to use the tool to its full effect.

As you implement `Page`, you will need to implement a header to manage free
space. As this is still unimplemented, the tool doesn't print anything prior to
the contents of the page. In line 200 we add a comment indicating where you
should add code to print the contents of the header. This could include
information such as the `PID` or the location of slots and slot IDs. 

This tool is not used in the tests so if you do not wish to use or do not wish
to modify it, that's totally fine. However, adding in some code to print the
headers of pages may be beneficial if you are running into bugs due to free
space management, record deletion, or slot management. 

Happy debugging!
