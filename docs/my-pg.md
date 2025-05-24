# Write up

## Name

Ziad Elshahawy

## Design

I tried to modularize everything. All the writing is made into functions to make things easier. The data that I mainly chose to save
as far as the metadata goes is a pointer to the next availble slot, number of slots, available space and lowest available slot. These
info allows me to easily answer some of the fucntions in O(1) time, so I found them useful

## Time Estimate / Reflection 

It took me a solid 12 hours, I think. It could be less if I did it in one go, but I decided to modularize it over a longer period.
Rust made me understand things like saturation, borrowing and safe arithmatic calculations that I never paid too much time to, glad
to learn it. Doing disk operations is a nice switch from completely memory boubd operations.

## Incomplete

N/A 

## References

Rust documentation
