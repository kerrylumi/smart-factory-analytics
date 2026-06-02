-- Singular test: kõik OEE komponendid ja liit-OEE peavad olema vahemikus [0, 1].
-- Tagastab read, mis rikuvad reeglit (test läbib, kui ridu pole).

select machine, paev, availability, performance, quality, oee
from {{ ref('gold_oee') }}
where availability not between 0 and 1
   or performance  not between 0 and 1
   or quality      not between 0 and 1
   or oee          not between 0 and 1
