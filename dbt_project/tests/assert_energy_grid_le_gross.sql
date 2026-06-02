-- Singular test: võrguost ei saa maksta rohkem kui kogu tarve sama hinnaga
-- (grid_cost <= gross_cost), ja kulud peavad olema mittenegatiivsed.
-- Tagastab read, mis rikuvad reeglit (test läbib, kui ridu pole).

select ts_utc, grid_cost_eur, gross_cost_eur, solar_saving_eur
from {{ ref('gold_energy') }}
where grid_cost_eur > gross_cost_eur + 1e-9
   or grid_cost_eur < 0
   or solar_saving_eur < 0
