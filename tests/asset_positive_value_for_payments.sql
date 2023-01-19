select 
    * 
from {{ ref('stg_payments') }}
where amount > 0
limit 10