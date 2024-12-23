

{{
    config(
        materialized='incremental'
    )
}}

with last_day as (
select * from {{ ref('int_fuel_price') }} ifp  where DATE("DT_TRANSACTION") = DATE(NOW()) - 1
),
current as (
	select * from {{ ref('int_fuel_price') }} ifp  where DATE("DT_TRANSACTION") = DATE(NOW())
)

select 
	cr."CODE_STATION", 
	cr."LONGITUDE",
	cr."LATITUDE",
	cr."ADDRESS", 
	cr."CITY", 
	cr."GAZOLE_PRICE", 
	cr."AVG_GAZOLE_PRICE_OVER_DEP",
	case 
		when ld."GAZOLE_PRICE" is null then null
		else ((cr."GAZOLE_PRICE" - ld."GAZOLE_PRICE") / ld."GAZOLE_PRICE") * 100
	end as "PERCENT_GAZOLE_VAR_PRICE",
	cr."SP95_PRICE", 
	cr."AVG_SP95_PRICE_OVER_DEP",
	case 
		when ld."SP95_PRICE" is null then null
		else ((cr."SP95_PRICE" - ld."SP95_PRICE") / ld."SP95_PRICE") * 100
	end as "PERCENT_SP95_VAR_PRICE",
	cr."E85_PRICE", 
	cr."AVG_E85_PRICE_OVER_DEP",
	case 
		when ld."E85_PRICE" is null then null
		else ((cr."E85_PRICE" - ld."E85_PRICE") / ld."E85_PRICE") * 100
	end as "PERCENT_E85_VAR_PRICE",
	cr."GPLC_PRICE", 
	cr."AVG_GPLC_PRICE_OVER_DEP",
	case 
		when ld."GPLC_PRICE" is null then null
		else ((cr."GPLC_PRICE" - ld."GPLC_PRICE") / ld."GPLC_PRICE") * 100
	end as "PERCENT_GPLC_PRICE_VAR_PRICE",
	cr."E10_PRICE", 
	cr."AVG_E10_PRICE_OVER_DEP",
	case 
		when ld."E10_PRICE" is null then null
		else ((cr."E10_PRICE" - ld."E10_PRICE") / ld."E10_PRICE") * 100
	end as "PERCENT_E10_VAR_PRICE",
	cr."SP98_PRICE", 
	cr."AVG_SP98_PRICE_OVER_DEP",
	case 
		when ld."SP98_PRICE" is null then null
		else ((cr."SP98_PRICE" - ld."SP98_PRICE") / ld."SP98_PRICE") * 100
	end as "PERCENT_SP98_VAR_PRICE",
	dd."NOM_DEPARTEMENT", 
	dr."NOM_REGION" 
from current cr left outer join last_day ld on cr."CODE_STATION" = ld."CODE_STATION" 
inner join dim_departement dd on cr."CODE_DEPARTEMENT" = dd."CODE_DEPARTEMENT" 
inner join dim_regions dr on cr."CODE_REGION" = dr."CODE_REGION"
