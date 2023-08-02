{{ config(materialized='view') }}

select c.C_CUSTKEY, c.C_NAME, c.C_NATIONKEY as nation, sum(o.O_TOTALPRICE) as total_order_price
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER c 
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS o
on c.c_custkey = o.o_custkey
group by c.C_CUSTKEY, c.C_NAME, c.C_NATIONKEY