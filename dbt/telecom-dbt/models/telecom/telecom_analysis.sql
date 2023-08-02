{{ config(materialized='table') }}

select t1.msisdn, t1.gender, t1.mobile_type, t1.system_status, t1.value_segment, 
t1.year_of_birth, t2.brand_name, t2.imei_tac, t2.model_name, t2.os_name, t2.os_vendor, 
t3.revenue_usd, t3.week_number from ANALYTICS.CLEANSED.CRM t1 inner join 
ANALYTICS.CLEANSED.DEVICE t2 on t1.msisdn = t2.msisdn inner join ANALYTICS.CLEANSED.REVENUE t3 on 
t2.msisdn = t3.msisdn