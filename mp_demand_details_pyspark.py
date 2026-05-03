
# #### Imports

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, coalesce, lit, udf, when, to_timestamp,
    row_number, from_json, explode, split as spark_split,
    concat_ws, collect_list, min as spark_min, max as spark_max,
    sum as spark_sum, count as spark_count, floor, cast,
    regexp_replace, trim
)
from pyspark.sql.types import (
    ArrayType, StringType, IntegerType, DoubleType, FloatType
)
from pyspark.sql.window import Window
import datetime as dt
from datetime import datetime, timedelta


# In[ ]:





# In[2]:


def create_spark_session():
    """Create Spark session with memory-optimized configuration"""
    return (SparkSession.builder
        .appName("mp_demand_details")
        # Iceberg configuration
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue_catalog.warehouse", "s3://weye-data-platform/dataPlatform/sourcedata/")
        .config("spark.sql.catalog.glue_catalog_saas_analytics", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog_saas_analytics.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog_saas_analytics.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue_catalog_saas_analytics.warehouse", "s3://weye.analytics/saas/sourcedata/")
        .config("spark.sql.catalog.glue_catalog_mp_analytics", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog_mp_analytics.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog_mp_analytics.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue_catalog_mp_analytics.warehouse", "s3://weye.analytics/marketplace/sourcedata/")
        .config("spark.sql.catalog.glue_catalog_ds", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog_ds.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog_ds.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue_catalog_ds.warehouse", "s3://weye-data-science/marketplace/sourcedata/")
        .config("spark.sql.defaultCatalog", "glue_catalog")
        # Memory optimization settings
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.autoBroadcastJoinThreshold", 100 * 1024 * 1024)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate())

spark = create_spark_session()


# In[3]:


jdbc_url = "jdbc:redshift://redshift-cluster-2.ct9kqx1dcuaa.ap-south-1.redshift.amazonaws.com:5439/datalake"
usr = 'singhgajinder'
pasw = 'Gajju123'
properties = {
    "user": usr,
    "password": pasw,
    "driver": "com.amazon.redshift.jdbc42.Driver"
}


# In[ ]:





# In[ ]:





# #### Demand Range for update

# In[4]:


# start_date = dt.date.today() - timedelta(days=10)
start_date = dt.date(2019,1,1)


# In[5]:


year = start_date.year
month = start_date.month
day = start_date.day


# In[6]:


print(year, month, day)


# In[7]:


q = '''
select min(demand_id) as min_demand_id, max(demand_id) as max_demand_id
from deanalytics.mp_demand_tmp
where date(demand_date)>'{}'
'''.format(start_date)
demand_id = spark.sql(q)


# In[8]:


start_demand = demand_id.select('min_demand_id').collect()[0][0]
end_demand = demand_id.select('max_demand_id').collect()[0][0]


# In[9]:


print(start_demand, end_demand, end_demand-start_demand)


# In[ ]:





# In[ ]:





# In[ ]:





# #### DEMAND QUERY

# In[10]:


q = f'''

select a.demand_id, a.demand_date, a.updated_at, a.consigner_user_code, a.vehicle_type_id, a.commodity, a.created_by, a.assigned_to, a.status, 
a.metadata_remarks, a.metadata_remarkscategory, a.metadata_plt, a.app_version, a.dr_type, a.from_lat, a.from_long, a.to_lat, a.to_long, 
a.route_distance, a.shortest_route_distance, a.predicted_rate_source, a.consigner_freight_fare, a.actual_consigner_freight_fare, a.service_charge, 
a.supplyfare_before_consignment, a.origin_district_key, a.destination_district_key,

cast(case when cast(a.service_charge as float) > 0 then floor(((a.consigner_freight_fare / (1.0 + cast(a.service_charge as float))) + 99) / 100) * 100
    else a.consigner_freight_fare end as double) as base_price,
case when a.actual_consigner_freight_fare >= a.consigner_freight_fare then a.actual_consigner_freight_fare - a.consigner_freight_fare
     else 0 end as extra_discount,
case when a.actual_consigner_freight_fare >= 0 then a.actual_consigner_freight_fare else a.consigner_freight_fare end as updated_consigner_freight_fare,

cust_type.customer_type, cust_type.customer_flag,

b.body_type, b.tyre_count, b.size_in_ft, b.tonnage,
e.min_size, e.max_size, e.min_tonnage, e.max_tonnage, e.veh_tyre_type, e.vt_pricing_id, e2.vt_id, coalesce(g.drop_points,'Single') as drop_points,
h.consignment_code, h.total_consignments, h.supplyfare, h.consignment_date, h.operator_code, h.vehicle_id, h.trip_state, h2.trip_end_time,
i.fo_commission, i.fo_commission_reversal, 
l.origin, l.origin_id, l.origin_state, l.origin_cluster, l.origin_cluster_id,
m.destination, m.destination_id, m.destination_state, m.destination_cluster, m.destination_cluster_id,
o.shipper_discount_tmp, o.shipper_discount_reversal_tmp, 
r.dr_flag, r.first_dr_time, r.demand_cancel_time, s.exp_ids

from

(
select id as demand_id, (created + interval 5 hours 30 minutes)  as demand_date, (updated + interval 5 hours 30 minutes)  as updated_at,
consigner_user_code, vehicle_type_id, commodity, created_by, assigned_to, status,
get_json_object(metadata, '$.remarks') as metadata_remarks,
case when get_json_object(metadata, '$.remarksCategory') != '' then get_json_object(metadata, '$.remarksCategory')
    else expiry_remark end as metadata_remarkscategory,
get_json_object(metadata, '$.ptl') as metadata_plt,
get_json_object(metadata, '$.appVersion') as app_version,
get_json_object(metadata, '$.searchMode') as dr_type,
cast(get_json_object(get_json_object(from_address_dto,'$.geoLoc'),'$.x') as double) as from_lat,
cast(get_json_object(get_json_object(from_address_dto,'$.geoLoc'),'$.y') as double) as from_long,
cast(get_json_object(get_json_object(to_address_dto,'$.geoLoc'),'$.x') as double) as to_lat,
cast(get_json_object(get_json_object(to_address_dto,'$.geoLoc'),'$.y') as double) as to_long,
case when get_json_object(metadata,'$.estimatedRouteDistance') != '' then cast(get_json_object(metadata,'$.estimatedRouteDistance') as double)
    else 0 end as route_distance,
case when get_json_object(metadata,'$.shortestRouteDistance') != '' then cast(get_json_object(metadata,'$.shortestRouteDistance') as double)
    else 0 end as shortest_route_distance,
get_json_object(metadata, '$.predictedRateSource') as predicted_rate_source,
consigner_freight_fare,
actual_consigner_freight_fare,
case when get_json_object(metadata,'$.serviceChargePct') = '' or get_json_object(metadata,'$.serviceChargePct') is null then 0
    else cast(get_json_object(metadata,'$.serviceChargePct') as double) end as service_charge,
case when get_json_object(metadata,'$.supplyFreightFareBeforeConsignment') != '' then cast(get_json_object(metadata,'$.supplyFreightFareBeforeConsignment') as double)
    else null end as supplyfare_before_consignment,
origin_district_key, destination_district_key

from data_platform.wfms_demands
where (
        dpyear > {year}
        or (dpyear = {year} and dpmonth > {month})
        or (dpyear = {year} and dpmonth = {month} and dpday >= {day})
        )
and deleted='false' 
) a


left join

( 
select code, type as customer_type,
       case when (lower(name) like '%alternate%' or lower(name) like '%testing%')
            then 'Testing' else 'WE_Customer' end as customer_flag
from data_platform.vasooli_consigners
where deleted='false' 
) cust_type
on a.consigner_user_code=cust_type.code


left join

( 
select id, size_in_ft, tonnage, tyre_count, lower(body_type) as body_type,
       vehicle_height, adjusted_tonnage
from data_platform.wfms_vehicle_types
where deleted='false' 
) b
on a.vehicle_type_id=b.id


left join

( 
select id as vt_pricing_id,
       lower(body_type) as body_type, tyre,
       min_length as min_size, max_length as max_size,
       min_weight as min_tonnage, max_weight as max_tonnage,
       case when lower(body_type)='container' and tyre=4 and min_length>=1  and max_length<=7  and min_weight<=1.5 then 'LCV'
            when lower(body_type)='container' and tyre=4 and min_length>=8  and max_length<=9  and min_weight<=2   then 'LCV'
            when lower(body_type)='open'      and tyre=4 and min_length>=1  and max_length<=7  and min_weight<=1.5 then 'LCV'
            when lower(body_type)='open'      and tyre=4 and min_length>=8  and max_length<=9  and min_weight<=2   then 'LCV'
            when lower(body_type)='open'      and tyre=4 and min_length>=12 and max_length<=13 and min_weight<=2   then 'LCV'
            when lower(body_type)='container' and tyre=6 and min_length>=27 and max_length<=50 and min_weight<=9   then 'SXL'
            when lower(body_type)='container' and tyre=10 and min_length>=16 and max_length<=50 and min_weight>=10 and min_weight<=18 then 'MXL'
            when lower(body_type)='trailer'   then 'TRAILER'
            when tyre=4 or tyre=6             then '4_6'
            when tyre=10 or tyre=12 or tyre=14 then '10_12_14'
            else 'Other' end as veh_tyre_type
from data_platform.wfms_vehicle_classification
where flow='PRICING' and deleted='false' 
) e
on b.body_type=e.body_type and b.tyre_count=e.tyre and b.size_in_ft>=e.min_size and b.size_in_ft<=e.max_size
and b.tonnage>=e.min_tonnage and b.tonnage<=e.max_tonnage


left join

( 
select id as vt_id, lower(body_type) as body_type, tyre as tyre, size
from deanalytics.fact_demand_vt 
) e2
on b.body_type=e2.body_type and b.tyre_count=e2.tyre and b.size_in_ft=e2.size


left join

( 
select demand_id, 'Multiple' as drop_points
from data_platform.wfms_address_v2
where (
        dpyear > {year}
        or (dpyear = {year} and dpmonth > {month})
        or (dpyear = {year} and dpmonth = {month} and dpday >= {day})
        )
group by 1
having count(demand_id) > 2
) g
on a.demand_id=g.demand_id


left join

( 
select code as consignment_code, demand_id, total_consignments, supplyfare, consignment_date,
       operator_code, vehicle_id, vehicle_number, trip_state, id
from
(
select code, demand_id,
cast(case when get_json_object(expense_data,'$.supplyFare')='' then '0' else get_json_object(expense_data,'$.supplyFare') end as float) as supplyfare,
(created + interval 5 hours 30 minutes) as consignment_date,
operator_code, vehicle_id, vehicle_number, state as trip_state, id,
count(operator_code) over(partition by demand_id) as total_consignments,
row_number() over(partition by demand_id order by updated desc) as rnk
from data_platform.wfms_consignments
where (
        dpyear > {year}
        or (dpyear = {year} and dpmonth > {month})
        or (dpyear = {year} and dpmonth = {month} and dpday >= {day})
        )
and deleted='false' 
)
where rnk=1 
) h
on a.demand_id=h.demand_id


left join

( 
select consignment_code, max(created + interval 5 hours 30 minutes) as trip_end_time
from data_platform.wfms_consignment_state_info
where dpyear >= {year} and deleted='false' and state='TRIP_END'
group by 1 
) h2
on h.consignment_code=h2.consignment_code


left join

( 
select wce.consignment_id,
sum(case when wce.category='CREDIT' and wce.type in ('TRIP DISCOUNT - REVERSAL','TRIP DISCOUNT','WE DISCOUNT','WE DISCOUNT - REVERSAL') then wce.amount else 0 end) as fo_commission_reversal,
sum(case when wce.category='DEBIT'  and wce.type in ('TRIP DISCOUNT - REVERSAL','TRIP DISCOUNT','WE DISCOUNT','WE DISCOUNT - REVERSAL') then wce.amount else 0 end) as fo_commission
from
(
select consignment_id, type, amount, category
from data_platform.wfms_consignment_expense
where (
        dpyear > {year}
        or (dpyear = {year} and dpmonth > {month})
        or (dpyear = {year} and dpmonth = {month} and dpday >= {day})
        )
and deleted='false' and party_type='OPERATOR'
and type in ('TRIP DISCOUNT - REVERSAL','TRIP DISCOUNT','WE DISCOUNT','WE DISCOUNT - REVERSAL')
) wce
join
(
select cast(split_part(code,'-',2) as int) as code_id, demand_id
from data_platform.wfms_consignments
where (
        dpyear > {year}
        or (dpyear = {year} and dpmonth > {month})
        or (dpyear = {year} and dpmonth = {month} and dpday >= {day})
        )
) wc
on wce.consignment_id = wc.code_id
group by 1 
) i
on h.id=i.consignment_id


left join

(
select key, name as origin, id as origin_id, state as origin_state, cluster as origin_cluster, cluster_id as origin_cluster_id
from deanalytics.fact_districts_v2
) l
on a.origin_district_key=l.key


left join

(
select key, name as destination, id as destination_id, state as destination_state, cluster as destination_cluster, cluster_id as destination_cluster_id
from deanalytics.fact_districts_v2
) m
on a.destination_district_key=m.key


left join

( 
select demand_id,
sum(case when category='CREDIT' and party_type='CONSIGNER' and type in ('COUPON DISCOUNT','COUPON DISCOUNT - REVERSAL','DISCOUNT COUPON') then amount else 0 end) as shipper_discount_tmp,
sum(case when category='DEBIT'  and party_type='CONSIGNER' and type in ('COUPON DISCOUNT','COUPON DISCOUNT - REVERSAL','DISCOUNT COUPON') then amount else 0 end) as shipper_discount_reversal_tmp
from data_platform.wfms_trip_expense
where (
        dpyear > {year}
        or (dpyear = {year} and dpmonth > {month})
        or (dpyear = {year} and dpmonth = {month} and dpday >= {day})
        )
and deleted='false' and party_type='CONSIGNER'
group by 1 ) o
on a.demand_id=o.demand_id


left join

( 
select demand_id,
max(case when sub_status in ('DR','DR_BOOK') then 1 else 0 end) as dr_flag,
min(case when sub_status in ('DR','DR_BOOK') then sub_status_time else null end) as first_dr_time,
min(case when sub_status='CANCELLED' then sub_status_time else null end) as demand_cancel_time
from
(
select demand_id, sub_status, (created + interval 5 hours 30 minutes) as sub_status_time
from data_platform.wfms_demand_sub_status_info
where (
    dpyear > {year}
    or (dpyear = {year} and dpmonth > {month})
    or (dpyear = {year} and dpmonth = {month} and dpday >= {day})
    )
and sub_status in ('DR','DR_BOOK','CANCELLED')
)
group by 1 
) r
on a.demand_id=r.demand_id


left join

( 
select demand_id, experiment_ids as exp_ids
from data_platform.pricing_demand_bidding_config
where (
        dpyear > {year}
        or (dpyear = {year} and dpmonth > {month})
        or (dpyear = {year} and dpmonth = {month} and dpday >= {day})
        )
and experiment_ids != '' and experiment_ids is not null 
) s
on a.demand_id=s.demand_id


'''

demand = spark.sql(q)


# In[ ]:





# In[ ]:





# In[ ]:





# #### Repeat/New Consigners

# In[11]:


q = '''
select consigner_user_code, min(demand_id) as first_trip_id
from deanalytics.mp_demand_tmp
where status='FULFILLED'
group by 1
'''
first_trip_id = spark.sql(q)


# In[12]:


demand = demand.join(first_trip_id, how='left', on='consigner_user_code')


# In[13]:


demand = demand.withColumn(
    'consigner_type',
    when(col('demand_id') > col('first_trip_id'), lit('Repeat Consigner'))
    .otherwise(lit('New Consigner'))
)


# In[14]:


del first_trip_id


# In[15]:


print('Demand Query')


# In[ ]:





# In[ ]:





# #### Base Rate

# In[16]:


l7d = dt.date.today() - timedelta(days=7)


# In[17]:


q = f'''
select demand_id, operator_code, quote
from
(
select demand_id, operator_code,
case when cast(get_json_object(calling_details,'$.quote') as double) > 0
    then cast(get_json_object(calling_details,'$.quote') as double)
    else cast(get_json_object(app_response_details,'$.quote') as double) end as quote
from data_platform.pricing_operator_demand_quotation
where (
        dpyear > {l7d.year}
        or (dpyear = {l7d.year} and dpmonth > {l7d.month})
        or (dpyear = {l7d.year} and dpmonth = {l7d.month} and dpday >= {l7d.day})
        )
and deleted='false'
and operator_code in ('rate_card_1','rate_card_2','rate_card_3')
)
where quote>0
'''
l7d_base_rate = spark.sql(q)


# In[18]:


query = '''
(
select demand_id, operator_code, quote
from de_analytics.operator_demand_quotation_archive
where operator_code in ('rate_card_1','rate_card_2','rate_card_3') and quote is not null
and date(created)<date(current_date-7)
and date(created)>='{}'
)
'''.format(l7d)
old_base_rate = spark.read.jdbc(
    url=jdbc_url,
    table=query,
    properties=properties
)


# In[19]:


base_rate = l7d_base_rate.unionByName(old_base_rate)


# In[20]:


del l7d_base_rate
del old_base_rate


# In[21]:


q = f'''
select demand_id, max(cast(get_json_object(internal_commission,'$.value') as double)) as int_commission
from data_platform.pricing_demand_bidding_config
where (
        dpyear > {year}
        or (dpyear = {year} and dpmonth > {month})
        or (dpyear = {year} and dpmonth = {month} and dpday >= {day})
        )
and deleted='false' and get_json_object(internal_commission,'$.value') != ''
group by 1
'''
int_commission = spark.sql(q)


# In[22]:


base_rate = base_rate.join(int_commission, how='left', on='demand_id')


# In[23]:


base_rate = base_rate.withColumn(
    'quote_new',
    floor((col('quote') * (1.0 + coalesce(col('int_commission'), lit(0)) / 100) + 99) / 100) * 100
)


# In[24]:


base_rate = base_rate.groupBy('demand_id').agg(
    lit(1).alias('base_rate_flag'),
    F.max(when(col('operator_code') == 'rate_card_1', col('quote_new')).otherwise(None)).alias('base_rate1'),
    F.max(when(col('operator_code') == 'rate_card_2', col('quote_new')).otherwise(None)).alias('base_rate2'),
    F.max(when(col('operator_code') == 'rate_card_3', col('quote_new')).otherwise(None)).alias('base_rate3')
)


# In[25]:


demand = demand.join(base_rate, how='left', on='demand_id')


# In[26]:


print('Base Rate')


# In[ ]:





# In[ ]:





# #### Placement Type

# In[27]:


# Extracting Token Forfiet Data also


# In[28]:


q = f'''
select demand_id, operator_code, status,
get_json_object(metadata,'$.triggerSource') as trigger_source,
case when get_json_object(metadata,'$.triggerSource') in ('BOOKING_AUTOMATION','TESSERACT_SERVICE') then 'Auto' else 'Manual' end as token,
case when get_json_object(metadata,'$.biddingType') like '%B%' then 'Bidding' else 'Matching' end as placement_phase,
cast(get_json_object(metadata,'$.amountInPaisa') as int)/100 as amount_in_paisa
from data_platform.wfms_operator_demand_token
where (
        dpyear > {l7d.year}
        or (dpyear = {l7d.year} and dpmonth > {l7d.month})
        or (dpyear = {l7d.year} and dpmonth = {l7d.month} and dpday >= {l7d.day})
        )
and deleted='false' and status in ('SUCCESS','PAYMENT_SUCCESS','REFUNDED','FORFEITED')

'''
l7d_token = spark.sql(q)


# In[29]:


query = '''
(
select demand_id, operator_code, status, 
trigger_source, 
case when trigger_source in ('BOOKING_AUTOMATION','TESSERACT_SERVICE') then 'Auto' else 'Manual' end as token,
case when bidding_type like '%%B%%' then 'Bidding' else 'Matching' end as placement_phase,
amount_in_paisa/100 as amount_in_paisa
from analytics.wfms_operator_demand_token_archive
where deleted='false' and status in ('SUCCESS','PAYMENT_SUCCESS','REFUNDED','FORFEITED')
and date(created)<date(current_date-7)
and date(created)>='{}'
)
'''.format(l7d)
old_token = spark.read.jdbc(
    url=jdbc_url,
    table=query,
    properties=properties
)


# In[30]:


token_data = l7d_token.unionByName(old_token)


# In[31]:


del old_token
del l7d_token


# In[32]:


demand_token = token_data.filter(col('status').isin('SUCCESS', 'PAYMENT_SUCCESS', 'REFUNDED')) \
    .groupBy('demand_id', 'operator_code').agg(
        F.count(when(col('token') == 'Manual', 1)).alias('manual'),
        F.count(when(col('token') == 'Auto', 1)).alias('auto'),
        F.count(when(col('placement_phase') == 'Matching', 1)).alias('matching'),
        F.count(when(col('placement_phase') == 'Bidding', 1)).alias('bidding')
    )


# In[33]:


demand_token = demand_token.withColumn(
    'plac_type',
    when(col('manual') > 0, lit('Manual')).otherwise(lit('Automation'))
).withColumn(
    'placement_phase',
    when(col('matching') > 0, lit('Matching')).otherwise(lit('Bidding'))
).select('demand_id', 'operator_code', 'plac_type', 'placement_phase')


# In[34]:


plac_type = demand.filter(col('total_consignments') > 0) \
    .select('demand_id','operator_code') \
    .join(demand_token, on=['demand_id','operator_code'], how='left')
plac_type = plac_type.withColumn('plac_type',       when(col('plac_type').isNull(),       lit('Manual')).otherwise(col('plac_type')))
plac_type = plac_type.withColumn('placement_phase', when(col('placement_phase').isNull(), lit('Matching')).otherwise(col('placement_phase')))


# In[35]:


demand = demand.join(plac_type.select('demand_id','plac_type','placement_phase'), on='demand_id', how='left')


# In[36]:


del demand_token
del plac_type


# In[37]:


print('Placement Type')


# In[ ]:





# In[ ]:





# In[ ]:





# #### PNL Calculation

# In[38]:


# shipper fare
demand = demand.withColumn(
    "shipper_discount",
    when(col("actual_consigner_freight_fare") < col("consigner_freight_fare"), lit(0))
    .otherwise(col("shipper_discount_tmp"))
).withColumn(
    "shipper_discount_reversal",
    when(col("actual_consigner_freight_fare") < col("consigner_freight_fare"), lit(0))
    .otherwise(col("shipper_discount_reversal_tmp"))
).drop("shipper_discount_tmp", "shipper_discount_reversal_tmp")


# In[39]:


# Token Forfiet
demand = demand.join(token_data.filter(col('status')=='FORFEITED').groupBy('demand_id').agg(F.sum(col('amount_in_paisa')).alias('token_forfeit')), on='demand_id', how='left')
del token_data


# In[40]:


demand = demand.withColumn(
    'consigner_pnl',
    col('updated_consigner_freight_fare')
    - col('updated_consigner_freight_fare') / (1.0 + col('service_charge').cast('double'))
    - (coalesce(col('shipper_discount'), lit(0)) - coalesce(col('shipper_discount_reversal'), lit(0)))
    - coalesce(col('extra_discount'), lit(0))
).withColumn(
    'fo_pnl',
    col('updated_consigner_freight_fare') / (1.0 + col('service_charge').cast('double'))
    - coalesce(coalesce(col('supplyfare'), col('supplyfare_before_consignment')), lit(0))
    - (coalesce(col('fo_commission_reversal'), lit(0)) - coalesce(col('fo_commission'), lit(0)))
    + coalesce(col('token_forfeit'), lit(0))
).withColumn('pnl', col('consigner_pnl') + col('fo_pnl'))


# In[41]:


print('PNL Calculation')


# In[ ]:





# In[ ]:





# #### Experiment Names

# In[42]:


q = '''
select id as exp_ids_int, name as experiment_name
from data_platform.pricing_experiments
where deleted='false'
'''
exp = spark.sql(q)


# In[43]:


df_exp = demand.filter(col('exp_ids').isNotNull()).select('demand_id', 'exp_ids')
df_exp = df_exp.withColumn('exp_ids', regexp_replace(col('exp_ids'), '[{}]', ''))
df_exp = df_exp.withColumn('exp_ids', F.split(col('exp_ids'), ','))
df_exp = df_exp.withColumn('exp_ids', explode(col('exp_ids')))
df_exp = df_exp.withColumn('exp_ids', trim(col('exp_ids')).cast('int'))
df_exp = df_exp.join(exp, df_exp['exp_ids'] == exp['exp_ids_int'], how='left').drop('exp_ids_int', 'exp_ids')
df_exp = df_exp.fillna({'experiment_name': 'NA'})
df_exp = df_exp.groupBy('demand_id').agg(concat_ws(', ', collect_list('experiment_name')).alias('experiment_name'))


# In[44]:


demand = demand.join(df_exp, on='demand_id', how='left')


# In[45]:


del exp
del df_exp


# In[46]:


print('Experiment Handling')


# In[ ]:





# In[ ]:





# In[ ]:





# #### Segment Config

# In[47]:


q_seg = '''
select demand_id,
max(case when rnk=1  then segment_config else null end) as segment_config_first,
max(case when rnk2=1 then segment_config else null end) as segment_config_latest
from
(
select demand_id, segment_config,
row_number() over(partition by demand_id order by created_at) as rnk,
row_number() over(partition by demand_id order by created_at desc) as rnk2
from
(
select demand_id, segment_config, min(created_at) as created_at
from data_platform.ingrid_demand_operator_segment_mapping
group by 1,2
)
)
group by 1
'''
seg_config = spark.sql(q_seg)


# In[48]:


demand = demand.join(seg_config, on='demand_id', how='left')


# In[49]:


del seg_config


# In[50]:


print('Segment Config done')


# In[ ]:





# In[ ]:





# In[ ]:





# #### Special Request

# In[51]:


q = f'''
select a.demand_id, a.overheight, a.overweight, a.expressdeliverytat,
       a.opendala, a.extrawidth, a.extraperson, a.dieselvehicle,
       case when b.vehicle_height > c.height_limit then 1 else 0 end as pricing_overheight,
       case when b.tonnage       > c.tonnage_limit then 'true' else 'false' end as pricing_overweight,
       special_req,
       case when (case when b.vehicle_height > c.height_limit then 1 else 0 end) > 0
                 or (case when b.tonnage > c.tonnage_limit then 'true' else 'false' end) = 'true'
                 or a.expressdeliverytat > 0
                 or a.opendala    = 'true'
                 or a.extrawidth  = 'true'
                 or a.dieselvehicle='true'
                 or a.extraperson ='true'
            then 1 else 0 end as pricing_special_req
from
(
select id as demand_id, vehicle_type_id,
     cast(get_json_object(get_json_object(metadata,'$.specialRequestBody'),'$.overHeight')        as double) as overheight,
     get_json_object(get_json_object(metadata,'$.specialRequestBody'),'$.overWeight')                        as overweight,
     cast(get_json_object(get_json_object(metadata,'$.specialRequestBody'),'$.expressDeliveryTat') as double) as expressdeliverytat,
     get_json_object(get_json_object(metadata,'$.specialRequestBody'),'$.openDala')                          as opendala,
     get_json_object(get_json_object(metadata,'$.specialRequestBody'),'$.extraWidth')                        as extrawidth,
     get_json_object(get_json_object(metadata,'$.specialRequestBody'),'$.extraPerson')                       as extraperson,
     get_json_object(get_json_object(metadata,'$.specialRequestBody'),'$.dieselVehicle')                     as dieselvehicle,
     case when get_json_object(get_json_object(metadata,'$.specialRequestBody'),'$.overHeight') > 0
               or get_json_object(get_json_object(metadata,'$.specialRequestBody'),'$.overWeight') = 'true'
               or get_json_object(get_json_object(metadata,'$.specialRequestBody'),'$.expressDeliveryTat') > 0
               or get_json_object(get_json_object(metadata,'$.specialRequestBody'),'$.openDala')    = 'true'
               or get_json_object(get_json_object(metadata,'$.specialRequestBody'),'$.extraWidth')  = 'true'
               or get_json_object(get_json_object(metadata,'$.specialRequestBody'),'$.dieselVehicle')='true'
               or get_json_object(get_json_object(metadata,'$.specialRequestBody'),'$.extraPerson') ='true'
          then 1 else 0 end as special_req
from data_platform.wfms_demands
where (
        dpyear > {year}
        or (dpyear = {year} and dpmonth > {month})
        or (dpyear = {year} and dpmonth = {month} and dpday >= {day})
        )
and deleted='false' 
) a

left join
(
select id, size_in_ft, tonnage, tyre_count, lower(body_type) as body_type, vehicle_height
from data_platform.wfms_vehicle_types
where deleted='false'
) b on a.vehicle_type_id = b.id

left join
(
select
lower(body_type) as body_type, tyre,
min_length, max_length,
max(max_weight) as tonnage_limit,
max(max_height) as height_limit
from data_platform.wfms_vehicle_classification
where flow='PRICING' and deleted='false' 
group by 1,2,3,4
) c on b.body_type=c.body_type and b.tyre_count=c.tyre
   and b.size_in_ft>=c.min_length and b.size_in_ft<=c.max_length

where a.special_req=1
'''
special = spark.sql(q)


# In[52]:


def special_type_fn(overheight, overweight, expressdeliverytat, opendala, extrawidth, extraperson, dieselvehicle):
    output = []
    if overheight and overheight > 0:
        output.append('OVER_HEIGHT')
    if overweight == 'true':
        output.append('OVER_WEIGHT')
    if expressdeliverytat and expressdeliverytat > 0:
        output.append('EXPRESS_DELIVERY')
    if opendala == 'true':
        output.append('OPEN_DALA')
    if extrawidth == 'true':
        output.append('OVER_WIDTH')
    if extraperson == 'true':
        output.append('EXTRA_PERSON')
    if dieselvehicle == 'true':
        output.append('DIESEL_VEHICLE')
    return output

special_type_udf = udf(special_type_fn, ArrayType(StringType()))

special = special.withColumn(
    'special_type',
    special_type_udf('overheight', 'overweight', 'expressdeliverytat', 'opendala',
                     'extrawidth', 'extraperson', 'dieselvehicle')
)
special = special.withColumn(
    'pricing_special_type',
    special_type_udf('pricing_overheight', 'pricing_overweight', 'expressdeliverytat',
                     'opendala', 'extrawidth', 'extraperson', 'dieselvehicle')
)


# In[53]:


demand = demand.join(
    special.filter(col('special_req') == 1)
           .select('demand_id', 'special_req', 'special_type'),
    on='demand_id', how='left'
)


# In[54]:


demand = demand.join(
    special.filter(col('pricing_special_req') == 1)
           .select('demand_id', 'pricing_special_req', 'pricing_special_type'),
    on='demand_id', how='left'
)


# In[55]:


demand = demand.withColumn('special_req', coalesce(col('special_req'),        lit(0)))
demand = demand.withColumn('pricing_special_req', coalesce(col('pricing_special_req'), lit(0)))


# In[56]:


del special


# In[57]:


print('Special Request')


# In[ ]:





# #### Region

# In[58]:


NCR = {
    'WEST DELHI_DELHI', 'CENTRAL DELHI_DELHI', 'NEW DELHI_DELHI',
    'NORTH WEST DELHI_DELHI', 'NORTH DELHI_DELHI', 'SOUTH DELHI_DELHI',
    'SHAHDARA_DELHI', 'SOUTH EAST DELHI_DELHI', 'EAST DELHI_DELHI',
    'SOUTH WEST DELHI_DELHI', 'NORTH EAST DELHI_DELHI',
    'FARIDABAD_HARYANA', 'GURUGRAM_HARYANA', 'JHAJJAR_HARYANA', 'NUH_HARYANA',
    'PALWAL_HARYANA', 'REWARI_HARYANA', 'ROHTAK_HARYANA', 'SONIPAT_HARYANA',
    'BAGHPAT_UTTAR PRADESH', 'GAUTAM BUDDHA NAGAR_UTTAR PRADESH',
    'GHAZIABAD_UTTAR PRADESH', 'MEERUT_UTTAR PRADESH', 'BHIWANI_HARYANA'
}

ncr_flag_udf = udf(lambda origin: 'NCR' if origin in NCR else 'Non_NCR', StringType())
demand = demand.withColumn('ncr_flag', ncr_flag_udf(col('origin')))


# In[59]:


ROI_BASE = {
    'AHMEDABAD_GUJARAT', 'AMRELI_GUJARAT', 'ANAND_GUJARAT', 'ARAVALLI_GUJARAT',
    'BOTAD_GUJARAT', 'CHHOTA UDAIPUR_GUJARAT',
    'DADRA AND NAGAR HAVELI_DADRA AND NAGAR HAVELI AND DAMAN AND DIU',
    'DAHOD_GUJARAT', 'DAMAN_DADRA AND NAGAR HAVELI AND DAMAN AND DIU',
    'DANG_GUJARAT', 'DEVBHUMI DWARKA_GUJARAT', 'GANDHINAGAR_GUJARAT',
    'GIR SOMNATH_GUJARAT', 'JAMNAGAR_GUJARAT', 'JUNAGADH_GUJARAT',
    'KHEDA_GUJARAT', 'MAHISAGAR_GUJARAT', 'MEHSANA_GUJARAT', 'MORBI_GUJARAT',
    'NARMADA_GUJARAT', 'NAVSARI_GUJARAT', 'PANCHMAHAL_GUJARAT', 'PORBANDAR_GUJARAT',
    'RAJKOT_GUJARAT', 'SABARKANTHA_GUJARAT', 'SURAT_GUJARAT', 'SURENDRANAGAR_GUJARAT',
    'TAPI_GUJARAT', 'VADODARA_GUJARAT', 'VALSAD_GUJARAT',
    'AHMEDNAGAR_MAHARASHTRA', 'AURANGABAD_MAHARASHTRA', 'BEED_MAHARASHTRA',
    'JALNA_MAHARASHTRA', 'LATUR_MAHARASHTRA', 'MUMBAI CITY_MAHARASHTRA',
    'MUMBAI SUBURBAN_MAHARASHTRA', 'NAGPUR_MAHARASHTRA', 'NANDED_MAHARASHTRA',
    'NASHIK_MAHARASHTRA', 'OSMANABAD_MAHARASHTRA', 'PALGHAR_MAHARASHTRA',
    'PARBHANI_MAHARASHTRA', 'PUNE_MAHARASHTRA', 'RAIGAD_MAHARASHTRA',
    'RATNAGIRI_MAHARASHTRA', 'SATARA_MAHARASHTRA', 'SOLAPUR_MAHARASHTRA',
    'THANE_MAHARASHTRA', 'RAIGARH_CHHATTISGARH',
    'BENGALURU RURAL_KARNATAKA', 'BENGALURU URBAN_KARNATAKA',
    'CHIKKABALLAPURA_KARNATAKA', 'KOLAR_KARNATAKA', 'RAMANAGARA_KARNATAKA',
    'TUMAKURU_KARNATAKA', 'KRISHNAGIRI_TAMIL NADU',
    'CENTRAL DELHI_DELHI', 'EAST DELHI_DELHI', 'NEW DELHI_DELHI',
    'NORTH DELHI_DELHI', 'NORTH EAST DELHI_DELHI', 'NORTH WEST DELHI_DELHI',
    'SHAHDARA_DELHI', 'SOUTH DELHI_DELHI', 'SOUTH EAST DELHI_DELHI',
    'SOUTH WEST DELHI_DELHI', 'WEST DELHI_DELHI',
    'BHIWANI_HARYANA', 'FARIDABAD_HARYANA', 'GURUGRAM_HARYANA', 'JHAJJAR_HARYANA',
    'NUH_HARYANA', 'PALWAL_HARYANA', 'PANIPAT_HARYANA', 'REWARI_HARYANA',
    'ROHTAK_HARYANA', 'SONIPAT_HARYANA',
    'BAGHPAT_UTTAR PRADESH', 'BULANDSHAHR_UTTAR PRADESH',
    'GAUTAM BUDDHA NAGAR_UTTAR PRADESH', 'GHAZIABAD_UTTAR PRADESH',
    'HAPUR_UTTAR PRADESH', 'MEERUT_UTTAR PRADESH', 'ALWAR_RAJASTHAN',
}
ROI_EXTENDED = ROI_BASE | {'JAIPUR_RAJASTHAN', 'HYDERABAD_TELANGANA'}
ROI3_ORIGINS = {'JAIPUR_RAJASTHAN', 'HYDERABAD_TELANGANA'}


# In[60]:


def get_demand_region(origin, destination, demand_date):
    if origin in NCR:
        return 'NCR'
    if origin in ROI_BASE:
        return 'ROI'
    try:
        d = demand_date.date() if hasattr(demand_date, 'date') else dt.datetime.strptime(str(demand_date)[:10], '%Y-%m-%d').date()
    except Exception:
        return 'OTHERS'
    if d >= dt.date(2025, 12, 9) and origin not in ROI_EXTENDED and destination in ROI_EXTENDED:
        return 'ROI:2'
    if d >= dt.date(2025, 12, 18) and origin in ROI3_ORIGINS:
        return 'ROI:3'
    return 'OTHERS'

demand_region_udf = udf(get_demand_region, StringType())
demand = demand.withColumn('demand_region', demand_region_udf(col('origin'), col('destination'), col('demand_date')))


# In[ ]:





# In[61]:


zone_to_states = {
    'EAST':        ['WEST BENGAL', 'BIHAR', 'JHARKHAND', 'ODISHA', 'CHHATTISGARH'],
    'NORTHEAST':   ['SIKKIM', 'ASSAM', 'MEGHALAYA', 'MIZORAM', 'MANIPUR', 'TRIPURA', 'ARUNACHAL PRADESH', 'NAGALAND'],
    'NORTH':       ['PUNJAB', 'HARYANA', 'DELHI', 'UTTAR PRADESH', 'CHANDIGARH'],
    'CENTRAL':     ['MADHYA PRADESH', 'RAJASTHAN'],
    'WEST':        ['MAHARASHTRA', 'GUJARAT', 'GOA', 'DADRA AND NAGAR HAVELI AND DAMAN AND DIU'],
    'SOUTH':       ['TAMIL NADU', 'KERALA', 'KARNATAKA', 'TELANGANA', 'ANDHRA PRADESH', 'PUDUCHERRY'],
    'NORTH_HILLS': ['JAMMU AND KASHMIR', 'UTTARAKHAND', 'HIMACHAL PRADESH', 'LADAKH'],
}
zone_rows = [(zone, state) for zone, states in zone_to_states.items() for state in states]
zone_mapping = spark.createDataFrame(zone_rows, ['destination_zone', 'destination_state'])
demand = demand.join(zone_mapping, on='destination_state', how='left')


# In[62]:


print('Region Define')


# In[ ]:





# In[ ]:





# In[63]:


demand = demand.withColumn('tyre_count',     coalesce(col('tyre_count'),     lit(-1)).cast('int'))
demand = demand.withColumn('vt_pricing_id',  coalesce(col('vt_pricing_id'),  lit(-1)).cast('int'))
demand = demand.withColumn('vt_id',          coalesce(col('vt_id'),          lit(-1)).cast('int'))


# In[64]:


demand = demand.withColumn(
    'app_version',
    regexp_replace(col('app_version'), '[A-Za-z .]+', '').cast('double').cast('int')
)
demand = demand.withColumn('app_version', coalesce(col('app_version'), lit(-1)))


# In[65]:


demand = demand.withColumn(
    'special_req',
    when(col('drop_points') == 'Multiple', 1).otherwise(col('special_req'))
)
demand = demand.withColumn(
    'pricing_special_req',
    when(col('drop_points') == 'Multiple', 1).otherwise(col('pricing_special_req'))
)


# In[66]:


demand = demand.withColumn('metadata_plt', coalesce(col('metadata_plt'), lit('')))


# In[ ]:





# In[ ]:





# In[ ]:





# #### DR Restriction

# In[67]:


q = f'''
select from_unixtime(cast(start_time as bigint), 'yyyy-MM-dd HH:mm:ss') + INTERVAL 5 HOURS + INTERVAL 30 MINUTES as start_time,
       consigner_code as consigner_user_code,
       status as consigner_payment_status
from data_platform.vasooli_consigner_restriction
where (
        dpyear > {year}
        or (dpyear = {year} and dpmonth > {month})
        or (dpyear = {year} and dpmonth = {month} and dpday >= {day})
        )
and deleted='false' 
'''
cons_pay = spark.sql(q)


# In[68]:


pay_temp = demand.join(cons_pay, on='consigner_user_code') \
    .filter(col('start_time') < col('demand_date')) \
    .withColumn('rn', row_number().over(Window.partitionBy('demand_id').orderBy(col('start_time').desc()))) \
    .filter(col('rn') == 1) \
    .select('demand_id', 'consigner_payment_status')
demand = demand.join(pay_temp, on='demand_id', how='left')
demand = demand.withColumn('consigner_payment_status', coalesce(col('consigner_payment_status'), lit('UNRESTRICTED')))


pay_temp_dr = demand.join(
    cons_pay.withColumnRenamed('consigner_payment_status', 'consigner_payment_status_at_dr'),
    on='consigner_user_code'
) \
    .filter(col('start_time') < col('first_dr_time')) \
    .withColumn('rn', row_number().over(Window.partitionBy('demand_id').orderBy(col('start_time').desc()))) \
    .filter(col('rn') == 1) \
    .select('demand_id', 'consigner_payment_status_at_dr')
demand = demand.join(pay_temp_dr, on='demand_id', how='left')
demand = demand.withColumn('consigner_payment_status_at_dr', coalesce(col('consigner_payment_status_at_dr'), lit('UNRESTRICTED')))


del cons_pay, pay_temp, pay_temp_dr
print('Restriction done')


# In[ ]:





# In[ ]:





# In[ ]:





# #### OpSearch FO

# In[69]:


q = '''
select demandid as demand_id, sub_operators as total_opsearch_fo, sub_more_than_20l_and_60km
from deanalytics.demand_score_op_distribution
'''
opsearch = spark.sql(q)
demand = demand.join(opsearch, on='demand_id', how='left')
del opsearch
print('OpSearch done')


# In[ ]:





# In[ ]:





# #### CCVT Lane Match FO

# In[70]:


q = '''
(
select distinct a.operator_code, d.origin_cluster, e.destination_cluster, c.vt_id, f.subscription_start_date

from
(
select distinct operator_code, from_district, to_district
from analytics.pincode_level_gps_lanes
where from_district!='' and to_district!='') a

inner join

( 
select id, code as operator_code
from trucking.ocms_operators
where deleted=0) oo
on a.operator_code=oo.operator_code

inner join

(
select operator_id, vehicle_id
from trucking.ocms_operator_vehicles
where state='LIVE' and deleted=0 and is_active=1 and service_type='GPS') oov
on oo.id=oov.operator_id

inner join

( 
select id, 
lower(case when json_extract_path_text(vehicle_attributes,'bodyType') != '' then json_extract_path_text(vehicle_attributes,'bodyType') else type end) as body_type,
case when json_extract_path_text(vehicle_attributes,'tyreCount') != '' then json_extract_path_text(vehicle_attributes,'tyreCount',true)::float else tyre_count end as tyre,
case when json_extract_path_text(vehicle_attributes,'sizeInFt') != '' then json_extract_path_text(vehicle_attributes,'sizeInFt',true)::float else size_in_ft end as size
from trucking.ocms_vehicles 
where deleted=0 ) b
on oov.vehicle_id=b.id

inner join

( 
select id as vt_id, body_type, tyre, size
from analytics.fact_demand_vt) c
on b.body_type=c.body_type and b.tyre=c.tyre and b.size=c.size

inner join

( 
select district, cluster as origin_cluster
from analytics.mp_district_cluster_mapping ) d
on a.from_district=d.district

inner join

( 
select district, cluster as destination_cluster
from analytics.mp_district_cluster_mapping ) e
on a.to_district=e.district

inner join 

( 
select user_code, created_at as subscription_start_date
from
(
select user_code, created_at, row_number() over(partition by user_code order by id) as rnk
from trucking.apollo_subscription
where deleted='false' and transaction_status='SUCCESS'
)
where rnk=1 ) f
on a.operator_code=f.user_code
)
'''
ccvt = spark.read.jdbc(url=jdbc_url, table=q, properties=properties)


# In[71]:


ccvt_join = demand.select('demand_id', 'demand_date', 'origin_cluster', 'destination_cluster', 'vt_id') \
    .join(ccvt, on=['origin_cluster', 'destination_cluster', 'vt_id'], how='inner') \
    .filter(to_timestamp('demand_date') >= to_timestamp('subscription_start_date')) \
    .groupBy('demand_id') \
    .agg(F.countDistinct('operator_code').alias('ccvt_supply_lane_match_fo'))

demand = demand.join(ccvt_join, on='demand_id', how='left')
del ccvt, ccvt_join
print('CCVT Lane Match FO done')


# In[ ]:





# In[ ]:





# In[ ]:





# #### Not Placed Reason

# In[72]:


q = '''
(
select demand_id, count(operator_code) as fo_engage
from
(
select distinct operator_code,
case when json_extract_path_text(entity,'demandId') ~ '^[0-9]+$' then json_extract_path_text(entity,'demandId')::int end as demand_id
from analytics.operator_app_events_current
where (screen_name='web_load_confirmation' and event_name='v1_load_confirmation' and target_product = 'marketplace' and event_action='view')
or (screen_name='web_rate_and_confirm' and event_name='v1_rate_and_confirm' and target_product = 'marketplace' and event_action='view')

union

select distinct operator_code, 
case when json_extract_path_text(entity,'demandId') ~ '^[0-9]+$' then json_extract_path_text(entity,'demandId')::int end as demand_id
from analytics.operator_app_view_events
where (screen_name='web_load_confirmation' and event_name='v1_load_confirmation' and target_product = 'marketplace')
or screen_name='web_rate_and_confirm' and event_name='v1_rate_and_confirm' and target_product = 'marketplace'

union

select distinct operator_code, 
case when json_extract_path_text(entity,'demandId') ~ '^[0-9]+$' then json_extract_path_text(entity,'demandId')::int end as demand_id
from analytics.operator_app_view_events_combined
where (screen_name='web_load_confirmation' and event_name='v1_load_confirmation' and target_product = 'marketplace')
or screen_name='web_rate_and_confirm' and event_name='v1_rate_and_confirm' and target_product = 'marketplace'
)
where demand_id>={}
group by 1 
)
'''.format(start_demand)
engage = spark.read.jdbc(url=jdbc_url, table=q, properties=properties)
demand = demand.join(engage, on='demand_id', how='left')
del engage


# In[ ]:





# In[73]:


q = f'''
select demand_id,
count(distinct case when status='DELIVERED' then user_code else null end)*100.0/count(distinct user_code) as noitf_delivered_fo
from deanalytics.fact_notification_history
where (
        dpyear > {year}
        or (dpyear = {year} and dpmonth > {month})
        or (dpyear = {year} and dpmonth = {month} and dpday >= {day})
        )
        and notification_type in ('Booking Automation','Bidding')
group by 1
'''
notif = spark.sql(q)
demand = demand.join(notif, on='demand_id', how='left')
del notif


# In[ ]:





# In[74]:


demand = demand.withColumn(
    'supply_confidence',
    when(
        (coalesce(col('ccvt_supply_lane_match_fo'), lit(0)) < 20) &
        (coalesce(col('total_opsearch_fo'), lit(0)) < 200),
        lit('Red')
    ).otherwise(lit('Green'))
)

hour_col    = F.hour(col('first_dr_time'))
diff_min    = (col('demand_cancel_time').cast('long') - col('first_dr_time').cast('long')) / 60
fo_engage   = coalesce(col('fo_engage'), lit(0))
notif_del   = coalesce(col('noitf_delivered_fo'), lit(0))
supply_qual = coalesce(col('sub_more_than_20l_and_60km'), lit(0))

demand = demand.withColumn(
    'non_plc_reason',
    when(col('total_consignments') > 0,                                        lit('Placed'))
    .when(col('supply_confidence') == 'Red',                                   lit('Low Supply'))
    .when(supply_qual <= 20,                                                    lit('Low Quality Supply'))
    .when((hour_col >= 21) | (hour_col < 8),                                   lit('Non Working Hours'))
    .when(diff_min <= 30,                                                       lit('Less Time to Place'))
    .when((fo_engage < 25) & (notif_del < 0.6),                                lit('Less FO Engagement [Low Del.]'))
    .when((fo_engage < 25) & (notif_del >= 0.6),                               lit('Less FO Engagement [High Del.]'))
    .when(col('pricing_special_req') == 1,                                     lit('Matching/Pricing Issue [SR]'))
    .otherwise(                                                                 lit('Matching/Pricing Issue [Non SR]'))
)


# In[75]:


print('Not Placed Reasons')


# In[ ]:





# In[76]:


# demand = demand[['demand_id', 'demand_date', 'app_version', 'dr_type',
#        'dr_flag', 'first_dr_time', 'demand_cancel_time', 'status', 'plac_type', 'placement_phase', 'non_plc_reason', 'segment_config_first',
#         'segment_config_latest', 'drop_points', 'special_req', 'special_type', 'pricing_special_req', 'pricing_special_type',
#        'metadata_plt', 'from_lat', 'from_long', 'to_lat', 'to_long',
#        'route_distance', 'shortest_route_distance', 'consigner_user_code', 'customer_type',
#        'customer_flag', 'consigner_payment_status', 'consigner_payment_status_at_dr', 'fulfilled_rank', 'consigner_type',
#        'experiment_name', 'vehicle_type_id', 'body_type', 'tyre_count',
#        'size_in_ft', 'tonnage', 'min_size', 'max_size', 'min_tonnage',
#        'max_tonnage', 'veh_tyre_type', 'vt_pricing_id', 'vt_id',
#         'total_opsearch_fo', 'opsearch_fo_10l_score', 'ccvt_supply_lane_match_fo', 
#        'consigner_freight_fare', 'actual_consigner_freight_fare', 'updated_consigner_freight_fare', 
#         'service_charge', 'shipper_discount', 'shipper_discount_reversal', 'extra_discount', 'supplyfare_before_consignment',
#        'supplyfare', 'token_forfeit', 'fo_commission',
#        'fo_commission_reversal', 'base_price', 'consigner_pnl', 'fo_pnl',
#        'pnl', 'consignment_date', 'consignment_code', 'operator_code',
#        'vehicle_id', 'trip_state', 'trip_end_time',
#        'total_consignments', 'origin', 'origin_state', 'destination',
#        'destination_state', 'origin_cluster', 'destination_cluster', 
#         'origin_id', 'destination_id', 'ncr_flag', 'demand_region', 'destination_zone',
#        'base_rate_flag', 'base_rate1', 'base_rate2', 'base_rate3',
#         'source', 'odvt_base_rate_id', 'p1', 'p2', 'p3', 'base_rate_updated_by','odvt_id']]


# In[77]:


demand = demand.select(
    'demand_id', 'demand_date', 'app_version', 'dr_type',
    'dr_flag', 'first_dr_time', 'demand_cancel_time', 'status', 'plac_type', 'placement_phase', 'non_plc_reason', 'segment_config_first',
    'segment_config_latest', 'drop_points', 'special_req', 'special_type', 'pricing_special_req', 'pricing_special_type',
    'metadata_plt', 'from_lat', 'from_long', 'to_lat', 'to_long',
    'route_distance', 'shortest_route_distance', 'consigner_user_code', 'customer_type',
    'customer_flag', 'consigner_payment_status', 'consigner_payment_status_at_dr', 'consigner_type',
    'experiment_name', 'vehicle_type_id', 'body_type', 'tyre_count',
    'size_in_ft', 'tonnage', 'min_size', 'max_size', 'min_tonnage',
    'max_tonnage', 'veh_tyre_type', 'vt_pricing_id', 'vt_id', 'total_opsearch_fo', 'ccvt_supply_lane_match_fo',
    'consigner_freight_fare', 'actual_consigner_freight_fare', 'updated_consigner_freight_fare', 
    'service_charge', 'shipper_discount', 'shipper_discount_reversal', 'extra_discount', 'supplyfare_before_consignment',
    'supplyfare', 'token_forfeit', 'fo_commission',
    'fo_commission_reversal', 'base_price', 'consigner_pnl', 'fo_pnl',
    'pnl', 'consignment_date', 'consignment_code', 'operator_code',
    'vehicle_id', 'trip_state', 'trip_end_time', 'total_consignments', 
    'origin', 'origin_state', 'origin_cluster', 'destination',
    'destination_state', 'destination_cluster', 'origin_id', 'origin_cluster_id', 'destination_id', 'destination_cluster_id',
    'ncr_flag', 'demand_region', 'destination_zone',
    'base_rate_flag', 'base_rate1', 'base_rate2', 'base_rate3')


# In[ ]:





# In[78]:


for ts_col in ['demand_date', 'first_dr_time', 'demand_cancel_time', 'consignment_date', 'trip_end_time']:
    demand = demand.withColumn(ts_col, to_timestamp(ts_col))

for f_col in ['base_rate1', 'base_rate2', 'base_rate3']:
    demand = demand.withColumn(f_col, col(f_col).cast('double'))

for fill_col in ['total_opsearch_fo', 'ccvt_supply_lane_match_fo']:
    demand = demand.withColumn(fill_col, coalesce(col(fill_col), lit(0)).cast('double'))


# In[ ]:





# In[ ]:





# In[79]:


# Writing on redshift
# select mode append/overwrite

# Writing on s3
# select append/createOrReplace at the end on code


# In[80]:


demand.writeTo("glue_catalog.deanalytics.mp_demand_details") \
    .using("iceberg") \
    .tableProperty("format-version", "2") \
    .tableProperty("write.format.default", "parquet") \
    .tableProperty("write.target-file-size-bytes", "134217728") \
    .tableProperty("write.metadata.delete-after-commit.enabled","true") \
    .tableProperty("write.metadata.previous-versions-max","1") \
    .tableProperty("write.metadata.compression-codec","gzip") \
    .tableProperty("write.parquet.compression-codec","zstd") \
    .tableProperty("location","s3://weye.analytics/marketplace/sourcedata/ep/mp_demand_details") \
    .createOrReplace()


# In[ ]:


demand.write \
  .format("io.github.spark_redshift_community.spark.redshift") \
  .option("url", "jdbc:redshift:iam://redshift-cluster-2.ct9kqx1dcuaa.ap-south-1.redshift.amazonaws.com:5439/datalake") \
  .option("dbtable", "de_analytics.mp_demand_details") \
  .option("user", "emr_spark_user") \
  .option("tempformat", "CSV GZIP") \
  .option("tempdir", "s3a://weye.analytics/de_analytics/mp_demand_details") \
  .option("forward_spark_s3_credentials", "true") \
  .mode("append") \
  .save()


