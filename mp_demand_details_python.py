import datetime as dt
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import time as tm
from pymongo import MongoClient
import sqlalchemy
import warnings
import time
import gspread
import gspread_dataframe as gd
from oauth2client.service_account import ServiceAccountCredentials
import requests
import json
import google.auth
import psycopg2
from functools import reduce
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, create_engine, select, inspect, and_, or_
import psycopg2.extensions
from sqlalchemy.sql import text
from google.cloud import bigquery
from google.oauth2 import service_account
import warnings
warnings.filterwarnings("ignore")
from airflow.models import Variable
usr = Variable.get ("user")
pasw = Variable.get ("password")

scope = ['https://spreadsheets.google.com/feeds',
'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name('/opt/airflow/dags/repo/read-g-sheet-322921-1674ac3ad251.json', scope)
gc = gspread.authorize(credentials)

galaxy=sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@redshift-cluster-2.ct9kqx1dcuaa.ap-south-1.redshift.amazonaws.com:5439/datalake".format(usr,pasw))







# In[ ]:


# Demand Query


# In[ ]:


q = '''
select min(id) as min_demand_id, max(id) as max_demand_id
from trucking.wfms_demands
where deleted='false'
and date(created + interval '5:30')>=current_date-30

'''
demand_id = pd.read_sql(q,galaxy)


# In[ ]:


start_demand = demand_id['min_demand_id'].min()


# In[ ]:


end_demand = demand_id['max_demand_id'].max()


# In[ ]:


print(start_demand, end_demand, end_demand-start_demand)


# In[ ]:





# In[ ]:





# In[ ]:


q = '''
select *
from
(
select a.id as demand_id, a.demand_date, a.commodity, a.created_by, a.assigned_to, 
a.metadata_remarks, a.metadata_remarkscategory, a.right_demand, a.app_version, a.dr_type, a.dr_flag, r.first_dr_time, a.status, 
case when a.status='EXPIRED' then coalesce(r.demand_cancel_time, a.updated_at) else null end as demand_cancel_time,
coalesce(g.drop_points,'Single') as drop_points, case when (f.ptl_comments='ptl' or a.metadata_plt='true') then 'true' else a.metadata_plt end as metadata_plt,
a.from_lat, a.from_long, a.to_lat, a.to_long, a.route_distance, a.shortest_route_distance, a.consigner_user_code, cust_type.customer_type, cust_type.customer_flag,

consigner_onboarding_date, consigner_fulfilled_demand_date, consigner_fulfilled_demand_id, consigner_type, coalesce(fulfilled_rank,0) as fulfilled_rank,


s.exp_ids, a.vehicle_type_id, b.body_type, b.tyre_count, b.size_in_ft, b.tonnage, e.min_size, e.max_size, e.min_tonnage, e.max_tonnage, 

case when lower(e.body_type)='container' and e.tyre=4 and e.min_size>=1 and e.max_size<=7 and e.min_tonnage<=1.5 then 'LCV'
when lower(e.body_type)='container' and e.tyre=4 and e.min_size>=8 and e.max_size<=9 and e.min_tonnage<=2 then 'LCV'
when lower(e.body_type)='open' and e.tyre=4 and e.min_size>=1 and e.max_size<=7 and e.min_tonnage<=1.5 then 'LCV'
when lower(e.body_type)='open' and e.tyre=4 and e.min_size>=8 and e.max_size<=9 and e.min_tonnage<=2 then 'LCV'
when lower(e.body_type)='open' and e.tyre=4 and e.min_size>=12 and e.max_size<=13 and e.min_tonnage<=2 then 'LCV'
when lower(e.body_type)='container' and e.tyre=6 and e.min_size>=27 and e.max_size<=50 and e.min_tonnage<=9 then 'SXL'
when lower(e.body_type)='container' and e.tyre=10 and e.min_size>=16 and e.max_size<=50 and e.min_tonnage>=10 and e.min_tonnage<=18 then 'MXL'
when lower(e.body_type) ilike '%%trailer%%' then 'TRAILER'
when e.tyre=4 or e.tyre=6 then '4_6'
when e.tyre=10 or e.tyre=12 or e.tyre=14 then '10_12_14'
else 'Other' end as veh_tyre_type,

e.id as vt_pricing_id, e2.id as vt_id,

coalesce(q.pricing_type, 'NA') as pricing_type, a.consigner_freight_fare, a.actual_consigner_freight_fare, a.service_charge, 

case when a.actual_consigner_freight_fare<a.consigner_freight_fare then 0 else o.shipper_discount_tmp end as shipper_discount,
case when a.actual_consigner_freight_fare<a.consigner_freight_fare then 0 else o.shipper_discount_reversal_tmp end as shipper_discount_reversal,


a.supplyfare_before_consignment, h.supplyfare, j.token_forfeit, i.fo_commission, i.fo_commission_reversal,

case when cast(a.service_charge as float)>0 then floor(((a.consigner_freight_fare/(1.0+cast(a.service_charge as float))) + 99) / 100) * 100 else a.consigner_freight_fare end as base_price,

case when a.actual_consigner_freight_fare>=a.consigner_freight_fare then a.actual_consigner_freight_fare-a.consigner_freight_fare else 0 end as extra_discount,

case when a.actual_consigner_freight_fare>=0 then a.actual_consigner_freight_fare else a.consigner_freight_fare end as updated_consigner_freight_fare,

updated_consigner_freight_fare - updated_consigner_freight_fare/(1.0+cast(a.service_charge as float))-(coalesce(shipper_discount,0) - coalesce(shipper_discount_reversal,0))-coalesce(extra_discount,0) as consigner_pnl,
updated_consigner_freight_fare/(1.0+cast(a.service_charge as float))-coalesce(coalesce(h.supplyfare,a.supplyfare_before_consignment),0)-(coalesce(i.fo_commission_reversal,0)-coalesce(i.fo_commission,0))+coalesce(j.token_forfeit,0) as fo_pnl,
consigner_pnl + fo_pnl as pnl,


h.consignment_date, h.consignment_code, h.operator_code, h.vehicle_id, h.trip_state, h2.trip_end_time, h.total_consignments,
coalesce(l.origin,'NA') as origin, split_part(l.origin,'_',2) as origin_state, l.id as origin_id, t.origin_cluster, a.origin_district_key,
coalesce(m.destination,'NA') as destination, split_part(m.destination,'_',2) as destination_state, m.id as destination_id, u.destination_cluster, a.destination_district_key, 
coalesce(n.base_rate_flag,0) as base_rate_flag, n.base_rate1, n.base_rate2, n.base_rate3


from
(
select *
from
(
select id, (created + interval '5:30') as demand_date, (updated + interval '5:30') as updated_at,
consigner_user_code, vehicle_type_id, commodity, created_by, assigned_to,
json_extract_path_text(metadata,'remarks',true) as metadata_remarks,
case when json_extract_path_text(metadata,'remarksCategory',true)!='' then json_extract_path_text(metadata,'remarksCategory',true) else expiry_remark end as metadata_remarkscategory,
json_extract_path_text(metadata,'ptl') as metadata_plt,
json_extract_path_text(metadata,'appVersion',true) as app_version,
json_extract_path_text(metadata,'searchMode',true) as dr_type,
case when lower(json_extract_path_text(metadata, 'remarks', true)) like '%%just checking price%%' 
or lower(TRIM(json_extract_path_text(metadata, 'remarks', true))) ~* '^(rfq_demand_expiry|repeated_indent_demand_expiry|sent_by_ptl_demand_expiry|data_entry_mistake_demand_expiry|change_in_truck_type/weight|just_checking_price|just_checking|just[ ]checking[ _]price|via_point[ _]not[ _]added|via[- ]point[ _]not[ _]added|out_of_scope_demand_expiry|data_entry_mistake|repeated_indent|rfq|out_of_scope|demand_expiry|not_in_focus|just|demand_modified_consignor_app|change_in_truck_type/weight_total|via_point_not_added_total|via_point_not_added|sent_by_ptl_demand_expiry_total|change_in_truck_type_cancelled_by_consigner_without_consignment_total|change_in_via_point_cancelled_by_consigner_without_consignment_total|booking_requirement_change_cancelled_by_consigner_at_loading_total|change_in_via_point_cancelled_by_consigner_without_consignment)$'
or lower(TRIM(json_extract_path_text(metadata, 'remarksCategory', true))) ~* '^(rfq_demand_expiry|repeated_indent_demand_expiry|sent_by_ptl_demand_expiry|data_entry_mistake_demand_expiry|change_in_truck_type/weight|just_checking_price|just_checking|just[ ]checking[ _]price|via_point[ _]not[ _]added|via[- ]point[ _]not[ _]added|out_of_scope_demand_expiry|data_entry_mistake|repeated_indent|rfq|out_of_scope|demand_expiry|not_in_focus|just|demand_modified_consignor_app|change_in_truck_type/weight_total|via_point_not_added_total|via_point_not_added|sent_by_ptl_demand_expiry_total|change_in_truck_type_cancelled_by_consigner_without_consignment_total|change_in_via_point_cancelled_by_consigner_without_consignment_total|booking_requirement_change_cancelled_by_consigner_at_loading_total|change_in_via_point_cancelled_by_consigner_without_consignment)$'
or lower(TRIM(json_extract_path_text(metadata, 'remarksCategory', true))) like '%%type/weight%%' 
or lower(remarks) like '%%just checking price%%' 
or lower(remarks) like '%%via point not added%%' 
or lower(TRIM(expiry_remark)) ~* '^(rfq_demand_expiry|repeated_indent_demand_expiry|sent_by_ptl_demand_expiry|data_entry_mistake_demand_expiry|change_in_truck_type/weight|just_checking_price|just_checking|just[ ]checking[ _]price|via_point[ _]not[ _]added|via[- ]point[ _]not[ _]added|out_of_scope_demand_expiry|data_entry_mistake|repeated_indent|rfq|out_of_scope|demand_expiry|not_in_focus|just|demand_modified_consignor_app|change_in_truck_type/weight_total|via_point_not_added_total|via_point_not_added|sent_by_ptl_demand_expiry_total|change_in_truck_type_cancelled_by_consigner_without_consignment_total|change_in_via_point_cancelled_by_consigner_without_consignment_total|booking_requirement_change_cancelled_by_consigner_at_loading_total|change_in_via_point_cancelled_by_consigner_without_consignment)$'
or lower(TRIM(expiry_remark)) like '%%type/weight%%' 
or lower(TRIM(remarks)) ~* '^(rfq_demand_expiry|repeated_indent_demand_expiry|sent_by_ptl_demand_expiry|data_entry_mistake_demand_expiry|change_in_truck_type/weight|just_checking_price|just_checking|just[ ]checking[ _]price|via_point[ _]not[ _]added|via[- ]point[ _]not[ _]added|out_of_scope_demand_expiry|data_entry_mistake|repeated_indent|rfq|out_of_scope|demand_expiry|not_in_focus|just|demand_modified_consignor_app|change_in_truck_type/weight_total|via_point_not_added_total|via_point_not_added|sent_by_ptl_demand_expiry_total|change_in_truck_type_cancelled_by_consigner_without_consignment_total|change_in_via_point_cancelled_by_consigner_without_consignment_total|booking_requirement_change_cancelled_by_consigner_at_loading_total|change_in_via_point_cancelled_by_consigner_without_consignment)$'
then 0 else 1 end as right_demand,  status,
case when id in (select demand_id from wfms_demand_sub_status_info where sub_status in ('DR','DR_BOOK')) then 1 else 0 end as dr_flag,
cast(json_extract_path_text(json_extract_path_text(from_address_dto,'geoLoc'),'x') as float) as from_lat,
cast(json_extract_path_text(json_extract_path_text(from_address_dto,'geoLoc'),'y') as float) as from_long,
cast(json_extract_path_text(json_extract_path_text(to_address_dto,'geoLoc'),'x') as float) as to_lat,
cast(json_extract_path_text(json_extract_path_text(to_address_dto,'geoLoc'),'y') as float) as to_long,
case when json_extract_path_text(metadata,'estimatedRouteDistance')!='' then cast(json_extract_path_text(metadata,'estimatedRouteDistance') as float) else 0 end as route_distance,
case when json_extract_path_text(metadata,'shortestRouteDistance')!='' then cast(json_extract_path_text(metadata,'shortestRouteDistance') as float) else 0 end as shortest_route_distance,

json_extract_path_text(metadata, 'predictedRateSource') as predicted_rate_source,
consigner_freight_fare, actual_consigner_freight_fare, cast(json_extract_path_text(metadata, 'experimentFreightDiscount') as float) as experiment_freight_discount,
case when json_extract_path_text(metadata,'serviceChargePct')='' then 0 when json_extract_path_text(metadata,'serviceChargePct') is null then 0 else cast(json_extract_path_text(metadata,'serviceChargePct') as float) end as service_charge,

case when json_extract_path_text(metadata,'supplyFreightFareBeforeConsignment')!='' then cast(json_extract_path_text(metadata,'supplyFreightFareBeforeConsignment') as float)
else null end as supplyfare_before_consignment, origin_district_key, destination_district_key,

min(created + interval '5:30') over(partition by consigner_user_code) as consigner_onboarding_date,
min(case when status='FULFILLED' then (created + interval '5:30') else null end) over(partition by consigner_user_code) as consigner_fulfilled_demand_date,
min(case when status='FULFILLED' then id else null end) over(partition by consigner_user_code) as consigner_fulfilled_demand_id,
case when id>consigner_fulfilled_demand_id then 'Repeat Consigner' else 'New Consigner' end as consigner_type, 
sum(case when status='FULFILLED' then 1 else 0 end) over(partition by consigner_user_code order by id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as fulfilled_rank


from trucking.wfms_demands
where deleted='false'
) 
where id>={} ) a

left join

(
select code, type as customer_type,
case when (name ilike '%%alternate%%' or name ilike '%%testing%%') then 'Testing' else 'WE_Customer' end as customer_flag
from trucking.vasooli_consigners 
where deleted='false' ) cust_type
on a.consigner_user_code=cust_type.code

left join

(
select id, size_in_ft, tonnage, tyre_count, lower(body_type) as body_type, vehicle_height, adjusted_tonnage
from trucking.wfms_vehicle_types
where deleted='false' ) b
on a.vehicle_type_id=b.id


left join

(
select id, lower(body_type) as body_type, tyre, 
min_length as min_size, max_length as max_size, 
min_weight as min_tonnage, max_weight as max_tonnage,

case when lower(body_type)='container' and tyre=4 and min_length>=1 and max_length<=7 and min_weight<=1.5 then 'LCV'
when lower(body_type)='container' and tyre=4 and min_length>=8 and max_length<=9 and min_weight<=2 then 'LCV'
when lower(body_type)='open' and tyre=4 and min_length>=1 and max_length<=7 and min_weight<=1.5 then 'LCV'
when lower(body_type)='open' and tyre=4 and min_length>=8 and max_length<=9 and min_weight<=2 then 'LCV'
when lower(body_type)='open' and tyre=4 and min_length>=12 and max_length<=13 and min_weight<=2 then 'LCV'
when lower(body_type)='container' and tyre=6 and min_length>=27 and max_length<=50 and min_weight<=9 then 'SXL'
when lower(body_type)='container' and tyre=10 and min_length>=16 and max_length<=50 and min_weight>=10 and min_weight<=18 then 'MXL'
when lower(body_type) ilike '%%trailer%%' then 'TRAILER'
when tyre=4 or tyre=6 then '4_6'
when tyre=10 or tyre=12 or tyre=14 then '10_12_14'
else 'Other' end as veh_tyre_type

from trucking.wfms_vehicle_classification
where flow='PRICING' and deleted='false') e
on b.body_type=e.body_type and b.tyre_count=e.tyre and b.size_in_ft>=e.min_size and b.size_in_ft<=e.max_size
and b.tonnage>=e.min_tonnage and b.tonnage<=e.max_tonnage

left join

(
select id, lower(body_type) as body_type, tyre, size
from analytics.fact_demand_vt ) e2
on b.body_type=e2.body_type and b.tyre_count=e2.tyre and b.size_in_ft=e2.size

left join

(
select demand_id, 'ptl' as ptl_comments
from analytics_wfms_demand_comments
where remarks ilike '%%wheelseye_ptl%%' or remarks ilike '%%wheelseye ptl%%'
and demand_id>={}
group by 1) f
on a.id=f.demand_id

left join

(
select demand_id, 'Multiple' as drop_points
from trucking.wfms_address_v2 
where demand_id>={}
group by 1  
having count(demand_id) > 2) g
on a.id=g.demand_id

left join

(
select code as consignment_code, demand_id, total_consignments, supplyfare, consignment_date, operator_code, vehicle_id, vehicle_number, trip_state, id
from
(
select code, demand_id, cast((case when json_extract_path_text(expense_data,'supplyFare')='' then '0' else json_extract_path_text(expense_data,'supplyFare') end) as float) as supplyfare,
(created + interval '5:30') as consignment_date, operator_code, vehicle_id, vehicle_number, state as trip_state, id,
count(operator_code) over(partition by demand_id) as total_consignments,
row_number() over(partition by demand_id order by updated desc) as rnk
from trucking.wfms_consignments
where deleted='false' and demand_id>={} )
where rnk=1  ) h
on a.id=h.demand_id

left join

(
select consignment_code, max(created + interval '5:30') as trip_end_time
from trucking.wfms_consignment_state_info
where state='TRIP_END' and deleted='false'
group by 1 ) h2
on h.consignment_code=h2.consignment_code

left join

(
select wce.consignment_id,
sum(case when wce.category = 'CREDIT' and wce.type in ('TRIP DISCOUNT - REVERSAL','TRIP DISCOUNT','WE DISCOUNT','WE DISCOUNT - REVERSAL')  then wce.amount else 0 end) as fo_commission_reversal,
sum(case when wce.category = 'DEBIT'  and wce.type in ('TRIP DISCOUNT - REVERSAL','TRIP DISCOUNT','WE DISCOUNT','WE DISCOUNT - REVERSAL')  then wce.amount else 0 end) as fo_commission
from
(
select consignment_id, type, amount, category
from trucking.wfms_consignment_expense
where party_type='OPERATOR' and deleted='false'
and type in ('TRIP DISCOUNT - REVERSAL','TRIP DISCOUNT','WE DISCOUNT','WE DISCOUNT - REVERSAL')
) wce
join
(
select cast(split_part(code,'-',2) as int) as code_id, demand_id
from trucking.wfms_consignments
where demand_id>={}) wc
on wce.consignment_id=wc.code_id
group by 1 ) i
on h.id=i.consignment_id

left join

(
select demand_id, sum(token_forfeit/100) as token_forfeit
from
(
(
select demand_id, cast(json_extract_path_text(metadata,'amountInPaisa') as int) as token_forfeit
from trucking.wfms_operator_demand_token
where status = 'FORFEITED'
and date(created)>=date(current_date-7) )
union
(
select demand_id, amount_in_paisa as token_forfeit
from analytics.wfms_operator_demand_token_archive
where status = 'FORFEITED'
and date(created)<date(current_date-7)
and demand_id>={} 
)
)
group by 1) j
on a.id=j.demand_id


left join

(
select id, key, district||'_'||state as origin
from analytics.fact_districts ) l
on split_part(split_part(a.origin_district_key,'-',1),'–',1)=split_part(split_part(l.key,'–',1),'-',1)

left join

(
select id, key, district||'_'||state as destination
from analytics.fact_districts ) m
on split_part(split_part(a.destination_district_key,'-',1),'–',1)=split_part(split_part(m.key,'–',1),'-',1)

left join

(
select demand_id, 1 as base_rate_flag,
max(case when operator_code='rate_card_1' then quote else null end) as base_rate1,
max(case when operator_code='rate_card_2' then quote else null end) as base_rate2,
max(case when operator_code='rate_card_3' then quote else null end) as base_rate3
from
(
select odq.demand_id, odq.operator_code, floor((odq.quote*(1.00+coalesce(dbc.int_commission,0)/100) + 99) / 100) * 100 as quote
from
(
select demand_id, operator_code, 
case when cast(json_extract_path_text(calling_details, 'quote') as float)>0 then cast(json_extract_path_text(calling_details, 'quote') as float) 
else cast(json_extract_path_text(app_response_details, 'quote') as float) end as quote
from trucking.mp_pricing_operator_demand_quotation
where deleted='false' 
and operator_code in ('rate_card_1','rate_card_2','rate_card_3') and quote is not null and demand_id>={}
and date(created)>=date(current_date-7)

union 

select demand_id, operator_code, quote
from de_analytics.operator_demand_quotation_archive
where operator_code in ('rate_card_1','rate_card_2','rate_card_3') and quote is not null and demand_id>={}
and date(created)<date(current_date-7)
) as odq

left join

(
select demand_id, max(cast(json_extract_path_text(internal_commission, 'value') as float)) as int_commission
from trucking.mp_pricing_demand_bidding_config
where deleted='false' and json_extract_path_text(internal_commission, 'value')!=''
and demand_id>={}
group by 1 ) dbc
on odq.demand_id=dbc.demand_id
)
group by 1 ) n
on a.id=n.demand_id

left join

(
select demand_id,
sum(case when category = 'CREDIT' and party_type = 'CONSIGNER' and type in ('COUPON DISCOUNT','COUPON DISCOUNT - REVERSAL','DISCOUNT COUPON') 
then amount else 0 end) as shipper_discount_tmp,
sum(case when category = 'DEBIT' and party_type = 'CONSIGNER' and type in ('COUPON DISCOUNT','COUPON DISCOUNT - REVERSAL','DISCOUNT COUPON') 
then amount else 0 end) as shipper_discount_reversal_tmp
from trucking.wfms_trip_expense 
where party_type = 'CONSIGNER' and deleted = 'false' 
and demand_id>={}
group by 1  ) o
on a.id=o.demand_id

left join

(
select demand_id, selected_confidence,
case when base_rate_source in ('DS_PRICING_V1','DS_PRICING_INTRA_V1','DS_PRICING_V2','DS_PRICING_INTRA_V2') then base_rate_source
when base_rate_source is not null then 'BR_PRICING'
else 'NA' end as pricing_type
from trucking.mp_pricing_demand_bidding_config
where deleted='false' and demand_id>={} ) q
on a.id=q.demand_id

left join

(
select demand_id, min(case when sub_status in ('DR','DR_BOOK') then sub_status_time else null end) as first_dr_time,
max(case when sub_status='DR_BOOK' then sub_status_time else null end) as last_dr_time,
min(case when sub_status='CANCELLED' then sub_status_time else null end) as demand_cancel_time
from
(
select demand_id, sub_status, (created + interval '5:30') as sub_status_time
from trucking.wfms_demand_sub_status_info 
where sub_status in ('DR','DR_BOOK', 'CANCELLED') 
and demand_id>={} 
)
group by 1  ) r
on a.id=r.demand_id

left join

(
select demand_id, experiment_ids as exp_ids
from trucking.mp_pricing_demand_bidding_config
where experiment_ids!='' and experiment_ids is not null 
and demand_id>={} ) s
on a.id=s.demand_id

left join

( 
select district, cluster as origin_cluster
from analytics.mp_district_cluster_mapping ) t
on l.origin=t.district

left join

( 
select district, cluster as destination_cluster
from analytics.mp_district_cluster_mapping ) u
on m.destination=u.district


)


'''.format(start_demand,start_demand,start_demand,start_demand,start_demand,start_demand,start_demand,start_demand,start_demand,start_demand,start_demand,start_demand,start_demand,start_demand)
demand = pd.read_sql(q,galaxy,chunksize= 100000)
demand = pd.concat(demand)


# In[ ]:


demand = demand.rename(columns={'consigner_freight_fare':'wfms_consigner_freight_fare',
                    'updated_consigner_freight_fare':'consigner_freight_fare'})


# In[ ]:


print('Demand Query')


# In[ ]:





# In[ ]:





# In[ ]:


galaxy=sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@redshift-cluster-2.ct9kqx1dcuaa.ap-south-1.redshift.amazonaws.com:5439/datalake".format(usr,pasw))


# In[ ]:


q = '''
select demand_id, created_by as dr_book_by
from
(
select demand_id, created_by, row_number() over(partition by demand_id order by id desc) as rnk
from trucking.wfms_demand_sub_status_info 
where sub_status='DR_BOOK' 
and demand_id>={}
)
where rnk=1 
'''.format(start_demand)
dr_book_by = pd.read_sql(q,galaxy)


# In[ ]:


q = '''
select demand_id, placed_cancel_reason
from analytics.mp_cancellations_view_final
where placed_cancel_reason='Operator'
and demand_id>={}
group by 1,2
'''.format(start_demand)
fo_backout = pd.read_sql(q,galaxy)


# In[ ]:


demand = pd.merge(demand, dr_book_by, how='left', on='demand_id')


# In[ ]:


demand = pd.merge(demand, fo_backout, how='left', on='demand_id')


# In[ ]:


hour = pd.to_datetime(demand['demand_date']).dt.hour

conditions = [
  (demand['total_consignments'] >= 1) & (hour >= 21) | (hour < 8),
  (demand['total_consignments'] > 1) & (demand['placed_cancel_reason'] == 'Operator'),
  (demand['total_consignments'] > 1),
  (demand['total_consignments'] > 1) & (demand['dr_book_by'] == 'ingrid_retry@wheelseye.com'),
]
choices = [
  'NIGHT_DEMAND',
  'FO_BACKOUT_DEMAND',
  'MULTIPLE_CONSIGNMENTS',
  'RETRY_DEMAND',
]

demand['plc_demand_type'] = np.select(conditions, choices, default='NORMAL_DEMAND')


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


galaxy=sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@redshift-cluster-2.ct9kqx1dcuaa.ap-south-1.redshift.amazonaws.com:5439/datalake".format(usr,pasw))


# In[ ]:


demand['exp_ids'] = demand['exp_ids'].str.replace('{','').str.replace('}','')


# In[ ]:


q = '''
select id as exp_ids, name as experiment_name
from trucking.mp_pricing_experiments
where deleted='false'
'''
exp = pd.read_sql(q,galaxy)


# In[ ]:


df = demand[demand['exp_ids'].notnull()][['demand_id','exp_ids']]


# In[ ]:


df['exp_ids'] = df['exp_ids'].astype(str).str.split(',')
df = df.explode('exp_ids')
df['exp_ids'] = df['exp_ids'].astype(int) 


# In[ ]:


df = pd.merge(df, exp, how='left', on='exp_ids')


# In[ ]:


df['experiment_name'] = df['experiment_name'].fillna('NA')


# In[ ]:


df = df.groupby('demand_id')['experiment_name'].apply(lambda x: ', '.join(x)).reset_index()


# In[ ]:


demand = pd.merge(demand, df, how='left', on='demand_id')


# In[ ]:


del exp


# In[ ]:


print('Experiment Handling')


# In[ ]:





# In[ ]:


sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1dJ9WCws5BrLw3QzaS2wwU_qMyJm671ljK-RoT2Z87dc/edit?gid=0#gid=0').worksheet('Sheet1')
old_key = pd.DataFrame(sh.get_all_records())


# In[ ]:


q = '''
select id, key, district||'_'||state as district
from analytics.fact_districts
'''
places = pd.read_sql(q,galaxy)


# In[ ]:


old_key = pd.merge(old_key, places.rename(columns={'key':'from_key', 'district':'from_district', 'id':'from_id'}), how='left', on='from_key')


# In[ ]:


old_key = pd.merge(old_key, places.rename(columns={'key':'to_key', 'district':'to_district', 'id':'to_id'}), how='left', on='to_key')


# In[ ]:


demand = pd.merge(demand, old_key, how='left', on='demand_id')


# In[ ]:


demand.loc[demand['origin_district_key'].isnull(), 'origin'] = demand['from_district']
demand.loc[demand['origin_district_key'].isnull(), 'origin_state'] = demand['from_district'].str.split('_').str[1]
demand.loc[demand['origin_district_key'].isnull(), 'origin_id'] = demand['from_id']


# In[ ]:


demand.loc[demand['destination_district_key'].isnull(), 'destination'] = demand['to_district']
demand.loc[demand['destination_district_key'].isnull(), 'destination_state'] = demand['to_district'].str.split('_').str[1]
demand.loc[demand['destination_district_key'].isnull(), 'destination_id'] = demand['to_id']


# In[ ]:


demand.loc[demand['origin_district_key'].isnull(), 'origin_district_key'] = demand['from_key']
demand.loc[demand['destination_district_key'].isnull(), 'destination_district_key'] = demand['to_key']


# In[ ]:





# In[ ]:





# In[ ]:


# Segment Config


# In[ ]:


q = '''
select demand_id, 
max(case when rnk=1 then segment_config else null end) as segment_config_first,
max(case when rnk2=1 then segment_config else null end) as segment_config_latest
from
(
select demand_id, segment_config, 
row_number() over(partition by demand_id order by created_at) as rnk,
row_number() over(partition by demand_id order by created_at desc) as rnk2
from
(
select demand_id, segment_config, min(created_at) as created_at
from trucking.ingrid_demand_operator_segment_mapping
where demand_id>={}
group by 1,2))
group by 1
'''.format(start_demand)
segment_config = pd.read_sql(q,galaxy)


# In[ ]:


demand = pd.merge(demand, segment_config, how='left', on='demand_id')


# In[ ]:


del segment_config


# In[ ]:


print('Segment Config')


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


galaxy=sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@redshift-cluster-2.ct9kqx1dcuaa.ap-south-1.redshift.amazonaws.com:5439/datalake".format(usr,pasw))


# In[ ]:


# Special Request


# In[ ]:


q = '''
select a.demand_id, a.overheight, a.overweight, a.expressdeliverytat, a.opendala, a.extrawidth, a.extraperson, a.dieselvehicle,
case when b.vehicle_height>c.height_limit then 1 else 0 end as pricing_overheight,
case when b.tonnage>c.tonnage_limit then 'true' else 'false' end as pricing_overweight,
special_req,
case when pricing_overheight>0 or pricing_overweight='true' or a.expressDeliveryTat>0 or a.openDala='true' or a.extrawidth='true' or a.dieselvehicle='true' or a.extraPerson='true' then 1 else 0 end as pricing_special_req

from
(
select id as demand_id, vehicle_type_id,
cast(json_extract_path_text(json_extract_path_text(metadata,'specialRequestBody'), 'overHeight') as float) as overheight,
json_extract_path_text(json_extract_path_text(metadata,'specialRequestBody'), 'overWeight') as overweight,
cast(json_extract_path_text(json_extract_path_text(metadata,'specialRequestBody'), 'expressDeliveryTat') as float) as expressdeliverytat,
json_extract_path_text(json_extract_path_text(metadata,'specialRequestBody'), 'openDala') as opendala,
json_extract_path_text(json_extract_path_text(metadata,'specialRequestBody'), 'extraWidth') as extrawidth,
json_extract_path_text(json_extract_path_text(metadata,'specialRequestBody'), 'extraPerson') as extraperson,
json_extract_path_text(json_extract_path_text(metadata,'specialRequestBody'), 'dieselVehicle') as dieselvehicle,
case when overHeight>0 or overWeight='true' or expressDeliveryTat>0 or openDala='true' or extrawidth='true' or dieselvehicle='true'
or extraPerson='true' then 1 else 0 end as special_req

from trucking.wfms_demands
where deleted='false' 
and special_req=1
and demand_id>={}
) a

left join

(
select id, size_in_ft, tonnage, tyre_count, lower(body_type) as body_type, vehicle_height
from trucking.wfms_vehicle_types
where deleted='false' ) b
on a.vehicle_type_id=b.id

left join

(
select lower(type) as type, tyre_count, min_size, max_size, max(max_tonnage) as tonnage_limit, max(max_height) as height_limit
from analytics.vt_version_v0
group by 1,2,3,4) c
on b.body_type=c.type and b.tyre_count=c.tyre_count and b.size_in_ft>=c.min_size and b.size_in_ft<=c.max_size

'''.format(start_demand)
special = pd.read_sql(q,galaxy,chunksize= 200000)
special = pd.concat(special)


# In[ ]:


def special_type(overheight, overweight, expressdeliverytat, opendala, extrawidth, extraperson, dieselvehicle):
  output = []
  if overheight>0:
      output.append('OVER_HEIGHT')
  if overweight=='true':
      output.append('OVER_WEIGHT')
  if expressdeliverytat>0:
      output.append('EXPRESS_DELIVERY')
  if opendala=='true':
      output.append('OPEN_DALA')
  if extrawidth=='true':
      output.append('OVER_WIDTH')
  if extraperson=='true':
      output.append('EXTRA_PERSON')
  if dieselvehicle=='true':
      output.append('DIESEL_VEHICLE')
  return(output)


# In[ ]:


special['special_type'] = special.apply(lambda x: special_type(x['overheight'], x['overweight'], x['expressdeliverytat'], x['opendala'], x['extrawidth'], x['extraperson'], x['dieselvehicle']), axis=1)


# In[ ]:


special['pricing_special_type'] = special.apply(lambda x: special_type(x['pricing_overheight'], x['pricing_overweight'], x['expressdeliverytat'], x['opendala'], x['extrawidth'], x['extraperson'], x['dieselvehicle']), axis=1)


# In[ ]:





# In[ ]:


demand = pd.merge(demand, special[special['special_req']==1][['demand_id','special_req','special_type']], how='left', on='demand_id')


# In[ ]:


demand = pd.merge(demand, special[special['pricing_special_req']==1][['demand_id','pricing_special_req','pricing_special_type']], how='left', on='demand_id')


# In[ ]:


demand['special_req'] = demand['special_req'].fillna(0)
demand['pricing_special_req'] = demand['pricing_special_req'].fillna(0)


# In[ ]:


del special


# In[ ]:


print('Special Request')


# In[ ]:





# In[ ]:





# In[ ]:


del demand['consigner_fulfilled_demand_id']


# In[ ]:


NCR = {'WEST DELHI_DELHI', 'CENTRAL DELHI_DELHI', 'NEW DELHI_DELHI',
     'NORTH WEST DELHI_DELHI', 'NORTH DELHI_DELHI', 'SOUTH DELHI_DELHI',
     'SHAHDARA_DELHI', 'SOUTH EAST DELHI_DELHI', 'EAST DELHI_DELHI',
     'SOUTH WEST DELHI_DELHI', 'NORTH EAST DELHI_DELHI',
     'FARIDABAD_HARYANA', 'GURUGRAM_HARYANA','JHAJJAR_HARYANA', 'NUH_HARYANA',
     'PALWAL_HARYANA', 'REWARI_HARYANA', 'ROHTAK_HARYANA', 'SONIPAT_HARYANA',
     'BAGHPAT_UTTAR PRADESH','GAUTAM BUDDHA NAGAR_UTTAR PRADESH', 'GHAZIABAD_UTTAR PRADESH',
      'MEERUT_UTTAR PRADESH', 'BHIWANI_HARYANA'}


# In[ ]:


def ncr_flag(origin):
  if origin in NCR:
      return('NCR')
  else:
      return('Non_NCR')

demand['ncr_flag'] = demand['origin'].apply(ncr_flag)


# In[ ]:


ROI_BASE = {
  # Gujarat
  'AHMEDABAD_GUJARAT', 'AMRELI_GUJARAT', 'ANAND_GUJARAT',
  'ARAVALLI_GUJARAT', 'BOTAD_GUJARAT', 'CHHOTA UDAIPUR_GUJARAT',
  'DADRA AND NAGAR HAVELI_DADRA AND NAGAR HAVELI AND DAMAN AND DIU',
  'DAHOD_GUJARAT', 'DAMAN_DADRA AND NAGAR HAVELI AND DAMAN AND DIU',
  'DANG_GUJARAT', 'DEVBHUMI DWARKA_GUJARAT', 'GANDHINAGAR_GUJARAT',
  'GIR SOMNATH_GUJARAT', 'JAMNAGAR_GUJARAT', 'JUNAGADH_GUJARAT',
  'KHEDA_GUJARAT', 'MAHISAGAR_GUJARAT', 'MEHSANA_GUJARAT',
  'MORBI_GUJARAT', 'NARMADA_GUJARAT', 'NAVSARI_GUJARAT',
  'PANCHMAHAL_GUJARAT', 'PORBANDAR_GUJARAT', 'RAJKOT_GUJARAT',
  'SABARKANTHA_GUJARAT', 'SURAT_GUJARAT', 'SURENDRANAGAR_GUJARAT',
  'TAPI_GUJARAT', 'VADODARA_GUJARAT', 'VALSAD_GUJARAT',

  # Maharashtra
  'AHMEDNAGAR_MAHARASHTRA', 'AURANGABAD_MAHARASHTRA',
  'BEED_MAHARASHTRA', 'JALNA_MAHARASHTRA', 'LATUR_MAHARASHTRA',
  'MUMBAI CITY_MAHARASHTRA', 'MUMBAI SUBURBAN_MAHARASHTRA',
  'NAGPUR_MAHARASHTRA', 'NANDED_MAHARASHTRA', 'NASHIK_MAHARASHTRA',
  'OSMANABAD_MAHARASHTRA', 'PALGHAR_MAHARASHTRA', 'PARBHANI_MAHARASHTRA',
  'PUNE_MAHARASHTRA', 'RAIGAD_MAHARASHTRA', 'RATNAGIRI_MAHARASHTRA',
  'SATARA_MAHARASHTRA', 'SOLAPUR_MAHARASHTRA', 'THANE_MAHARASHTRA',

  # Chhattisgarh
  'RAIGARH_CHHATTISGARH',

  # Karnataka
  'BENGALURU RURAL_KARNATAKA', 'BENGALURU URBAN_KARNATAKA',
  'CHIKKABALLAPURA_KARNATAKA', 'KOLAR_KARNATAKA',
  'RAMANAGARA_KARNATAKA', 'TUMAKURU_KARNATAKA',

  # Tamil Nadu
  'KRISHNAGIRI_TAMIL NADU',

  # Delhi (NCR districts also listed here; NCR check runs first)
  'CENTRAL DELHI_DELHI', 'EAST DELHI_DELHI', 'NEW DELHI_DELHI',
  'NORTH DELHI_DELHI', 'NORTH EAST DELHI_DELHI',
  'NORTH WEST DELHI_DELHI', 'SHAHDARA_DELHI',
  'SOUTH DELHI_DELHI', 'SOUTH EAST DELHI_DELHI',
  'SOUTH WEST DELHI_DELHI', 'WEST DELHI_DELHI',

  # Haryana (NCR overlap handled by NCR-first check)
  'BHIWANI_HARYANA', 'FARIDABAD_HARYANA', 'GURUGRAM_HARYANA',
  'JHAJJAR_HARYANA', 'NUH_HARYANA', 'PALWAL_HARYANA',
  'PANIPAT_HARYANA', 'REWARI_HARYANA', 'ROHTAK_HARYANA',
  'SONIPAT_HARYANA',

  # Uttar Pradesh (NCR overlap handled by NCR-first check)
  'BAGHPAT_UTTAR PRADESH', 'BULANDSHAHR_UTTAR PRADESH',
  'GAUTAM BUDDHA NAGAR_UTTAR PRADESH', 'GHAZIABAD_UTTAR PRADESH',
  'HAPUR_UTTAR PRADESH', 'MEERUT_UTTAR PRADESH',

  # Rajasthan
  'ALWAR_RAJASTHAN',
}

# ROI_EXTENDED = ROI_BASE + Jaipur & Hyderabad (used in ROI:2 destination check)
ROI_EXTENDED = ROI_BASE | {'JAIPUR_RAJASTHAN', 'HYDERABAD_TELANGANA'}

# Origins that trigger ROI:3 (from 2025-12-18)
ROI3_ORIGINS = {'JAIPUR_RAJASTHAN', 'HYDERABAD_TELANGANA'}


def get_demand_region(row):
  origin      = str(row['origin'])       # e.g. 'GURUGRAM_HARYANA'
  destination = str(row['destination'])  # e.g. 'MUMBAI CITY_MAHARASHTRA'
  demand_date = pd.to_datetime(row['demand_date']).date()

  # 1. NCR — full DISTRICT_STATE match on origin
  if origin in NCR:
      return 'NCR'

  # 2. ROI — full DISTRICT_STATE match on origin
  if origin in ROI_BASE:
      return 'ROI'

  # 3. ROI:2 — from 2025-12-09, origin NOT in extended set, destination IS
  if (
      demand_date >= dt.date(2025, 12, 9)
      and origin not in ROI_EXTENDED
      and destination in ROI_EXTENDED
  ):
      return 'ROI:2'

  # 4. ROI:3 — from 2025-12-18, origin is Jaipur or Hyderabad
  if demand_date >= dt.date(2025, 12, 18) and origin in ROI3_ORIGINS:
      return 'ROI:3'

  return 'OTHERS'


demand['demand_region'] = demand.apply(get_demand_region, axis=1)


# In[ ]:


demand['tyre_count'] = demand['tyre_count'].fillna(-1).astype(int)
demand['origin_id'] = demand['origin_id'].fillna(-1).astype(int)
demand['destination_id'] = demand['destination_id'].fillna(-1).astype(int)
demand['vt_pricing_id'] = demand['vt_pricing_id'].fillna(-1).astype(int)
demand['vt_id'] = demand['vt_id'].fillna(-1).astype(int)


# In[ ]:


demand['app_version'] = demand['app_version'].str.replace(r'[A-Za-z]','', regex=True).str.replace(' ','').str.replace('.','').astype(float).fillna(-1).astype(int)


# In[ ]:


demand.loc[demand['drop_points']=='Multiple', 'special_req'] = 1
demand.loc[demand['drop_points']=='Multiple', 'pricing_special_req'] = 1


# In[ ]:


demand['metadata_plt'] = demand['metadata_plt'].fillna('')


# In[ ]:





# In[ ]:


zone_to_states_dict = {
  'EAST' : ['WEST BENGAL', 'BIHAR', 'JHARKHAND', 'ODISHA', 'CHHATTISGARH'],
  'NORTHEAST' : ['SIKKIM', 'ASSAM', 'MEGHALAYA','MIZORAM','MANIPUR','TRIPURA','ARUNACHAL PRADESH','NAGALAND'],
  'NORTH' : ['PUNJAB', 'HARYANA', 'DELHI', 'UTTAR PRADESH', 'CHANDIGARH'],
  'CENTRAL' : ['MADHYA PRADESH', 'RAJASTHAN'],
  'WEST' : ['MAHARASHTRA', 'GUJARAT', 'GOA','DADRA AND NAGAR HAVELI AND DAMAN AND DIU'],
  'SOUTH' : ['TAMIL NADU', 'KERALA', 'KARNATAKA', 'TELANGANA', 'ANDHRA PRADESH', 'PUDUCHERRY'],
  'NORTH_HILLS' : ['JAMMU AND KASHMIR', 'UTTARAKHAND', 'HIMACHAL PRADESH', 'LADAKH'],
}
new_state_to_zone_list = []
for z in zone_to_states_dict:
  for s in zone_to_states_dict.get(z):
      new_state_to_zone_list.append([z,s])


# In[ ]:


zone_mapping = pd.DataFrame(new_state_to_zone_list, columns=['destination_zone','destination_state'])


# In[ ]:


demand = pd.merge(demand, zone_mapping, how='left', on='destination_state')


# In[ ]:





# In[ ]:





# In[ ]:


# DR Restriction


# In[ ]:


galaxy=sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@redshift-cluster-2.ct9kqx1dcuaa.ap-south-1.redshift.amazonaws.com:5439/datalake".format(usr,pasw))


# In[ ]:


consigner_list = list(demand['consigner_user_code'].unique())


# In[ ]:


len(consigner_list)


# In[ ]:


cons_pay_status = pd.DataFrame()
for con in range(0,len(consigner_list), 10000):
  print('Consigner Range', con, con+10000)

  q = '''
  select (TIMESTAMP 'epoch' + cast(start_time AS bigint) * interval '1 second' + Interval '5:30 hours') as start_time, 
  consigner_code as consigner_user_code, status as consigner_payment_status
  from trucking.vasooli_consigner_restriction
  where deleted='false' and consigner_code in {}
  '''.format(tuple(consigner_list[con: con+10000]))
  temp = pd.read_sql(q,galaxy,chunksize= 200000)
  temp = pd.concat(temp)

  cons_pay_status = pd.concat([cons_pay_status, temp])

  con = con+10000


# In[ ]:


pay_temp = pd.merge(demand[['demand_id','consigner_user_code','demand_date']], cons_pay_status, how='inner', on='consigner_user_code')

pay_temp = pay_temp[pay_temp['start_time']<pay_temp['demand_date']].sort_values(by='start_time', ascending=False).groupby('demand_id').head(1)[['demand_id','consigner_payment_status']]


demand = pd.merge(demand, pay_temp, how='left', on='demand_id')

demand['consigner_payment_status'] = demand['consigner_payment_status'].fillna('UNRESTRICTED')


# In[ ]:


cons_pay_status['consigner_payment_status_at_dr'] = cons_pay_status['consigner_payment_status']


# In[ ]:


pay_temp = pd.merge(demand[['demand_id','consigner_user_code','first_dr_time']], cons_pay_status, how='inner', on='consigner_user_code')

pay_temp = pay_temp[pay_temp['start_time']<pay_temp['first_dr_time']].sort_values(by='start_time', ascending=False).groupby('demand_id').head(1)[['demand_id','consigner_payment_status_at_dr']]


demand = pd.merge(demand, pay_temp, how='left', on='demand_id')

demand['consigner_payment_status_at_dr'] = demand['consigner_payment_status_at_dr'].fillna('UNRESTRICTED')


# In[ ]:


del cons_pay_status
del pay_temp


# In[ ]:


print('Restriction')


# In[ ]:





# In[ ]:





# In[ ]:


# Placement Type


# In[ ]:


galaxy=sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@redshift-cluster-2.ct9kqx1dcuaa.ap-south-1.redshift.amazonaws.com:5439/datalake".format(usr,pasw))


# In[ ]:


q = '''
select demand_id, operator_code, 
case when manual>0 then 'Manual' else 'Automation' end as plac_type,
case when matching>0 then 'Matching' else 'Bidding' end as placement_phase
from
(
select demand_id, operator_code, 
count(case when token='Manual' then 1 else null end) as manual,
count(case when token='Auto' then 1 else null end) as auto,
count(case when placement_phase='Matching' then 1 else null end) as matching,
count(case when placement_phase='Bidding' then 1 else null end) as bidding

from
(
select demand_id, operator_code, trigger_source, token, placement_phase
from
(
(
select demand_id, operator_code, trigger_source, 
case when trigger_source in ('BOOKING_AUTOMATION','TESSERACT_SERVICE') then 'Auto' else 'Manual' end as token,
case when bidding_type like '%%B%%' then 'Bidding' else 'Matching' end as placement_phase
from analytics.wfms_operator_demand_token_archive
where status in ('SUCCESS','PAYMENT_SUCCESS','REFUNDED') and demand_id>={} and date(created)<date(current_date-7)
)
union
(
select demand_id, operator_code, json_extract_path_text(metadata, 'triggerSource') AS trigger_source, 
case when trigger_source in ('BOOKING_AUTOMATION','TESSERACT_SERVICE') then 'Auto' else 'Manual' end as token,
case when json_extract_path_text(metadata, 'biddingType') like '%%B%%' then 'Bidding' else 'Matching' end as placement_phase
from trucking.wfms_operator_demand_token
where  status in ('SUCCESS','PAYMENT_SUCCESS','REFUNDED') and date(created)>=date(current_date-7)
)
)
)
group by 1,2 )
'''.format(start_demand)
plac_type = pd.read_sql(q,galaxy,chunksize= 200000)
plac_type = pd.concat(plac_type)


# In[ ]:


plac_type = pd.merge(demand[demand['total_consignments']>0][['demand_id','operator_code']], plac_type, how='left', on=['demand_id','operator_code'])


# In[ ]:


plac_type['plac_type'] = plac_type['plac_type'].fillna('Manual')
plac_type['placement_phase'] = plac_type['placement_phase'].fillna('Matching')


# In[ ]:


demand = pd.merge(demand, plac_type[['demand_id','plac_type', 'placement_phase']], how='left', on='demand_id')


# In[ ]:


del plac_type


# In[ ]:


print('Placement Type')


# In[ ]:





# In[ ]:





# In[ ]:


# Base Rate


# In[ ]:


galaxy=sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@redshift-cluster-2.ct9kqx1dcuaa.ap-south-1.redshift.amazonaws.com:5439/datalake".format(usr,pasw))


# In[ ]:


q = '''
select a.odvt_base_rate_id, c.origin, d.destination, lower(e.body_type) as body_type,
e.tyre as tyre_count, e.min_length, e.max_length, e.min_weight, e.max_weight,
a.source, a.created, a.base_rate_updated_by, a.p1, a.p2, a.p3, a.odvt_id, a.active_experiment
from
(
select id as odvt_base_rate_id, source, created, updated_by as base_rate_updated_by, p1, p2, p3, odvt_id, active_experiment
from private.mp_pricing_odvt_base_rate_v1 
where deleted='false') a

left join

(
select id, origin_key, destination_key, vehicle_classification_id
from private.mp_pricing_odvt
where deleted='false') b
on a.odvt_id=b.id

left join

(
select key, district||'_'||state as origin
from trucking.places_districts_weye) c
on b.origin_key=c.key

left join

(
select key, district||'_'||state as destination
from trucking.places_districts_weye) d
on b.destination_key=d.key

left join

(
select id, body_type, tyre, min_length, max_length, min_weight, max_weight
from trucking.wfms_vehicle_classification
where deleted='false') e
on b.vehicle_classification_id=e.id

'''
base_rate = pd.read_sql(q,galaxy)


# In[ ]:


br_map = pd.merge(demand[['demand_id','origin','destination','body_type','tyre_count','size_in_ft','tonnage']], 
                base_rate, how='inner', on=['origin', 'destination', 'body_type', 'tyre_count'])
br_map = br_map[(br_map['size_in_ft']>=br_map['min_length']) & (br_map['size_in_ft']<=br_map['max_length']) &
(br_map['tonnage']>=br_map['min_weight']) & (br_map['tonnage']<=br_map['max_weight']) ]
br_map = br_map.sort_values(by='created', ascending=False).groupby('demand_id').head(1)[['demand_id','source','base_rate_updated_by','odvt_base_rate_id','p1','p2','p3','odvt_id']]


# In[ ]:


demand = pd.merge(demand, br_map, how='left', on='demand_id')


# In[ ]:


del base_rate
del br_map


# In[ ]:


print('Base Rate')


# In[ ]:





# In[ ]:





# In[ ]:


# OpSearch FO


# In[ ]:


galaxy=sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@redshift-cluster-2.ct9kqx1dcuaa.ap-south-1.redshift.amazonaws.com:5439/datalake".format(usr,pasw))


# In[ ]:


q = '''
select distinct demandid as demand_id, sub_operators as total_opsearch_fo, sub_more_than_10l as opsearch_fo_10l_score,
sub_more_than_20l_and_60km
from analytics.demand_score_op_distribution
where demandid>={}
'''.format(start_demand)
opsearch = pd.read_sql(q,galaxy)


# In[ ]:


demand = pd.merge(demand, opsearch, how='left', on='demand_id')


# In[ ]:


del opsearch


# In[ ]:


print('OpSearch')


# In[ ]:





# In[ ]:





# In[ ]:


# CCVT Lane Match FO


# In[ ]:


galaxy=sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@redshift-cluster-2.ct9kqx1dcuaa.ap-south-1.redshift.amazonaws.com:5439/datalake".format(usr,pasw))

# In[ ]:


q = '''
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
'''
ccvt = pd.read_sql(q,galaxy)


# In[ ]:





# In[ ]:





# In[ ]:


temp_demand = demand[['demand_id','demand_date','origin_cluster','destination_cluster','vt_id']]


# In[ ]:


step = 100000
len(temp_demand)/step


# In[ ]:


ccvt_df = pd.DataFrame()
for i in range(0, len(temp_demand), step):
  print('Demand Row ', i, '-', i+step)

  temp = pd.merge(temp_demand.iloc[i: i+step] , ccvt,
          how='inner', on=['origin_cluster','destination_cluster','vt_id'])

  temp['demand_date'] = pd.to_datetime(temp['demand_date'])
  temp['subscription_start_date'] = pd.to_datetime(temp['subscription_start_date'])
  temp = temp[temp['demand_date']>=temp['subscription_start_date']]

  temp = temp.groupby('demand_id').agg(ccvt_supply_lane_match_fo=('operator_code','nunique')).reset_index()

  ccvt_df = pd.concat([ccvt_df, temp])

  del temp


# In[ ]:


demand = pd.merge(demand, ccvt_df, how='left', on='demand_id')


# In[ ]:


del ccvt
del ccvt_df


# In[ ]:


print('CCVT Lane Match FO')


# In[ ]:





# In[ ]:





# In[ ]:


# Not Placed Reason


# In[ ]:


galaxy=sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@redshift-cluster-2.ct9kqx1dcuaa.ap-south-1.redshift.amazonaws.com:5439/datalake".format(usr,pasw))


# In[ ]:


q = '''
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

'''.format(start_demand)
engage = pd.read_sql(q,galaxy)


# In[ ]:


demand = pd.merge(demand, engage, how='left', on='demand_id')


# In[ ]:





# In[ ]:


q = '''
select demand_id, count(distinct case when status='DELIVERED' then user_code else null end)*100.0/count(distinct user_code) as noitf_delivered_fo
from analytics.fact_notification_history_s3
where notification_type in ('Booking Automation','Bidding')
and demand_id>={}
group by 1
'''.format(start_demand)
delivery_perc = pd.read_sql(q,galaxy)


# In[ ]:


demand = pd.merge(demand, delivery_perc, how='left', on='demand_id')


# In[ ]:





# In[ ]:


demand['supply_confidence'] = np.where((demand['ccvt_supply_lane_match_fo'] < 20) & (demand['total_opsearch_fo'] < 200),'Red','Green')


hour = pd.to_datetime(demand['first_dr_time']).dt.hour
diff_minutes = (pd.to_datetime(demand['demand_cancel_time']) - pd.to_datetime(demand['first_dr_time'])).dt.total_seconds() / 60
fo_engage       = demand['fo_engage'].fillna(0)
notif_delivered = demand['noitf_delivered_fo'].fillna(0)
supply_quality  = demand['sub_more_than_20l_and_60km'].fillna(0)


conditions = [
  demand['total_consignments'] > 0,
  demand['supply_confidence'] == 'Red',
  supply_quality <= 20,
  (hour >= 21) | (hour < 8),
  diff_minutes <= 30,
  (fo_engage < 25) & (notif_delivered < 0.6),
  (fo_engage < 25) & (notif_delivered >= 0.6),
  demand['pricing_special_req'] == 1,
]
choices = [
  'Placed',
  'Low Supply',
  'Low Quality Supply',
  'Non Working Hours',
  'Less Time to Place',
  'Less FO Engagement [Low Del.]',
  'Less FO Engagement [High Del.]',
  'Matching/Pricing Issue [SR]',
]

demand['non_plc_reason'] = np.select(conditions, choices, default='Matching/Pricing Issue [Non SR]')


# In[ ]:


del engage
del delivery_perc


# In[ ]:


print('Not Placed Reasons')


# In[ ]:





# In[ ]:





# In[ ]:


demand = demand[['demand_id', 'demand_date', 'app_version', 'dr_type',
     'dr_flag', 'first_dr_time', 'demand_cancel_time', 'status', 'plac_type', 'placement_phase', 'plc_demand_type', 'non_plc_reason', 'segment_config_first',
      'segment_config_latest', 'drop_points', 'special_req', 'special_type', 'pricing_special_req', 'pricing_special_type',
     'metadata_plt', 'from_lat', 'from_long', 'to_lat', 'to_long',
     'route_distance', 'shortest_route_distance', 'consigner_user_code', 'customer_type',
     'customer_flag', 'consigner_payment_status', 'consigner_payment_status_at_dr', 'fulfilled_rank', 'consigner_type',
     'experiment_name', 'vehicle_type_id', 'body_type', 'tyre_count',
     'size_in_ft', 'tonnage', 'min_size', 'max_size', 'min_tonnage',
     'max_tonnage', 'veh_tyre_type', 'vt_pricing_id', 'vt_id',
      'total_opsearch_fo', 'opsearch_fo_10l_score', 'ccvt_supply_lane_match_fo', 
     'consigner_freight_fare', 'actual_consigner_freight_fare', 'wfms_consigner_freight_fare', 
      'service_charge', 'shipper_discount', 'shipper_discount_reversal', 'extra_discount', 'supplyfare_before_consignment',
     'supplyfare', 'token_forfeit', 'fo_commission',
     'fo_commission_reversal', 'base_price', 'consigner_pnl', 'fo_pnl',
     'pnl', 'consignment_date', 'consignment_code', 'operator_code',
     'vehicle_id', 'trip_state', 'trip_end_time',
     'total_consignments', 'origin', 'origin_state', 'destination',
     'destination_state', 'origin_cluster', 'destination_cluster', 
      'origin_id', 'destination_id', 'ncr_flag', 'demand_region', 'destination_zone',
     'base_rate_flag', 'base_rate1', 'base_rate2', 'base_rate3', 'pricing_type',
      'source', 'odvt_base_rate_id', 'p1', 'p2', 'p3', 'base_rate_updated_by','odvt_id']]


# In[ ]:





# In[ ]:


demand['demand_date'] = pd.to_datetime(demand['demand_date'])
demand['first_dr_time'] = pd.to_datetime(demand['first_dr_time'])
demand['demand_cancel_time'] = pd.to_datetime(demand['demand_cancel_time'])


# In[ ]:


demand['base_rate1'] = demand['base_rate1'].astype(float)
demand['base_rate2'] = demand['base_rate2'].astype(float)
demand['base_rate3'] = demand['base_rate3'].astype(float)


# In[ ]:


demand['total_opsearch_fo'] = demand['total_opsearch_fo'].fillna(0).astype(float)
demand['opsearch_fo_10l_score'] = demand['opsearch_fo_10l_score'].fillna(0).astype(float)
demand['ccvt_supply_lane_match_fo'] = demand['ccvt_supply_lane_match_fo'].fillna(0).astype(float)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


connect = psycopg2.connect(dbname='datalake',
      host='redshift-cluster-2.ct9kqx1dcuaa.ap-south-1.redshift.amazonaws.com',
      port=5439,
      user=usr,
      password=pasw)


cur = connect.cursor();
cur.execute("begin;")
cur.execute(''' 

delete from analytics.mp_demand_details
where demand_id>={}

;
'''.format(start_demand) )
cur.execute("commit;")
cur.close()




# In[ ]:





# In[ ]:


import upload_s3_to_redshift_airflow
table_name='mp_demand_details'
update_type='append_concat'
col_name=None
# overwrite_table
# append_concat


# In[ ]:


upload_s3_to_redshift_airflow.upload_to_s3(demand,table_name)
upload_s3_to_redshift_airflow.update_execute(update_type,demand,table_name,col_name)
upload_s3_to_redshift_airflow.delete_from_s3(table_name)

