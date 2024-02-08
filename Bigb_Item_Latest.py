from pyathena import connect
#from pyathena.util import as_pandas
#from pyathena.pandas_cursor import PandasCursor
import boto3
import time
from datetime import datetime
import smtplib, ssl
import time
from datetime import datetime,timedelta
import logging

def success_email():

        body            = "Incremental load successfull on {} for bigb item flag test.".format(datetime.now())
        subject         = 'bigb_item_incremental flag test Load Report on : {}'.format(datetime.now().date())
        message         = 'Subject: {}\n\n{}'.format(subject, body)
        port            = 465
        password        = "Pass@future1"
        context         = ssl.create_default_context()
        EmailAddress    = ['sreeha.mr@akira.co.in','nevedha.a@akira.co.in','pradipta.dalal@akira.co.in']

        with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
                server.login("cashback.futurepay@gmail.com", password)
                server.sendmail('cashback.futurepay@gmail.com', EmailAddress, message)

def fail_email(e):

        body            = "Incremental load failed on {} for bigb_item_incremental flag test with error {}".format(datetime.now(),e)
        subject         = 'bigb_item_incremental flag test Load Report on : {}'.format(datetime.now().date())
        message         = 'Subject: {}\n\n{}'.format(subject, body)
        port            = 465
        password        = "Pass@future1"
        context         = ssl.create_default_context()
        EmailAddress    = ['sreeha.mr@akira.co.in','nevedha.a@akira.co.in','pradipta.dalal@akira.co.in']

        with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
                server.login("cashback.futurepay@gmail.com", password)
                server.sendmail('cashback.futurepay@gmail.com', EmailAddress, message)

try:
    now = datetime.now()
    dateVar = now.strftime("%Y%m%d%H%M")
    bigb_item_path = "s3://fg-tathastu.dp.datamarts/org/fg_commerce/bigb_item_fact_flag_interim"""
    sales_order_item_status_history_pivot_path = "s3://fg-tathastu.dp.datamarts/org/fg_commerce/sales_order_item_status_history_pivot_flag_test"""
    sales_order_item_status_history_pivot_name = '''fg_commerce.sales_order_item_status_history_pivot_flag_test'''
    bigb_item_name = """fg_commerce.bigb_item_flag_test"""
    bigb_item_withflag_path = "s3://fg-tathastu.dp.datamarts/org/fg_commerce/bigb_item_fact_withflag/dt="""+dateVar
    bigb_item_withflag_name = """stage_fg_commerce.bigb_item_withflag_"""+dateVar
	
    awssecretaccesskey   = '4t7fRasLdpIj/F9ZxaojyStdnnFTjevAfVE3NFR1'
    awsaccesskeyid       = 'AKIA4JJIVH42A63WDAG3'

    client = boto3.client('athena',
                    aws_access_key_id=awsaccesskeyid,
                    aws_secret_access_key=awssecretaccesskey,
                    region_name='ap-south-1')

    def execute_query(query_string, database, s3_output):
            print('Starting to execute : "{}" on database : "{}"'.format(query_string, database))
            query_id = client.start_query_execution(
                    QueryString=query_string,
                    QueryExecutionContext={
                    'Database': database
                    },
                    ResultConfiguration={
                    'OutputLocation': s3_output
                    }
                    )['QueryExecutionId']
            print('query_id : "{}" - Query execution started'.format(query_id))
            query_status = None
            while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
                    response = client.get_query_execution(QueryExecutionId=query_id)
                    query_status = response['QueryExecution']['Status']['State']
                    if query_status == 'FAILED' or query_status == 'CANCELLED':
                            print('Query : "{}" failed with the reason  : "{}"'.format(response['QueryExecution']['Query'],response['QueryExecution']['Status']['StateChangeReason']))
                            raise Exception('Athena query with the string "{}" failed or was cancelled'.format(query_string))
                    time.sleep(10)
            results_page = client.get_query_results(QueryExecutionId=query_id)
            results = []
            data_list = []
            print(results_page)
            print('query_id : "{}" - Query execution completed successfully'.format(query_id))
            for row in results_page['ResultSet']['Rows']:
                    row_item=row['Data'][0]['VarCharValue']
                    row_index=results_page['ResultSet']['Rows'].index(row)
                    print('_{}_ : {}    '.format(row_index, row_item))
                    data_list.append(row['Data'][0]['VarCharValue'])
            return data_list



    sales_order_item_status_history_pivot_query = """CREATE TABLE """+sales_order_item_status_history_pivot_name+"""
                WITH (
                  external_location = '"""+sales_order_item_status_history_pivot_path+"""',
                  format = 'PARQUET',
                  parquet_compression = 'GZIP')
    AS
                (
    select *,case
       when hist.cancelled_dt is not null and lower(hist.cancelled_source) like '%picker%' then 1
       when hist.cancelled_dt is not null and lower(hist.cancelled_reason) like '%stock%' then 1
       when hist.export_dt is not null and hist.invoice_dt is null and hist.cancelled_dt is not null then 0 -- 09/07/2019 handle customer cancellation
       when hist.invoice_dt is not null then 1
       when hist.export_dt is not null then 1
       when hist.cancelled_dt is not null then 0
    else 0 end as is_relevant from(
    select a.fk_sales_order_item, order_nr,
       cast(max(case when a.fk_sales_order_item_status='4' then a.created_at else null end) as timestamp) as export_dt,
       cast(max(case when a.fk_sales_order_item_status in ('19','18','8','36','28','12') then a.created_at else null end) as timestamp) as reject_dt,
       max(case when a.fk_sales_order_item_status in ('19','18','8','36','28','12') then note else null end) as reject_reason,
       max(case when a.fk_sales_order_item_status in ('19','18','8','36','28','12') then source else null end) as reject_source,
       max(case when a.fk_sales_order_item_status in ('19','18','8','36','28','12') then request_type else null end) as reject_req_type,
       cast(max(case when a.fk_sales_order_item_status in ('8','36','28','12') and a.fk_sales_order_item_status not in ('11') then a.created_at else null end) as timestamp) as return_dt,
       max(case when a.fk_sales_order_item_status in ('8','36','28','12') and a.fk_sales_order_item_status not in ('11') then note else null end) as return_reason,
       max(case when a.fk_sales_order_item_status in ('8','36','28','12') and a.fk_sales_order_item_status not in ('11') then source else null end) as return_source,
       max(case when a.fk_sales_order_item_status in ('8','36','28','12') and a.fk_sales_order_item_status not in ('11') then request_type else null end) as return_req_type,
       cast(max(case when a.fk_sales_order_item_status='25' then a.created_at else null end) as timestamp) as bob_delivery_dt,
       cast(max(case when a.fk_sales_order_item_status in ('9','13') then a.created_at else null end) as timestamp) as cancelled_dt,
       cast(max(case when a.fk_sales_order_item_status='5' then a.created_at else null end) as timestamp) as invoice_dt,
       max(case when a.fk_sales_order_item_status in ('9','13') then note else null end) as cancelled_reason,
       max(case when a.fk_sales_order_item_status in ('9','13') then source else null end) as cancelled_source,
       max(case when a.fk_sales_order_item_status in ('9','13') then request_type else null end) as cancelled_req_type
       from fg_mysql_bigb.sales_order_item_status_history a
       join fg_mysql_bigb.sales_order_item b on a.fk_sales_order_item = b.id_sales_order_item
       join fg_mysql_bigb.sales_order c on b.fk_sales_order = c.id_sales_order
       group by order_nr,a.fk_sales_order_item
                  )hist )
                  """

    flag_test_flag_table_query = """ CREATE TABLE fg_commerce.flag_test_flag_table
            WITH (
              external_location = 's3://fg-tathastu.dp.datamarts/org/fg_commerce/flag_test_flag_table/',
              format = 'PARQUET',
              parquet_compression = 'GZIP')
            AS
            (
        select * from (
with defective as(
  select distinct order_nr, '1' as is_defective_flag
  from fg_commerce.sales_order_item_status_history_pivot_flag_test
  where invoice_dt is null
  and is_relevant = 1
  and order_nr not in(select distinct order_nr from
                   fg_mysql_bigb.sales_order ord where
                   lower(ord.customer_first_name) like '%test%' or lower(ord.customer_last_name) like '%test%'
                   or lower(ord.payment_method) like '%mswipe%'
                   or ord.store_id = '381')
  ),
invoiced as(
  select distinct order_nr, '1' is_invoiced_flag
  from fg_commerce.sales_order_item_status_history_pivot_flag_test
  where invoice_dt is not null
  and is_relevant = 1
  and order_nr not in(select distinct order_nr from
                   fg_mysql_bigb.sales_order ord where
                   lower(ord.customer_first_name) like '%test%' or lower(ord.customer_last_name) like '%test%'
                   or lower(ord.payment_method) like '%mswipe%'
                   or ord.store_id = '381')

  )
select coalesce(a.order_nr, b.order_nr) as order_nr
,coalesce(b.is_invoiced_flag,'0') as is_invoiced_flag
,coalesce(a.is_defective_flag,'0') as is_defective_flag
from defective a full join invoiced b
on a.order_nr = b.order_nr
)
)"""

    flag_test_temp_joined_query = """ CREATE TABLE fg_commerce.flag_test_temp_joined WITH (
              external_location = 's3://fg-tathastu.dp.datamarts/org/fg_commerce/flag_test_temp_joined/',
              format = 'PARQUET',
              parquet_compression = 'GZIP')
            AS
            (
              select a.*,coalesce(b.is_defective_flag,'0') as is_defective_flag, coalesce(b.is_invoiced_flag,'0') as is_invoiced_flag from fg_commerce.sales_order_item_status_history_pivot_flag_test a left join fg_commerce.flag_test_flag_table b on a.order_nr = b.order_nr
              ) """

    bigb_item_query = """CREATE TABLE """+bigb_item_name+"""
                WITH (
                  external_location = '"""+bigb_item_path+"""',
                  format = 'PARQUET',
                  parquet_compression = 'GZIP')
                AS
                (
    select
    try_cast(item.id_sales_order_item as bigint) as id_sales_order_item
    ,item.sku
    ,lcat.sku_supplier_simple as article_no
    ,case when strpos(cast(sim.sku_supplier_simple as varchar), '_')=0 then sim.sku_supplier_simple
    else substr(cast(sim.sku_supplier_simple as varchar),1,(strpos(cast(sim.sku_supplier_simple as varchar), '_')-1)) end as item_article_no
    ,item.name as product_name
    --,coalesce(cb.name,'Others') as brand
    ,coalesce(lcat.ol_of_brand,'Others') as brand
    ,coalesce(lcat.LOB,'Others') as node_category
    ,coalesce(lcat.L1,'Others') L0_category
    ,coalesce(lcat.L2,'Others') L1_category
    ,coalesce(lcat.L3,'Others') L2_category
    ,coalesce(lcat.L4,'Others') L3_category
    ,typp.name as product_wt_type
    ,wt.name as product_wt_measure
    ,try_cast(item.fk_sales_order as bigint) as sales_order_id
    ,try_cast(ord.order_nr as bigint) as order_nr
    ,inv.invoice_nr
    ,ord.payment_method
    ,try_cast(item.fk_sales_order_item_status as integer) as latest_status_id
    ,stat.en_display_name as latest_status_description
    ,shipall.shipment_id as shipment_id
    ,case when try_cast(shipall.fk_sales_order_pickup_method as integer)=1 then 'in_store_pickup' when try_cast(shipall.fk_sales_order_pickup_method as integer)=2 then 'home_delivery' when try_cast(shipall.fk_sales_order_pickup_method as integer)=3 then 'express_delivery' else 'unknown' end as delivery_type
    ,try_cast(ord.created_at as timestamp) as order_dt
    ,try_cast(item.created_at as timestamp)  as item_creation_dt
    ,concat('Slot ',cast(sslot.fk_delivery_slot as varchar),' ',substr(dels.start_time,8,5),'-',substr(dels.end_time,8,5)) as delivery_slot
    ,cast(concat(cast(cal.date as varchar),' ',substr(dels.start_time,8,8)) as timestamp) expected_dt_start
    ,cast(concat(cast(cal.date as varchar),' ',substr(dels.end_time,8,8)) as timestamp) expected_dt_end
    ,date(cast(concat(cast(cal.date as varchar),' ',substr(dels.end_time,8,8)) as timestamp)) expected_dt
    ,hist.export_dt
    ,case when date_diff('minute',cast(item.created_at as timestamp),cast(concat(cast(cal.date as varchar),' ',substr(dels.start_time,8,8)) as timestamp)) < 30 then cast(item.created_at as timestamp) + interval '30' minute else cast(concat(cast(cal.date as varchar),' ',substr(dels.start_time,8,8)) as timestamp) end as to_be_picked_by
    ,try_cast(pick.in_picking as TIMESTAMP) as picking_start_dt
    ,try_cast(pick.picked     as timestamp) as picked_dt
    ,hist.invoice_dt
    ,del.created_at as load_dt
    ,hist.bob_delivery_dt
    ,try_cast(del.actual_delivery_date as timestamp) as fe_delivery_dt
    ,case when hist.bob_delivery_dt is null then try_cast(del.actual_delivery_date as TIMESTAMP) when try_cast(del.actual_delivery_date as timestamp) is null then cast(hist.bob_delivery_dt as timestamp) when cast(hist.bob_delivery_dt as timestamp)<try_cast(del.actual_delivery_date as timestamp) then cast(hist.bob_delivery_dt as timestamp) else cast(del.actual_delivery_date as TIMESTAMP) end delivery_dt
    ,hist.reject_dt
    ,hist.reject_reason
    ,hist.reject_source
    ,hist.reject_req_type
    ,hist.return_dt
    ,hist.return_reason
    ,hist.return_source
    ,hist.return_req_type
    ,hist.cancelled_dt
    ,hist.cancelled_reason
    ,hist.cancelled_source
    ,hist.cancelled_req_type
    ,case
       when hist.is_relevant=1 and hist.cancelled_dt is not null and hist.invoice_dt is null -- and lower(replace(a.cancelled_req_type,' ','')) in ('pickerapp')
       then 'picker'
       when hist.is_relevant=0 and hist.cancelled_dt is not null and hist.invoice_dt is null and lower(replace(hist.cancelled_req_type,' ','')) in ('webservice')
         then 'customer'
       when hist.cancelled_dt is not null and (lower(replace(hist.cancelled_req_type,' ','')) in ('gui','cli') or lower(replace(hist.cancelled_reason,' ','')) like '%paymentnotrealized%')
       then 'system'
       when hist.cancelled_dt is null then 'na' else 'unknown' end as cancelled_by
    ,rem.inventory as system_inventory
    ,rem.removed_inventory as removed_inventory
    ,rem.reason as product_removal_reason
    ,rem.unrestricted_qty
    ,rem.unprocessed_qty
    ,rem.reserved_quantity
    ,cast(rem.created_time as timestamp) as removal_created_at
    ,try_cast(item.unit_price as double) as product_price
    ,try_cast(item.tax_amount as double) as tax_amount
    ,try_cast(item.payment_processing_charge as double) as payment_processing_charge
    ,try_cast(item.coupon_money_value as double) as coupon_money_value
    ,try_cast(item.paid_price as double) as amount_paid
    ,try_cast(item.quantity as integer) as quantity
    ,try_cast(picko.picker_id as integer) as picker_id
    ,try_cast(picko.picker_name as integer) as picker_emp_id
    ,concat(picku.first_name,' ',picku.last_name) as picker_name
    ,try_cast(ord.fk_customer as bigint) as customer_id
    ,try_cast(stm.id_store  as bigint) as store_id
    ,stm.store_name
    ,try_cast(stm.store_code as bigint ) as store_code
    ,zo.district
    ,zo.market
    ,zo.zone
    ,zo.batch
    ,case
       when hist.cancelled_dt is not null and lower(hist.cancelled_source) like '%picker%' then 1
       when hist.cancelled_dt is not null and lower(hist.cancelled_reason) like '%stock%' then 1
       when hist.export_dt is not null and hist.invoice_dt is null and hist.cancelled_dt is not null then 0 -- 09/07/2019 handle customer cancellation
       when hist.invoice_dt is not null then 1
       when hist.export_dt is not null then 1
       when hist.cancelled_dt is not null then 0
    else 0 end as is_relevant
     -- ,hist.is_defective_flag
     ,hist.is_invoiced_flag
    ,case when try_cast(typp.id_catalog_attribute_option_global_uom_type as integer)=1 then 1
       when try_cast(typp.id_catalog_attribute_option_global_uom_type as integer)=2 then 0
       else 0 end as is_weighted
    ,case when lower(ord.customer_first_name) like '%test%' or lower(ord.customer_last_name) like '%test%' then 1 else 0 end as is_test
    ,case when lower(ord.payment_method) like '%mswipe%' then 1 else 0 end as is_Mswipe
    ,stm.status as is_active_store
    ,case when stm.store_code='1052' then 1 else 0 end as is_test_store
    ,case when lower(lcat.sku_supplier_simple) like '%membership%' then 1 else 0 end as is_membership_category

    ,concat('wk-',cast(date_format(date(cast(concat(cast(cal.date as varchar),' ',substr(dels.end_time,8,8)) as timestamp)), '%v') as varchar),':',cast(year(date(cast(concat(cast(cal.date as varchar),' ',substr(dels.end_time,8,8)) as timestamp))) as varchar)) as expected_week
s varchar),' ',substr(dels.end_time,8,8)) as timestamp))) as varchar)) as expected_week

    ,concat(cast(date_format(date(cast(concat(cast(cal.date as varchar),' ',substr(dels.end_time,8,8)) as timestamp)), '%m') as varchar),'-',cast(year(date(cast(concat(cast(cal.date as varchar),' ',substr(dels.end_time,8,8)) as timestamp))) as varchar)) as expected_month_yr

    ,date_format(date(cast(concat(cast(cal.date as varchar),' ',substr(dels.end_time,8,8)) as timestamp)),'%x') expected_yr

    ,try_cast(concat(cast(date_format(date(cast(concat(cast(cal.date as varchar),' ',substr(dels.end_time,8,8)) as timestamp)), '%x') as varchar),cast(date_format(date(cast(concat(cast(cal.date as varchar),' ',substr(dels.end_time,8,8)) as timestamp)), '%v') as varchar)) as integer) as expected_yr_wk

    ,try_cast(concat(cast(date_format(date(cast(concat(cast(cal.date as varchar),' ',substr(dels.end_time,8,8)) as timestamp)), '%x') as varchar),cast(date_format(date(cast(concat(cast(cal.date as varchar),' ',substr(dels.end_time,8,8)) as timestamp)), '%m') as varchar)) as integer) as expected_yr_month

    ,substr(cast(current_timestamp AT TIME ZONE 'Asia/Kolkata' as VARCHAR),1,19) as last_updated_at

    ,concat(concat('wk-',date_format(date(cast(ord.created_at as timestamp)),'%v')),':',cast(year(cast(ord.created_at as timestamp)) as varchar)) as order_week

    ,concat(case when cast(month(cast(ord.created_at as timestamp)) as integer)<10 then concat('0',cast(month(cast(ord.created_at as timestamp)) as varchar) ) else cast(month(cast(ord.created_at as timestamp)) as varchar)  end,'-',cast(year(cast(ord.created_at as timestamp)) as varchar))as order_month_yr

    ,case when cast(month(cast(ord.created_at as timestamp)) as integer)<10 then concat('0',cast(month(cast(ord.created_at as timestamp)) as varchar) ) else cast(month(cast(ord.created_at as timestamp)) as varchar)  end as order_month

    ,year(cast(ord.created_at as timestamp)) as order_yr
    ,b.pincode
    ,stm.store_city_name
    ,pf.platform_name


    from fg_commerce.flag_test_temp_joined hist
    left join fg_mysql_bigb.sales_order_item item on hist.fk_sales_order_item = item.id_sales_order_item
    left join fg_mysql_bigb.sales_order_item_status stat on item.fk_sales_order_item_status = stat.id_sales_order_item_status
    left join fg_mysql_bigb.sales_order ord on item.fk_sales_order = ord.id_sales_order
    left join (select * from fg_mysql_bigb.catalog_simple where status = 'active') sim on item.sku = sim.sku
    left join (select id_catalog_config,fk_catalog_brand, fk_catalog_attribute_option_global_weight_uom,fk_catalog_attribute_option_global_uom_type from fg_mysql_bigb.catalog_config where status not in ('deleted')) conf on conf.id_catalog_config = sim.fk_catalog_config
    left join fg_mysql_bigb.catalog_brand cb on cast(conf.fk_catalog_brand as varchar) = cb.id_catalog_brand
    left join fg_commerce.product_hierarchy lcat on  item.sku = lcat.CAT_SKU --sim.sku_supplier_simple=lcat.sku_supplier_simple
    left join fg_mysql_bigb.catalog_attribute_option_global_uom_type typp on cast(conf.fk_catalog_attribute_option_global_uom_type as varchar) = try_cast(typp.id_catalog_attribute_option_global_uom_type as varchar)
    left join fg_mysql_bigb.catalog_attribute_option_global_weight_uom  wt on cast(conf.fk_catalog_attribute_option_global_weight_uom as varchar)=try_cast(wt.id_catalog_attribute_option_global_weight_uom as varchar)
    left join (select order_nr, shipment_id,regexp_replace(fk_delivery_store_slot,'\.0+$','') as fk_delivery_store_slot,fk_sales_order_pickup_method from fg_mysql_bigb.sales_order_shipment where try_cast(regexp_replace(fk_delivery_store_slot,'\.0+$','') as integer) > 0) shipall on ord.order_nr = shipall.order_nr
    left join fg_mysql_bigb.sales_invoice inv on shipall.shipment_id = inv.shipment_id
    left join fg_mysql_bigb.delivery_store_slots sslot on shipall.fk_delivery_store_slot = sslot.id_delivery_store_slot
    left join fg_mysql_bigb.delivery_slots dels on sslot.fk_delivery_slot = dels.id_delivery_slot
    left join fg_mysql_bigb.delivery_slots_calender cal on sslot.fk_delivery_slot_calender = cal.id_delivery_slot_calender
    left join fg_mysql_bigb.picking_state_transition_log pick on shipall.shipment_id=pick.shipment_id
    left join fg_mysql_bigb.sales_order_item_delivery del on item.fk_sales_order_item_delivery = del.id_sales_order_item_delivery
    left join fg_mysql_bigb.item_removal_inventory rem on item.id_sales_order_item=rem.item_id
    left join fg_mysql_bigb.picker_app_order picko on ord.order_nr=picko.order_number
    left join fg_mysql_bigb.picker_app_user picku on picko.picker_id=picku.id
    left join fg_mysql_bigb.store_master stm on case when ord.store_id ='' then 0 else cast(replace(ord.store_id,'.0','') as integer) end=cast(stm.id_store as integer)
    left join fg_commerce.zone_sites_bb zo on stm.store_code=zo.store_code
    LEFT JOIN fg_mysql_bigb.picker_app_order b ON cast(ord.order_nr as varchar) = b.order_number
    left join fg_mysql_bigb.sales_rule_set_platform_lookup pf on pf.id_sales_rule_set_platform_lookup = ord.platform_bit
    /**/
    where try_cast(regexp_replace(shipall.fk_delivery_store_slot,'\.0+$','') as integer) > 0
   )"""

    odr_cols_query = """CREATE TABLE fg_commerce.odr_cols
            WITH (
              external_location = 's3://fg-tathastu.dp.datamarts/org/fg_commerce/flag_test_odr_cols/',
              format = 'PARQUET',
              parquet_compression = 'GZIP')
            AS
            (
select order_nr, cast(count(distinct(case when is_test=0 and is_test_store=0 and is_mswipe=0 and is_relevant=1 and is_weighted=1 and cancelled_dt is not null and invoice_dt is null and cancelled_by in ('picker') then sku when is_test=0 and is_test_store=0 and is_mswipe=0 and is_relevant=1 and is_weighted=0 and cancelled_dt is not null and invoice_dt is null and cancelled_by in ('picker') then cast(id_sales_order_item as varchar)
    else null end)) as double) as items_cancelled_by_picker
    ,count(distinct(case when is_relevant=1 and is_weighted=1 then sku when is_relevant=1 and is_weighted=0
then cast(id_sales_order_item as varchar) else null end))
 as items_ordered,
 count(distinct(case when is_relevant=1 and is_weighted=1 and invoice_dt is not null then sku when is_relevant=1 and is_weighted=0 and invoice_dt is not null
then cast(id_sales_order_item as varchar) else null end))
items_invoiced
    from fg_commerce.bigb_item_flag_test group by order_nr )"""

    odr_cols_joined_query = """CREATE TABLE fg_commerce.odr_cols_joined
            WITH (
              external_location = 's3://fg-tathastu.dp.datamarts/org/fg_commerce/flag_test_odr_cols_joined/',
              format = 'PARQUET',
              parquet_compression = 'GZIP')
            AS
            (
select a.*,b.items_cancelled_by_picker,b.items_ordered,b.items_invoiced from fg_commerce.bigb_item_flag_test a join fg_commerce.odr_cols b on a.order_nr=b.order_nr)
"""

    final_odr_query = """CREATE TABLE """+bigb_item_withflag_name+"""
                WITH (
                  external_location = '"""+bigb_item_withflag_path+"""',
                  format = 'PARQUET',
                  parquet_compression = 'GZIP')
                AS
            (
select *,case when is_relevant=1 and export_dt is not null
and items_invoiced < items_ordered and items_cancelled_by_picker > 0
then 1 else 0 end as is_defective_order from fg_commerce.odr_cols_joined )"""


    print('Writing to path '+sales_order_item_status_history_pivot_path)
    execute_query("""drop table if exists """+sales_order_item_status_history_pivot_name,'fg_commerce','s3://aws-athena-query-results-844583223092-ap-south-1/')
    bucket = boto3.resource('s3',aws_access_key_id=awsaccesskeyid, aws_secret_access_key=awssecretaccesskey,region_name='ap-south-1').Bucket('fg-tathastu.dp.datamarts')
    bucket.objects.filter(Prefix="org/fg_commerce/sales_order_item_status_history_pivot_flag_test/").delete()
    execute_query(sales_order_item_status_history_pivot_query,'fg_commerce','s3://aws-athena-query-results-844583223092-ap-south-1/')
    result = execute_query("""select count(*) as res from """+sales_order_item_status_history_pivot_name,'fg_commerce','s3://aws-athena-query-results-844583223092-ap-south-1/')
    print(result[1])

    print('Writing to path flag_test_flag_table')
    execute_query("""drop table if exists fg_commerce.flag_test_flag_table""",'fg_commerce','s3://aws-athena-query-results-844583223092-ap-south-1/')
    bucket = boto3.resource('s3',aws_access_key_id=awsaccesskeyid, aws_secret_access_key=awssecretaccesskey,region_name='ap-south-1').Bucket('fg-tathastu.dp.datamarts')
    bucket.objects.filter(Prefix="org/fg_commerce/flag_test_flag_table/").delete()
    execute_query(flag_test_flag_table_query,'fg_commerce','s3://aws-athena-query-results-844583223092-ap-south-1/')
    result = execute_query("""select count(*) as res from fg_commerce.flag_test_flag_table""",'fg_commerce','s3://aws-athena-query-results-844583223092-ap-south-1/')
    print(result[1])

    print('Writing to path flag_test_temp_joined')
    execute_query("""drop table if exists fg_commerce.flag_test_temp_joined""",'fg_commerce','s3://aws-athena-query-results-844583223092-ap-south-1/')
    bucket = boto3.resource('s3',aws_access_key_id=awsaccesskeyid, aws_secret_access_key=awssecretaccesskey,region_name='ap-south-1').Bucket('fg-tathastu.dp.datamarts')
    bucket.objects.filter(Prefix="org/fg_commerce/flag_test_temp_joined/").delete()
    execute_query(flag_test_temp_joined_query,'fg_commerce','s3://aws-athena-query-results-844583223092-ap-south-1/')
    result = execute_query("""select count(*) as res from fg_commerce.flag_test_temp_joined""",'fg_commerce','s3://aws-athena-query-results-844583223092-ap-south-1/')
    print(result[1])

    print('Writing to path '+bigb_item_path)
    execute_query("""drop table if exists """+bigb_item_name,'fg_commerce','s3://aws-athena-query-results-844583223092-ap-south-1/')
    bucket = boto3.resource('s3',aws_access_key_id=awsaccesskeyid, aws_secret_access_key=awssecretaccesskey,region_name='ap-south-1').Bucket('fg-tathastu.dp.datamarts')
    bucket.objects.filter(Prefix="org/fg_commerce/bigb_item_fact_flag_interim").delete()
    execute_query(bigb_item_query,'fg_commerce','s3://aws-athena-query-results-844583223092-ap-south-1/')
    result = execute_query("""select count(*) as res from """+bigb_item_name,'fg_commerce','s3://aws-athena-query-results-844583223092-ap-south-1/')
    print(result[1])

    #print("alter location for bigb_item interim table ")
    #execute_query("""alter table fg_commerce.bigb_item_flag_test set location '"""+bigb_item_path+"""'""",'fg_commerce','s3://aws-athena-query-results-844583223092-ap-south-1/')

    print('Writing to path odr_cols')
    execute_query("""drop table if exists fg_commerce.odr_cols""",'fg_commerce','s3://aws-athena-query-results-844583223092-ap-south-1/')
    bucket = boto3.resource('s3',aws_access_key_id=awsaccesskeyid, aws_secret_access_key=awssecretaccesskey,region_name='ap-south-1').Bucket('fg-tathastu.dp.datamarts')
    bucket.objects.filter(Prefix="org/fg_commerce/flag_test_odr_cols/").delete()
    execute_query(odr_cols_query,'fg_commerce','s3://aws-athena-query-results-844583223092-ap-south-1/')
    result = execute_query("""select count(*) as res from fg_commerce.odr_cols""",'fg_commerce','s3://aws-athena-query-results-844583223092-ap-south-1/')
    print(result[1])

    print('Writing to path odr_cols_joined')
    execute_query("""drop table if exists fg_commerce.odr_cols_joined""",'fg_commerce','s3://aws-athena-query-results-844583223092-ap-south-1/')
    bucket = boto3.resource('s3',aws_access_key_id=awsaccesskeyid, aws_secret_access_key=awssecretaccesskey,region_name='ap-south-1').Bucket('fg-tathastu.dp.datamarts')
    bucket.objects.filter(Prefix="org/fg_commerce/flag_test_odr_cols_joined/").delete()
    execute_query(odr_cols_joined_query,'fg_commerce','s3://aws-athena-query-results-844583223092-ap-south-1/')
    result = execute_query("""select count(*) as res from fg_commerce.odr_cols_joined""",'fg_commerce','s3://aws-athena-query-results-844583223092-ap-south-1/')
    print(result[1])

    print('Writing to path '+bigb_item_withflag_name)
    execute_query("""drop table if exists """+bigb_item_withflag_name,'fg_commerce','s3://aws-athena-query-results-844583223092-ap-south-1/')
    bucket = boto3.resource('s3',aws_access_key_id=awsaccesskeyid, aws_secret_access_key=awssecretaccesskey,region_name='ap-south-1').Bucket('fg-tathastu.dp.datamarts')
    bucket.objects.filter(Prefix="org/fg_commerce/bigb_item_fact_withflag/dt="+dateVar+"/").delete()
    execute_query(final_odr_query,'fg_commerce','s3://aws-athena-query-results-844583223092-ap-south-1/')
    result = execute_query("""select count(*) as res from """+bigb_item_withflag_name,'fg_commerce','s3://aws-athena-query-results-844583223092-ap-south-1/')
    print(result[1])

    execute_query("""alter table fg_commerce.bigb_item set location '"""+bigb_item_withflag_path+"""'""",'fg_commerce','s3://aws-athena-query-results-844583223092-ap-south-1/')
    success_email()
    #print('Writing to path final_flag_test_table')
    #execute_query("""drop table if exists fg_commerce.final_flag_test_table""",'fg_commerce','s3://aws-athena-query-results-844583223092-ap-south-1/')
    #bucket = boto3.resource('s3',aws_access_key_id=awsaccesskeyid, aws_secret_access_key=awssecretaccesskey,region_name='ap-south-1').Bucket('fg-tathastu.dp.datamarts')
    #bucket.objects.filter(Prefix="org/fg_commerce/final_odr_flag_table/").delete()
    #execute_query(final_odr_query,'fg_commerce','s3://aws-athena-query-results-844583223092-ap-south-1/')
    #result = execute_query("""select count(*) as res from fg_commerce.final_flag_test_table""",'fg_commerce','s3://aws-athena-query-results-844583223092-ap-south-1/')
    #print(result[1])

    #execute_query("""alter table fg_commerce.bigb_item_flag_test set location '"""+bigb_item_path+"""'""",'fg_commerce','s3://aws-athena-query-results-844583223092-ap-south-1/')
    #success_email()

except Exception as e:
    fail_email(e)
    print("failed")
                                                                    
