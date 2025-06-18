create or replace table dev.gtm.spreetham_customer_360_v5 as

with total_users as 
(
select subscription_account_id, 
       report_as_of_dt, 
       coalesce(daily_actuals_core_user_qty, 0) + coalesce(daily_actuals_full_user_qty, 0) as total_users   
from reporting.consumption_metrics.consumption_daily_metrics cdm 
where report_as_of_dt between date_trunc('month',current_date) - interval '30 months' and current_date 
group by all 
), 

median_l60 as 
(
select a.*, 
       dateadd('day',-60,a.report_as_of_dt) as date_m60, 
       median(b.total_users) as median_l60
from total_users a 
left join total_users b on a.subscription_account_id = b.subscription_account_id 
and datediff('day', a.report_as_of_dt, b.report_as_of_dt) between -60 and 0
group by all
),  

/*
median_l30 as 
(
select a.*, 
       dateadd('day',-30,a.report_as_of_dt) as date_m30, 
       median(b.total_users) as median_l30

from total_users a 
left join total_users b on a.subscription_account_id = b.subscription_account_id 
and datediff('day', a.report_as_of_dt, b.report_as_of_dt) between -30 and 0
group by all
), 

median_l7 as 
(
select a.*, 
       dateadd('day',-7,a.report_as_of_dt) as date_m7, 
       median(b.total_users) as median_l7

from total_users a 
left join total_users b on a.subscription_account_id = b.subscription_account_id 
and datediff('day', a.report_as_of_dt, b.report_as_of_dt) between -7 and 0
group by all
), 
*/

-------------------------------------------------------------
----- Data Preparation for User Activation Percentage -------
-------------------------------------------------------------

user_activation_percentage_base as 
(
select cdm.subscription_account_id, 
       effective_subscription_account_id, 
       sfdc_account_id, 
       sfdc_account_name,  
       
       cdm.report_as_of_dt, 
       date_trunc('month',cdm.report_as_of_dt) as report_month, 
       last_day(cdm.report_as_of_dt) as last_day_of_month, 
       
       subscription_term_start_date, 
       subscription_term_end_date, 
       contract_start_date, 
       datediff('month', contract_start_date, cdm.report_as_of_dt) as months_since_contract_start, 
 
       buying_program, 
       effective_acr,
       case when effective_acr > 0 and subscription_term_end_date <> '9999-12-31' then 1 else 0 end as is_contract_flag, 
       renewal_date, 
       multiyear_flag, 
       industry, 
       physical_country, 
       sales_hier_geo, 
       sales_hier_region, 
       sales_hier_sub_region, 
       churn_indicator, 
       employees, 
       mthly_fcst_consumption_eff_amt as bcm,

       coalesce(mthly_fcst_ingest_eff_amt,0) + coalesce(mthly_fcst_us_ingest_eff_amt,0) + coalesce(mthly_fcst_eu_ingest_eff_amt,0) as ingest_bcm, 
       coalesce(mthly_fcst_ingest_eff_unit_price,0) + coalesce(mthly_fcst_us_ingest_eff_unit_price,0) + coalesce(mthly_fcst_eu_ingest_eff_unit_price,0) as        ingest_unit_price,
       coalesce(mthly_fcst_us_ccu_eff_amt,0) + coalesce(mthly_fcst_us_core_ccu_eff_amt,0) + coalesce(mthly_fcst_us_advanced_ccu_eff_amt,0) + 
       coalesce(mthly_fcst_eu_ccu_eff_amt,0) + coalesce(mthly_fcst_eu_core_ccu_eff_amt,0) + coalesce(mthly_fcst_eu_advanced_ccu_eff_amt,0)
       as ccu_bcm,
       coalesce(mthly_fcst_us_ccu_eff_unit_price,0) + coalesce(mthly_fcst_us_core_ccu_eff_unit_price,0) +         coalesce(mthly_fcst_us_advanced_ccu_eff_unit_price,0) + 
       coalesce(mthly_fcst_eu_ccu_eff_unit_price,0) + coalesce(mthly_fcst_eu_core_ccu_eff_unit_price,0) + coalesce(mthly_fcst_eu_advanced_ccu_eff_unit_price,0) as ccu_unit_price, 

       coalesce(mthly_fcst_full_user_eff_amt,0) + coalesce(mthly_fcst_core_user_eff_amt,0) as users_bcm, 
       coalesce(mthly_fcst_full_user_eff_unit_price,0) + coalesce(mthly_fcst_core_user_eff_unit_price,0) as users_unit_price, 

       first_value(buying_program) over(partition by cdm.subscription_account_id order by cdm.report_as_of_dt desc) as 
       latest_buying_program, 
       first_value(effective_acr) over(partition by cdm.subscription_account_id order by cdm.report_as_of_dt desc) as latest_effective_acr,
       first_value(mthly_fcst_consumption_eff_amt) over(partition by cdm.subscription_account_id order by cdm.report_as_of_dt desc) as 
       latest_bcm, 
       
       coalesce(daily_actuals_core_user_qty,0) + coalesce(daily_actuals_full_user_qty,0) as daily_engaged_users, 
       avg(coalesce(daily_actuals_core_user_qty,0) + coalesce(daily_actuals_full_user_qty,0)) over(partition by 
       cdm.subscription_account_id order by cdm.report_as_of_dt rows between 6 preceding and current row) AS dau_r7d,
       
       coalesce(mthly_min_commit_core_user_qty,0) + coalesce(mthly_min_commit_full_user_qty,0) as committed_users,
       md.median_l60 as rolling_60_day_median, 
       greatest(committed_users, md.median_l60) as denom_users 

       -- md1.median_l30 as rolling_30_day_median, 
       -- md2.median_l7 as rolling_7_day_median
       -- User Activation % here
from reporting.consumption_metrics.consumption_daily_metrics cdm
left join median_l60 md on cdm.subscription_account_id = md.subscription_account_id and cdm.report_as_of_dt = md.report_as_of_dt 
-- left join median_l30 md1 on cdm.subscription_account_id = md1.subscription_account_id and cdm.report_as_of_dt = md1.report_as_of_dt
-- left join median_l7 md2 on cdm.subscription_account_id = md2.subscription_account_id and cdm.report_as_of_dt = md2.report_as_of_dt
where cdm.report_as_of_dt between date_trunc('month',current_date) - interval '30 months' and current_date
), 

----------------------------------------
----- User Activation Percentage -------
----------------------------------------

user_activation_percentage as 
(
select *, case when effective_acr > 0 then dau_r7d*100/nullif(denom_users,0)
          when effective_acr = 0 then dau_r7d*100/nullif(rolling_60_day_median,0)
          end as user_activation_percentage
          -- rolling_60_day_median*1.0/nullif(rolling_30_day_median,0) as user_stickiness_ratio_median 
          
from user_activation_percentage_base
where report_as_of_dt = last_day_of_month or report_as_of_dt = (select max(report_as_of_dt) from reporting.consumption_metrics.consumption_daily_metrics)
), 

--------------------------------------
----- Product Stickiness Ratio -------
--------------------------------------
product_stickiness_ratio as 
(
select subscription_account_id, 
       month1 as report_month, 
       sum(value) as adv_used_features, 
       count(distinct metrics) as total_adv_features, 
       sum(value)*100.0/nullif(count(distinct metrics),0) as product_stickiness_ratio
from (select subscription_account_id, 
       month1, 
       prevent_issue_biz_uptime,
        agentic_automation_dummy, 
        prediction_dummy, 
        autoflows_dummy, 
        security_rx_infra_dummy, 
        security_rx_cloud_dummy, 
        vm_accounts,
        user_journey_monthly_accounts,
        engagement_intelligence_dummy, 
        streaming_video_ads_dummy,
        monthly_session_replay_accounts
from dev.ea_product_analytics.tmp_table_name_aghosh_intelligent_obs_score_v2 
where month1 between current_date - interval '30 month' and current_date 
group by all 
) 
unpivot(value for metrics in 
       (prevent_issue_biz_uptime,
        agentic_automation_dummy, 
        prediction_dummy, 
        autoflows_dummy, 
        security_rx_infra_dummy, 
        security_rx_cloud_dummy, 
        vm_accounts,
        user_journey_monthly_accounts,
        engagement_intelligence_dummy, 
        streaming_video_ads_dummy,
        monthly_session_replay_accounts
))
where month1 between current_date - interval '30 month' and current_date
group by all
), 

-------------------------------------------------------------------------------------
----- Data Preparation for Product Utilization Rate and User Stickiness Ratio -------
-------------------------------------------------------------------------------------

feature_usage_stats as (
    select date_trunc('month', date) AS month,
           product_capability,
           count(distinct subscription_account_id) as account_count,
           (select count(distinct subscription_account_id) from ea_reporting.product_analytics.fct_ccu_user_activity_by_product_capability_daily 
           where date_trunc('month', date) = date_trunc('month', a.date) and tier in ('FULL_PLATFORM', 'CORE') 
           and date BETWEEN dateadd(month, -30, current_date) and current_date) as total_accounts,
           count(distinct subscription_account_id)*100.0/(select nullif(count(distinct subscription_account_id),0) from 
           ea_reporting.product_analytics.fct_ccu_user_activity_by_product_capability_daily 
           where date_trunc('month', date) = date_trunc('month', a.date) and tier in ('FULL_PLATFORM', 'CORE') 
           and date BETWEEN dateadd(month, -30, current_date) and current_date) as usage_percentage,
from ea_reporting.product_analytics.fct_ccu_user_activity_by_product_capability_daily a
where date between dateadd(month, -30, current_date) and current_date
and tier in ('FULL_PLATFORM', 'CORE') 
group by month, product_capability 
),

features_meeting_3_percent as( 
select month, product_capability from feature_usage_stats where usage_percentage >= 3.0), 

feature_threshold_counts AS (
select month, count(distinct product_capability) as total_features_3_percent from features_meeting_3_percent group by month), 

daily_account_usage as ( 
select a.date, 
       a.subscription_account_id, 
       date_trunc('month', a.date) AS month,
       -- a.product_capability, 
       count(distinct a.user_monetization_id || a.data_center) as dau_actuals, 
       count(distinct case when f.product_capability is not null then a.product_capability end) as used_features_3_percent 
from ea_reporting.product_analytics.fct_ccu_user_activity_by_product_capability_daily a 
left join features_meeting_3_percent f on date_trunc('month',a.date) = f.month and a.product_capability = f.product_capability 
left join ea_reporting.intermediate.int_subscription_account_flags_daily as b on a.date = b.date and a.subscription_account_id = b.subscription_account_id
left join ea_reporting.product_analytics.agg_subscription_daily_metrics as c on a.date = c.report_as_of_dt and a.subscription_account_id = c.subscription_account_id 
where daily_active and okr_paid_account_flag and (is_paid_user_analysis_eligible_pa or is_qbp_pa) and a.tier in ('FULL_PLATFORM', 'CORE') and 
a.date between dateadd(month,-30,current_date) and current_date 
-- and a.subscription_account_id = '1709707' 
group by 1,2 
), 

stickiness_base as ( 
select a.subscription_account_id, a.report_as_of_dt, a.effective_acr, coalesce(b.dau_actuals,0) as dau_actuals,
       coalesce(b.used_features_3_percent, 0) AS used_features, d.total_features_3_percent
from (select subscription_account_id, report_as_of_dt, effective_acr from reporting.consumption_metrics.consumption_daily_metrics 
      where report_as_of_dt between dateadd(month,-30,current_date) and current_date group by all)a 
left join daily_account_usage b on a.report_as_of_dt = b.date and a.subscription_account_id = b.subscription_account_id
left join feature_threshold_counts d on date_trunc('month',a.report_as_of_dt) = d.month group by all), 


-------------------------------------------------------------------------------------
---------- Product Utilization Rate and User Stickiness Ratio -----------------------
-------------------------------------------------------------------------------------

user_stickiness_product_utilization as ( 
select subscription_account_id, 
       report_as_of_dt, 
       last_day(report_as_of_dt) as last_day_of_month, 
       dau_actuals, 
       used_features, 
       total_features_3_percent,
       
       -- User Stickiness Ratio
       avg(dau_actuals) over(partition by subscription_account_id order by report_as_of_dt rows between 29 preceding and current row) AS dau_r30d,
       avg(dau_actuals) over(partition by subscription_account_id order by report_as_of_dt rows between 6 preceding and current row) AS dau_r7d,
       coalesce(dau_r7d*100.0/nullif(round(dau_r30d,1),0),0) as user_stickiness_ratio,
       
       -- Product Stickiness Ratio
       avg(total_features_3_percent) over(partition by subscription_account_id order by report_as_of_dt
       rows between 6 preceding and current row) as total_features_r7d,
       avg(used_features) over(partition by subscription_account_id order by report_as_of_dt 
       rows between 6 preceding and current row) AS used_features_r7d, 
       used_features_r7d*100.0/nullif(round(total_features_r7d,1), 0) as product_utilization_rate   
from stickiness_base group by 1,2,3,4,5,6), 

monthly_ctd_updated as 
(
select subscription_account_id, 
       report_as_of_dt, 
       sfdc_account_name, 
       subscription_term_start_date, 
       mthly_fcst_consumption_eff_amt, 
       min_commit_amt, 
       ctd_consumption_amt, 
       sum(greatest(mthly_fcst_consumption_eff_amt, min_commit_amt)) over(partition by subscription_account_id, subscription_term_start_date order by report_as_of_dt rows between unbounded preceding and current row) as ctd_consumption_amt_modified 
from reporting.consumption_metrics.consumption_daily_metrics 
where -- subscription_account_id = '1863519' and report_as_of_dt >= current_date - interval '2 year' and 
(report_as_of_dt = last_day(report_as_of_dt) or report_as_of_dt = current_date - 1)
), 

ctd_updated as (
select cdm.subscription_account_id, 
       cdm.report_as_of_dt, 
       cdm.sfdc_account_name, 
       cdm.subscription_term_start_date, 
       cdm.mthly_fcst_consumption_eff_amt, 
       cdm.min_commit_amt, 
       cdm.ctd_consumption_amt, 
       mcu.ctd_consumption_amt_modified,
       case when datediff('month',cdm.subscription_term_start_date,cdm.report_as_of_dt) = 0 or 
       dense_rank() over(partition by cdm.subscription_account_id, cdm.subscription_term_start_date
       order by date_trunc('month',cdm.report_as_of_dt)) = 1
       then greatest(cdm.min_commit_amt, cdm.mthly_fcst_consumption_eff_amt) 
       else ctd_consumption_amt_modified + greatest(cdm.min_commit_amt, cdm.mthly_fcst_consumption_eff_amt) 
       end as ctd_consumption_amt_updated
from reporting.consumption_metrics.consumption_daily_metrics cdm 
left join monthly_ctd_updated mcu on mcu.subscription_account_id = cdm.subscription_account_id and 
     last_day(cdm.report_as_of_dt - interval '1 month') = mcu.report_as_of_dt and cdm.subscription_term_start_date = mcu.subscription_term_start_date 
where cdm.report_as_of_dt >= current_date - interval '30 month'), 

financial_metrics_base as 
(
select a.subscription_account_id, 
       a.report_as_of_dt,
       buying_program, 
       effective_acr, 
       effective_acr*datediff('year',a.subscription_term_start_date,subscription_term_end_date) as total_acr, 
       datediff('month',a.report_as_of_dt,current_date) as mths_till_date, 
       subscription_term_end_date, 

--------------------------------------------------------------------------
------------------ Contract Length calculation ---------------------------
--------------------------------------------------------------------------

       case when effective_acr > 0 and subscription_term_end_date <> '9999-12-31'
       then datediff('month',a.subscription_term_start_date,subscription_term_end_date) else null end as contract_length_mths, 
       case when effective_acr > 0 and subscription_term_end_date <> '9999-12-31' and datediff('month',a.subscription_term_start_date,subscription_term_end_date) between 0 and 12 then 25 
            when effective_acr > 0 and subscription_term_end_date <> '9999-12-31' and datediff('month',a.subscription_term_start_date,subscription_term_end_date) between 12 and 24 then 50
            when effective_acr > 0 and subscription_term_end_date <> '9999-12-31' and datediff('month',a.subscription_term_start_date,subscription_term_end_date) between 24 and 36 then 75
            when effective_acr > 0 and subscription_term_end_date <> '9999-12-31' and datediff('month',a.subscription_term_start_date,subscription_term_end_date) > 36 then 100 else null end as contract_length_score, 
       case when effective_acr > 0 and subscription_term_end_date <> '9999-12-31' then datediff('year',a.subscription_term_start_date,subscription_term_end_date) else null end as contract_yrs, 


       (effective_acr*datediff('year',a.subscription_term_start_date,subscription_term_end_date))
       /NULLIF(datediff('day',a.subscription_term_start_date, subscription_term_end_date),0) as day_wise_acr, 
       (effective_acr*datediff('year',a.subscription_term_start_date,subscription_term_end_date))/
       NULLIF(datediff('month',a.subscription_term_start_date, subscription_term_end_date),0) as month_wise_acr, 
       (effective_acr*datediff('year',a.subscription_term_start_date,subscription_term_end_date)) - ctd_consumption_amt_updated as amount_left, 
       a.ctd_consumption_amt, 
       a.qtd_consumption_amt, 
       a.ytd_consumption_amt, 
       mtd_consumption_eff_amt, 
       a.min_commit_amt,  

--------------------------------------------------------------------------
---------- Overage Metric Calculation ------------------------------------
--------------------------------------------------------------------------

       case when buying_program ilike 'volume%' and (mtd_consumption_eff_amt-a.min_commit_amt) > 0 
       then (mtd_consumption_eff_amt-a.min_commit_amt)*100.0/NULLIF(a.min_commit_amt,0) 
       else null end as overages, 

       case when datediff('month',a.report_as_of_dt,renewal_date) = 0 then datediff('month',a.report_as_of_dt,renewal_date)+1 else 
       datediff('month',a.report_as_of_dt,renewal_date) end as months_until_renewal, 
       datediff('days',a.report_as_of_dt,renewal_date) as days_until_renewal, 
       round(a.mthly_fcst_consumption_eff_amt,0) as bcm, 
       datediff('day',date_trunc('month',a.report_as_of_dt),last_day(a.report_as_of_dt))+1 as days_in_month, 
       (a.mthly_fcst_consumption_eff_amt)/NULLIF(datediff('day',date_trunc('month',a.report_as_of_dt),last_day(a.report_as_of_dt)),0) as day_wise_bcm, 
       b.first_report_date, 
       ctd.ctd_consumption_amt_updated, 

       case when dateadd(month,1,a.report_as_of_dt) > current_date then null 
            when dateadd(month,1,a.report_as_of_dt) <= current_date and lead(a.mthly_fcst_consumption_eff_amt,1) over(partition by a.subscription_account_id 
            order by a.report_as_of_dt) is null then 0 
            else lead(a.mthly_fcst_consumption_eff_amt,1) over(partition by a.subscription_account_id order by a.report_as_of_dt) end as M_plus_1_BCM,

       case when dateadd(month,3,a.report_as_of_dt) > current_date then null 
            when dateadd(month,3,a.report_as_of_dt) <= current_date and lead(a.mthly_fcst_consumption_eff_amt,3) over(partition by a.subscription_account_id 
            order by a.report_as_of_dt) is null then 0 
            else lead(a.mthly_fcst_consumption_eff_amt,3) over(partition by a.subscription_account_id order by a.report_as_of_dt) end as M_plus_3_BCM,

       case when dateadd(month,4,a.report_as_of_dt) > current_date then null 
            when dateadd(month,4,a.report_as_of_dt) <= current_date and lead(a.mthly_fcst_consumption_eff_amt,4) over(partition by a.subscription_account_id 
            order by a.report_as_of_dt) is null then 0 
            else lead(a.mthly_fcst_consumption_eff_amt,4) over(partition by a.subscription_account_id order by a.report_as_of_dt) end as M_plus_4_BCM,

       case when dateadd(month,5,a.report_as_of_dt) > current_date then null 
            when dateadd(month,5,a.report_as_of_dt) <= current_date and lead(a.mthly_fcst_consumption_eff_amt,5) over(partition by a.subscription_account_id 
            order by a.report_as_of_dt) is null then 0 
            else lead(a.mthly_fcst_consumption_eff_amt,5) over(partition by a.subscription_account_id order by a.report_as_of_dt) end as M_plus_5_BCM,

       case when dateadd(month,6,a.report_as_of_dt) > current_date then null 
            when dateadd(month,6,a.report_as_of_dt) <= current_date and lead(a.mthly_fcst_consumption_eff_amt,6) over(partition by a.subscription_account_id 
            order by a.report_as_of_dt) is null then 0 
            else lead(a.mthly_fcst_consumption_eff_amt,6) over(partition by a.subscription_account_id order by a.report_as_of_dt) end as M_plus_6_BCM,

       case when dateadd(month,10,a.report_as_of_dt) > current_date then null 
            when dateadd(month,10,a.report_as_of_dt) <= current_date and lead(a.mthly_fcst_consumption_eff_amt,10) over(partition by 
            a.subscription_account_id 
            order by a.report_as_of_dt) is null then 0 
            else lead(a.mthly_fcst_consumption_eff_amt,10) over(partition by a.subscription_account_id order by a.report_as_of_dt) end as M_plus_10_BCM,

       case when dateadd(month,11,a.report_as_of_dt) > current_date then null 
            when dateadd(month,11,a.report_as_of_dt) <= current_date and lead(a.mthly_fcst_consumption_eff_amt,11) over(partition by 
            a.subscription_account_id 
            order by a.report_as_of_dt) is null then 0 
            else lead(a.mthly_fcst_consumption_eff_amt,11) over(partition by a.subscription_account_id order by a.report_as_of_dt) end as M_plus_11_BCM,

       case when dateadd(month,12,a.report_as_of_dt) > current_date then null 
            when dateadd(month,12,a.report_as_of_dt) <= current_date and lead(a.mthly_fcst_consumption_eff_amt,12) over(partition by 
            a.subscription_account_id 
            order by a.report_as_of_dt) is null then 0 
            else lead(a.mthly_fcst_consumption_eff_amt,12) over(partition by a.subscription_account_id order by a.report_as_of_dt) end as M_plus_12_BCM,

       row_number() over(partition by a.subscription_account_id order by a.report_as_of_dt) as Consumed_month_number, 
       row_number() over(partition by a.subscription_account_id, a.subscription_term_start_date order by a.report_as_of_dt) as consumed_month_number_per_subscription
from reporting.consumption_metrics.consumption_daily_metrics a 
left join 
(
select subscription_account_id, min(subscription_term_start_date) as first_report_date 
from reporting.consumption_metrics.consumption_daily_metrics 
group by all
)b  
on a.subscription_account_id = b.subscription_account_id 
left join ctd_updated ctd on ctd.subscription_account_id = a.subscription_account_id and ctd.report_as_of_dt = a.report_as_of_dt
where -- a.report_as_of_dt >= current_date - interval '24 months' and 
(a.report_as_of_dt = LAST_DAY(a.report_as_of_dt) or a.report_as_of_dt = (select max(report_as_of_dt) from reporting.consumption_metrics.consumption_daily_metrics))  
-- and (buying_program ilike 'volume%' or buying_program ilike 'savings%' or buying_program ilike 'payg%')
), 

-------------------------------------------------------------
---------- Revenue Growth Calculation -----------------------
-------------------------------------------------------------
        
financial_metrics_agg as 
(
select *, 
(coalesce(bcm,0) - case when l3m_bcm=0 then l3m_bcm+1 else l3m_bcm end)*100/
NULLIF(ROUND(CASE WHEN l3m_bcm=0 THEN l3m_bcm+1 ELSE l3m_bcm END, 1), 0) AS revenue_growth  
  from (
select *, 
        avg(coalesce(bcm,0)) over(partition by subscription_account_id order by report_as_of_dt rows between 3 
        preceding and 1 preceding) as l3m_bcm, 
        avg(coalesce(day_wise_bcm,0)) over(partition by subscription_account_id order by report_as_of_dt rows between 3 
        preceding and 1 preceding) as l3m_bcm_days, 

-------------------------------------------------------------
---------- Overage Score ------------------------------------
-------------------------------------------------------------
        1.0 / nullif(LOG(10,1+round(overages,1)),0) as overage_score, 
        (M_plus_4_BCM + M_plus_5_BCM + M_plus_6_BCM)/3 as target_m_plus_6_bcm, 
        (M_plus_10_BCM + M_plus_11_BCM + M_plus_12_BCM)/3 as target_m_plus_12_bcm 
from financial_metrics_base 
)
where report_as_of_dt >= current_date - interval '30 months'
), 

financial_metrics as 
(
select *, case when effective_acr>0 and subscription_term_end_date='9999-12-31' then days_to_deplete/nullif(days_until_renewal,0) end as renewal_urgency_days, 
       case when effective_acr>0 and subscription_term_end_date='9999-12-31' then months_to_deplete/NULLIF(months_until_renewal,0) end as renewal_urgency_months from 
(
select *, 
       round(l3m_bcm/NULLIF(round(day_wise_acr,1),0),2) as day_temperature, 
       round(l3m_bcm/NULLIF(round(month_wise_acr,1),0),2) as month_temperature, 
       amount_left/NULLIF(round(l3m_bcm_days,1),0) as days_to_deplete,
       amount_left/NULLIF(round(case when l3m_bcm=0 then l3m_bcm+1 else l3m_bcm end,1),0) as months_to_deplete, 

       (coalesce(M_plus_1_BCM,0) - coalesce(bcm,0))*100/nullif(coalesce(bcm,0),0) as pct_M_plus_1_BCM, 
       (coalesce(M_plus_3_BCM,0) - coalesce(bcm,0))*100/nullif(coalesce(bcm,0),0) as pct_M_plus_3_BCM, 
       (coalesce(target_m_plus_6_bcm,0) - coalesce(bcm,0))*100/nullif(coalesce(bcm,0),0) as pct_M_plus_6_BCM, 
       (coalesce(target_m_plus_12_bcm,0) - coalesce(bcm,0))*100/nullif(coalesce(bcm,0),0) as pct_M_plus_12_BCM, 

       case when datediff('month',first_report_date,report_as_of_dt) > 3 and revenue_growth < 0
             then greatest(0, 30+revenue_growth*6) 
             -- 0-5% growth range (maps to 31-60 points)
             -- Map from 0% to 5% onto 31-60 points
             when datediff('month',first_report_date,report_as_of_dt) > 3 and revenue_growth >= 0 and revenue_growth < 5
             then 31 + revenue_growth*6 
             -- 5-15% growth range (maps to 61-80 points
             -- Map from 5% to 15% onto 61-80 points
             when datediff('month',first_report_date,report_as_of_dt) > 3 and revenue_growth >= 5 and revenue_growth < 15
             then 61 + (revenue_growth-5)*2 
             -- Map from 15% to 25% onto 81-100 points
             -- Cap at 100 points for anything above 25%
             when datediff('month',first_report_date,report_as_of_dt) > 3 and revenue_growth >= 15
             then least(100, 81+ (revenue_growth-15)*2)
             else 50 end as revenue_growth_score
from financial_metrics_agg
group by all
)), 

latest_priority as (
select c.case_id, DATE_TRUNC('month', created_date) as case_creation_month,
        coalesce(priority_at_case_creation.first_priority,c.priority) as first_priority
from ea_reporting.gtm_analytics.fct_all_support_cases c
 
    left join (select distinct case_id,   
                      first_value(old_value) over (partition by case_id order by created_date desc) as first_priority 
                from conformed.sfdc.stg_sfdc_case_history
                where field = 'Priority'
                ) priority_at_case_creation on c.case_id = priority_at_case_creation.case_id
    where c.created_date >= '2023-01-01'
   ),

accnt_level_tickets as 
( select DATE_TRUNC('month', c.created_date) as case_creation_month,c.sf_account_id,c.sf_account_name,
c.case_id, 
p.first_priority 
from ea_reporting.gtm_analytics.fct_all_support_cases c
    left join latest_priority as p on c.case_id = p.case_id
    where c.created_date >= '2023-01-01'
),

prop_P1_tickets as (
select a.sf_account_id,
a.sf_account_name, 
a.case_creation_month,
sum(case when a.first_priority = 'P1' then 1 else 0 end) as cnt_P1,
sum(case when a.first_priority = 'P2' then 1 else 0 end) as cnt_P2,
sum(case when a.first_priority is not null then 1 else 0 end) as total_assigned_p_cases,
(cnt_P1*100.00/total_assigned_p_cases) as pct_P1_cases,
((cnt_P1+cnt_P2)*100.00/total_assigned_p_cases) as pct_P1P2_cases
from accnt_level_tickets a
group by all
having total_assigned_p_cases > 1
)
--resolution rate 
,
ticket_data AS (
select DATE_TRUNC('month', created_date) AS case_creation_month,
       DATE_TRUNC('month', solved_date) AS case_solved_month,
       sf_account_id,
       case_id,
       CASE WHEN DATE_TRUNC('month', created_date) = DATE_TRUNC('month', solved_date) THEN 1 ELSE 0 END AS resolved_in_same_month_flag
    FROM ea_reporting.gtm_analytics.fct_all_support_cases
    WHERE created_date >= '2023-01-01' OR solved_date >= '2023-01-01'
),

-- Open Ticket Volume
open_ticket_volume as (
SELECT case_creation_month,
       sf_account_id,
       COUNT(DISTINCT case_id) AS opened_case_volume
FROM ticket_data
GROUP BY case_creation_month, sf_account_id),

closed_ticket_volume as 
(
-- Resolved Ticket Volume
SELECT case_solved_month, sf_account_id, count(distinct case_id) AS resolved_case_volume
FROM ticket_data
WHERE resolved_in_same_month_flag = 1 -- Only count tickets closed in the same month
GROUP BY case_solved_month, sf_account_id
),

resolution_rate as
(
SELECT
        o.case_creation_month,
        o.sf_account_id,
        o.opened_case_volume AS open_tickets,
        c.resolved_case_volume AS solved_tickets,
        CASE
            WHEN o.opened_case_volume = 0 THEN 1.0 -- No tickets opened, considered 100% "resolved" for this metric's purpose
            WHEN o.opened_case_volume > 0 THEN
                COALESCE(
                    CAST(c.resolved_case_volume AS FLOAT) / CAST(o.opened_case_volume AS FLOAT),
                    0.0  -- If opened > 0 but resolved_case_volume is NULL (no solved tickets), rate is 0.0
                )
            ELSE
                NULL -- For opened_case_volume IS NULL or negative
        END AS resolution_rate
    FROM
        open_ticket_volume o
    LEFT JOIN
        closed_ticket_volume c
        ON o.sf_account_id = c.sf_account_id AND o.case_creation_month = c.case_solved_month
),

avg_csat_score as (
select sf_account_id, 
        sf_account_name,
        DATE_TRUNC('month',solved_date) as solved_month,
        avg(csat_score) as avg_csat_score,
        count(case_id) as csat_counts,
from ea_reporting.gtm_analytics.fct_all_support_cases
where solved_date >= '2023-01-01' and csat_score is not null
group by all
),

fttr_calculated AS (
    SELECT
        DATE_TRUNC('month', created_date) AS case_creation_month,
        sf_account_id,
        sf_account_name,
        case_id,
        created_date,        
        first_response_met,
        solved_date,
        timediff('minute',
             created_date,
             COALESCE(
                 first_response_met,
                 (DATE_TRUNC('day', solved_date) + INTERVAL '1 day'), -- Start of the day AFTER solved_date
                 (DATE_TRUNC('day', current_date()) + INTERVAL '1 day') -- Start of the day AFTER current_date
             )
            ) / 60.00 AS fttr_hrs
    FROM
        ea_reporting.gtm_analytics.fct_all_support_cases
    WHERE
        created_date >= '2023-01-01'
    and fttr_hrs>=0
       
),
pctile_FTTR as 
(
SELECT
    fc.case_creation_month,
    fc.sf_account_id,
    fc.sf_account_name,
    fc.case_id,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY fc.fttr_hrs) OVER (PARTITION BY fc.case_creation_month, fc.sf_account_name) AS FTTR_25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY fc.fttr_hrs) OVER (PARTITION BY fc.case_creation_month, fc.sf_account_name) AS FTTR_50,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY fc.fttr_hrs) OVER (PARTITION BY fc.case_creation_month, fc.sf_account_name) AS FTTR_75
FROM
    fttr_calculated fc
),

acct_fttr as 
( 
select case_creation_month,
    sf_account_id,
    sf_account_name,
    max(FTTR_25) as mthly_fttr_25,
    max(FTTR_50) as mthly_fttr_50,
    max(FTTR_75) as mthly_fttr_75,
    
    from pctile_FTTR
    group by all
    ),

support_ticket_metrics as 
(
select r.sf_account_id,r.case_creation_month , r.resolution_rate, r.open_tickets, p.pct_P1_cases, pct_P1P2_cases, a.avg_csat_score, f.mthly_fttr_50
from resolution_rate r
left join prop_P1_tickets p
on r.sf_account_id = p.sf_account_id and r.case_creation_month = p.case_creation_month
left join avg_csat_score a
on a.sf_account_id = r.sf_account_id and a.solved_month = r.case_creation_month
left join acct_fttr f
on f.sf_account_id = r.sf_account_id and f.case_creation_month = r.case_creation_month
),

churn_risk as (
    SELECT
        "subscription_rpm_account_id",
        DATE_TRUNC('month', "close_week") as churn_month,
        "churn_risk_score"
    FROM (
        SELECT
            "subscription_rpm_account_id",
            "close_week",
            "churn_risk_score",
            ROW_NUMBER() OVER(
                PARTITION BY "subscription_rpm_account_id", DATE_TRUNC('month', "close_week")
                ORDER BY "close_week" DESC
            ) as rn
        FROM sagemaker_production.v3_ptc."weekly_ptc_d_summaries"
    ) ranked_summaries
    WHERE rn = 1
),

sentiment as
(
    SELECT
        "subscription_rpm_id",
        DATE_TRUNC('month', "date") as sentiment_month,
        "sentiment_score",
        "engagement_score"
    FROM (
        SELECT
            "subscription_rpm_id",
            "date",
            "sentiment_score",
            "engagement_score",
            ROW_NUMBER() OVER(
                PARTITION BY "subscription_rpm_id", DATE_TRUNC('month', "date")
                ORDER BY "date" DESC
            ) as rn
        FROM sagemaker_production.v3_ptc."weekly_ptc_d_engagement_sentiment_scores"
    ) ranked_scores
    WHERE rn = 1
),

base_invoices AS (
    SELECT
        t.RPM_ID,
        t.DUE_DATE,
        t.CLOSED AS CLOSED_DATE,
        t.ZUORA_AMOUNT_WITHOUT_TAX AS AMOUNT,
        t.STATUS,
        t.TRANSACTION_ID,
        -- DATEDIFF(DAY, t.DUE_DATE, COALESCE(t.CLOSED, CURRENT_DATE)) AS DAYS_PAST_DUE
    FROM
        nr_silver_datalake_prod.FPA_NETSUITE.TRANSACTIONS t
    JOIN
        nr_silver_datalake_prod.FPA_NETSUITE.CUSTOMERS c
        ON t.ENTITY_ID = c.CUSTOMER_ID
    WHERE
        t.DUE_DATE IS NOT NULL
        and t.DUE_DATE >= current_date - interval '30 months'

        
        --AND t.DUE_DATE >= '2023-04-01'
        --AND t.DUE_DATE <= '2025-12-31'
        AND t._FIVETRAN_DELETED = false
        AND c._FIVETRAN_DELETED = false
),

months_for_accounts AS (
    SELECT DISTINCT
        RPM_ID,
        LAST_DAY(DUE_DATE) as month_end_date
    FROM base_invoices
),

prior_month_agg AS (
    SELECT
        m.RPM_ID,
        m.month_end_date,
        MAX(CASE
                WHEN bi.CLOSED_DATE IS NULL
                THEN DATEDIFF(DAY, bi.DUE_DATE, m.month_end_date)
                ELSE 0
            END) AS max_days_past_due_before,
        SUM(CASE
                WHEN bi.CLOSED_DATE IS NULL
                THEN bi.AMOUNT
                ELSE 0
            END) AS sum_amount_due_before,
        SUM(CASE
                WHEN bi.CLOSED_DATE IS NULL
                THEN 1
                ELSE 0
            END) AS count_open_invoices_before_month_end
    FROM months_for_accounts m
    JOIN base_invoices bi
        ON m.RPM_ID = bi.RPM_ID
    WHERE bi.DUE_DATE <= m.month_end_date
    GROUP BY
        m.RPM_ID,
        m.month_end_date
),

ranked_current_invoices AS (
    SELECT
        RPM_ID,
        DUE_DATE,
        AMOUNT,
        STATUS,
        TRANSACTION_ID,
        ROW_NUMBER() OVER(
            PARTITION BY RPM_ID,LAST_DAY(DUE_DATE)
            ORDER BY DUE_DATE DESC, TRANSACTION_ID DESC
        ) as rn
    FROM base_invoices
),

recent_current_invoice AS (
    SELECT
        RPM_ID,
        DUE_DATE AS recent_invoice_due_date,
        AMOUNT AS recent_invoice_due_amt,
        STATUS AS recent_invoice_due_status
    FROM ranked_current_invoices
    WHERE rn = 1
),

invoice_data as 
(
SELECT
    m.RPM_ID AS account_id,
    date_trunc('month', m.month_end_date) AS invoice_month,
    COALESCE(pma.max_days_past_due_before, 0) AS maximum_days_past_due_before_month,
    COALESCE(pma.sum_amount_due_before, 0) AS sum_total_amount_due_before_month,
    COALESCE(pma.count_open_invoices_before_month_end, 0) AS number_of_open_invoices_due_before_month_end,
    rci.recent_invoice_due_date,
    rci.recent_invoice_due_amt,
    rci.recent_invoice_due_status
FROM months_for_accounts m
LEFT JOIN prior_month_agg pma
    ON m.RPM_ID = pma.RPM_ID
    AND m.month_end_date = pma.month_end_date
LEFT JOIN recent_current_invoice rci
    ON m.RPM_ID = rci.RPM_ID
    AND m.month_end_date = last_day(rci.recent_invoice_due_date)
ORDER BY
    m.RPM_ID,
    m.month_end_date
),

competitor_agents as 
(
select account_id, 
       date, 
       AppDynamics, 
       DataDog, 
       Dynatrace, 
       ElasticAgent, 
       Grafana, 
       LogicMonitor, 
       OpenTelemetry, 
       SignalFX,
       Splunk, 
       SumoLogic, 
       ZeroAgent
    from growth_marketing_reporting.competitor_agents.competitor_agents_us

    union all 

select account_id, 
       date, 
       AppDynamics, 
       DataDog, 
       Dynatrace, 
       ElasticAgent, 
       Grafana, 
       LogicMonitor, 
       OpenTelemetry, 
       SignalFX,
       Splunk, 
       SumoLogic, 
       ZeroAgent
    from growth_marketing_reporting.competitor_agents.competitor_agents_eu
), 

competitors_agg as 
(
select account_id, 
       date_trunc('month',date) as reporting_month, 
       max(AppDynamics) as AppDynamics, 
       max(DataDog) as DataDog, 
       max(Dynatrace) as Dynatrace, 
       max(ElasticAgent) as ElasticAgent, 
       max(Grafana) as Grafana, 
       max(LogicMonitor) as LogicMonitor, 
       max(OpenTelemetry) as OpenTelemetry, 
       max(SignalFX) as SignalFX,
       max(Splunk) as Splunk, 
       max(SumoLogic) as SumoLogic, 
       max(ZeroAgent) as ZeroAgent
from competitor_agents
-- where account_id = '1709707'
group by all 
-- order by reporting_month desc;
) 

base as 
(
select a.subscription_account_id, 
       a.effective_subscription_account_id, 
       a.sfdc_account_id, 
       a.sfdc_account_name,
       
       a.report_as_of_dt, 
       a.report_month, 
       
       a.buying_program, 
       a.latest_buying_program, 
       
       a.effective_acr,
       a.is_contract_flag, 
       a.latest_effective_acr,

       a.bcm, 
       a.latest_bcm,
       a.ingest_bcm, 
       a.ingest_unit_price,
       a.ccu_bcm,
       a.ccu_unit_price, 
       a.users_bcm, 
       a.users_unit_price, 
       
       a.subscription_term_start_date, 
       a.subscription_term_end_date, 
       a.renewal_date, 
       a.multiyear_flag, 
       d.total_acr,
       a.industry, 
       a.physical_country, 
       a.sales_hier_geo, 
       a.sales_hier_region, 
       a.sales_hier_sub_region,
       a.employees,
       a.churn_indicator, 

       a.contract_start_date, 
       a.months_since_contract_start, 

       a.daily_engaged_users,
       a.dau_r7d,
       a.committed_users, 
       a.rolling_60_day_median, 
       a.denom_users, 
       cast(a.user_activation_percentage as decimal(18,2)) as user_activation_percentage, 
       -- a.user_stickiness_ratio_median,
       
       b.adv_used_features, 
       b.total_adv_features, 
       cast(b.product_stickiness_ratio as decimal(18,2)) as product_stickiness_ratio, 
       
       c.dau_actuals, 
       c.used_features, 
       c.total_features_3_percent, 
       c.dau_r30d,
       cast(c.user_stickiness_ratio as decimal(18,2)) as user_stickiness_ratio, 
       
       c.total_features_r7d,
       c.used_features_r7d,
       cast(c.product_utilization_rate as decimal(18,2)) as product_utilization_rate, 

       d.l3m_bcm, 
       d.day_wise_acr, 
       d.month_wise_acr, 
       d.amount_left, 
       d.ctd_consumption_amt_updated, 
       
       d.day_temperature, 
       d.month_temperature, 
       d.days_to_deplete,
       d.months_to_deplete, 
       d.days_until_renewal, 
       d.months_until_renewal, 
       d.first_report_date, 
       cast(d.renewal_urgency_months as decimal(18,2)) as renewal_urgency_months,
       cast(d.renewal_urgency_days as decimal(18,2)) as renewal_urgency_days,

       cast(d.overages as decimal(18,2)) as overages, 
       cast(d.overage_score as decimal(18,2)) as overage_score, 
       cast(d.revenue_growth as decimal(18,2)) as revenue_growth, 
       cast(d.revenue_growth_score as decimal(18,2)) as revenue_growth_score, 
       
       cast(d.contract_length_mths as decimal(18,2)) as contract_length_mths, 
       cast(d.contract_length_score as decimal(18,2)) as contract_length_score,
       
       d.ctd_consumption_amt, 
       d.qtd_consumption_amt, 
       d.ytd_consumption_amt, 
       d.mtd_consumption_eff_amt, 
       d.min_commit_amt, 
       
       d.M_plus_1_BCM, 
       d.M_plus_3_BCM,
       d.M_plus_4_BCM,
       d.M_plus_5_BCM,
       d.M_plus_6_BCM,
       d.M_plus_10_BCM,
       d.M_plus_11_BCM,
       d.M_plus_12_BCM, 

       d.target_m_plus_6_bcm, 
       d.target_m_plus_12_bcm, 

       d.pct_M_plus_1_BCM, 
       d.pct_M_plus_3_BCM,
       d.pct_M_plus_6_BCM, 
       d.pct_M_plus_12_BCM, 

       d.Consumed_month_number, 
       d.Consumed_month_number_per_subscription, 
       datediff('month',a.subscription_term_start_date,a.report_as_of_dt) as mth_diff, 
       d.mths_till_date, 
       
       CAST(s.pct_P1_cases AS DECIMAL(18,2)) AS pct_P1_cases,
       CAST(s.pct_P1P2_cases AS DECIMAL(18,2)) AS pct_P1P2_cases,
       CAST(s.resolution_rate AS DECIMAL(18,2)) AS resolution_rate,
       CAST(s.open_tickets AS DECIMAL(18,2)) AS open_tickets,
       CAST(s.avg_csat_score AS DECIMAL(18,2)) AS avg_csat_score,
       CAST(s.mthly_fttr_50 AS DECIMAL(18,2)) AS mthly_fttr_50,
       CAST(i."churn_risk_score" AS DECIMAL(18,2)) AS churn_risk_score,
       CAST(j."sentiment_score" AS DECIMAL(18,2)) AS sentiment_score,
       CAST(j."engagement_score" AS DECIMAL(18,2)) AS engagement_score,
       CAST(k.maximum_days_past_due_before_month AS DECIMAL(18,2)) AS maximum_days_past_due,
       CAST(k.sum_total_amount_due_before_month AS DECIMAL(18,2)) AS sum_total_amount_due,
       CAST(k.number_of_open_invoices_due_before_month_end AS DECIMAL(18,2)) AS number_of_open_invoices_due_before_month_end, 

       AppDynamics, 
       DataDog, 
       Dynatrace, 
       ElasticAgent, 
       Grafana, 
       LogicMonitor, 
       OpenTelemetry, 
       SignalFX,
       Splunk, 
       SumoLogic, 
       ZeroAgent, 
       AppDynamics + DataDog + Dynatrace + ElasticAgent + Grafana + LogicMonitor + OpenTelemetry +  
       SignalFX + Splunk + SumoLogic + ZeroAgent as total_competitor_agents 
       
from user_activation_percentage a 
left join product_stickiness_ratio b on a.subscription_account_id = b.subscription_account_id 
and a.report_month = b.report_month 
left join user_stickiness_product_utilization c on a.subscription_account_id = c.subscription_account_id 
and a.report_as_of_dt = c.report_as_of_dt  
left join financial_metrics d on a.subscription_account_id = d.subscription_account_id 
and a.report_as_of_dt = d.report_as_of_dt  
left join support_ticket_metrics s on a.sfdc_account_id = s.sf_account_id and a.report_month = s.case_creation_month
left join churn_risk i on a.subscription_account_id = i."subscription_rpm_account_id" and a.report_month = i.churn_month
left join sentiment j on a.subscription_account_id = j."subscription_rpm_id" and a.report_month = j.sentiment_month
left join invoice_data k on a.subscription_account_id = k.account_id and a.report_month = k.invoice_month
left join competitors_agg ca on a.subscription_account_id = ca.account_id and a.report_month = ca.reporting_month
group by all
) 

select *
from base;

