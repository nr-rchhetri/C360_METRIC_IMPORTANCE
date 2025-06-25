with churn_indicator AS

(WITH not_churn_last_date AS 

(select effective_subscription_account_id, 
MAX (REPORT_AS_OF_DT) as not_churn_last_date_stamp
FROM reporting.consumption_metrics.consumption_daily_metrics
where churn_indicator = 'N'
AND subscription_account_id = EFFECTIVE_SUBSCRIPTION_ACCOUNT_ID
group by effective_subscription_account_id)

,gen_vals_max_date AS (-- this is to take the latest value of sales_channel and exclusion_flag when churn_indicator was N
select rt.effective_subscription_account_id, rt.account_sales_channel, rt.exclusion_flag
FROM reporting.consumption_metrics.consumption_daily_metrics rt
inner join not_churn_last_date
ON not_churn_last_date.effective_subscription_account_id = rt.effective_subscription_account_id
AND not_churn_last_date.not_churn_last_date_stamp = rt.report_as_of_dt)

,overall_max_date AS 

(select max(report_as_of_dt) as max_date FROM 
reporting.consumption_metrics.consumption_daily_metrics)

SELECT DISTINCT reporting.consumption_metrics.consumption_daily_metrics.EFFECTIVE_SUBSCRIPTION_ACCOUNT_ID,
gen_vals_max_date.account_sales_channel,
not_churn_last_date.not_churn_last_date_stamp  AS churn_date, NEXT_DAY(churn_date, 'SUN') - 7 AS churn_close_week

FROM reporting.consumption_metrics.consumption_daily_metrics

inner join not_churn_last_date
ON not_churn_last_date.effective_subscription_account_id = reporting.consumption_metrics.consumption_daily_metrics.effective_subscription_account_id

left join gen_vals_max_date 
ON gen_vals_max_date.effective_subscription_account_id = 
reporting.consumption_metrics.consumption_daily_metrics.EFFECTIVE_SUBSCRIPTION_ACCOUNT_ID

cross join overall_max_date

WHERE not_churn_last_date.not_churn_last_date_stamp < overall_max_date.max_date
AND churn_indicator = 'N'
AND gen_vals_max_date.account_sales_channel IN ('SALES_LED','Cancelled Account') -- cancelled account included bcoz some accounts randomly change to 'Cancelled' in the end
AND gen_vals_max_date.exclusion_flag = FALSE
), 

close_week_data AS (
    SELECT REPORT_AS_OF_DT AS date, NEXT_DAY(REPORT_AS_OF_DT, 'SUN') - 7 AS close_week, -- this essentially truncates date to last Sunday 
    * exclude (REPORT_AS_OF_DT) 
    FROM reporting.consumption_metrics.consumption_daily_metrics
    WHERE 1 =1 
    AND REPORT_AS_OF_DT >= '2023-08-01'  -- only from August 2023 onwards (change to October if performance is not good, missing values?)
    AND account_sales_channel = 'SALES_LED' -- only Sales_Led accounts
    --AND paid_account_flag = TRUE (not available in this table)
    AND exclusion_flag = FALSE
    AND subscription_account_id = EFFECTIVE_SUBSCRIPTION_ACCOUNT_ID
   
),

downgrades as (select distinct EFFECTIVE_SUBSCRIPTION_ACCOUNT_ID, max(close_week) as downgrade_close_week, 
-- we have to take the maximum value of close_week as an account can have multiple downgrades across a time horizon.. 
--SDM_EFF_ACR_CURRENT_DATE , FW_NET_ACR_CHANGE, pct_acr_change

from 
(SELECT distinct REPORT_AS_OF_DT , date_trunc('week',REPORT_AS_OF_DT) as week1 ,
NEXT_DAY(REPORT_AS_OF_DT, 'SUN') - 7 as close_week,
EFFECTIVE_SUBSCRIPTION_ACCOUNT_ID ,  
SDM_EFF_ACR_CURRENT_DATE , FW_NET_ACR_CHANGE , 
div0(FW_NET_ACR_CHANGE,SDM_EFF_ACR_CURRENT_DATE) as pct_acr_change,
ROW_NUMBER() OVER(PARTITION BY EFFECTIVE_SUBSCRIPTION_ACCOUNT_ID , WEEK1 ORDER BY REPORT_AS_OF_DT ASC) as r1


FROM DEV.GTM.ACR_CHANGE_CLASSIFICATION_WEEKLY_4 -- Continue using this table
WHERE 1=1
AND BROAD_CLASSIFICATION_4 IN('EXISTING BUSINESS - DOWNGRADE','LATE RENEWAL - DOWNGRADE')
AND REPORT_AS_OF_DT >= '2023-08-01'
--AND EFFECTIVE_SUBSCRIPTION_ACCOUNT_ID in ('732970' , '3771045')
)
where R1 = 1 and pct_acr_change <= -0.05
group by EFFECTIVE_SUBSCRIPTION_ACCOUNT_ID),

latest_close_week AS (
    SELECT MAX(close_week) AS latest_close_week
    FROM close_week_data
),

slg_accounts as (

SELECT c.effective_subscription_account_id as subscription_rpm_account_id,
    c.date,
    c.close_week,
    /*
    CASE 
        WHEN c.close_week = DATEADD(week,-1*{back_test},l.latest_close_week) THEN 'forecast' -- 0 can be replaced by 1,2 etc to go back that many weeks in time
            WHEN c.close_week = NEXT_DAY(DATEADD(day, -90 -{horizon} , l.latest_close_week),'SUN')- 7 THEN 'validation' -- this is valid for Q0, 0 to be replaced by horizon (90, 180) for Q1, Q2
            WHEN c.close_week < NEXT_DAY(DATEADD(day, -90 - {horizon} , l.latest_close_week),'SUN')- 7 THEN 'modeling'
    END AS cat,*/
    CASE  WHEN churn_indicator.churn_date is null THEN '9999-12-31'
          ELSE churn_indicator.churn_date
          END AS churn_date,
    CASE WHEN churn_indicator.churn_close_week is null THEN '9999-12-31'
         ELSE churn_indicator.churn_close_week 
         END AS churn_close_week,
    --downgrades.downgrade_close_week,
    CASE WHEN downgrades.downgrade_close_week is null THEN '9999-12-31'
         ELSE downgrades.downgrade_close_week
         END AS downgrade_close_week, 
/*
    CASE 
        WHEN churn_indicator.churn_close_week is null then 0
        WHEN (DATEADD(week,0 + ceil({horizon}/7),c.close_week) <=  churn_indicator.churn_close_week -- make sure we go beyond 90 days for Q2/Q3
        AND churn_indicator.churn_close_week  < DATEADD(week, 13 + ceil({horizon}/7), c.close_week)) then 1
        ELSE 0
    END As churn_target,*/
    --DATEDIFF('day',c.subscription_term_start_date, c.date) AS days_in_NR,-- this definition is wrong as earliest sub date is required
    --c.SUBSCRIPTION_TERM_START_DATE AS subscription_term_start_date,
    --c.subscription_term_end_date AS subscription_term_end_date,
    CASE 
        WHEN c.subscription_term_end_date = '9999-12-31' OR DATEDIFF('month', c.date, c.subscription_term_end_date) >= 60 then 60
        ELSE DATEDIFF('month', c.date, c.subscription_term_end_date)
        END AS months_until_term_end,
        DATEDIFF('month', c.subscription_term_start_date, c.date) AS months_since_term_start,
    c.buying_program,
    /*
    CASE 
        WHEN (c.customer_sentiment_rating is NULL OR c.customer_sentiment_rating = 'NOT UPDATED' OR 
        c.customer_sentiment_rating = 'Unknown') then 'NA'
        ELSE c.customer_sentiment_rating
        END AS customer_sentiment_rating, */ -- Not available in Consumpt table

    /*CASE 
        WHEN (c.temperature is NULL OR c.temperature = 'NOT APPLICABLE') then 'NA'
        ELSE c.temperature
        END AS temperature, */ -- we have this at the end
    CASE
        WHEN c.renewal_date = '9999-12-31' OR DATEDIFF('day',c.date,c.renewal_date) > 1100 then 1200
        ELSE DATEDIFF('day',c.date,c.renewal_date) 
        END as days_to_renewal,
    
    CASE WHEN 
    c.act_acr is NULL THEN 0
         ELSE c.act_acr
         END AS act_acr,
    /*CASE WHEN
    c.act_crr is NULL THEN 0
        ELSE c.act_crr
        END AS act_crr,*/      
    /*iff(c.physical_country in ('United States', 'United Kingdom', 'Canada', 'Australia', 'India'), 1, 0) as english_country,
    CASE 
            WHEN c.employees is NULL OR c.employees <= 1001 THEN '0-1001'
            WHEN c.employees > 1001 AND c.employees <= 10001  THEN '1001-10001'
            WHEN c.employees > 10001 THEN 'over10001' 
                END AS company_size, */ -- Not available in consump table
    
    CASE WHEN c.is_data_plus = TRUE THEN 1
         WHEN (c.is_data_plus = FALSE OR c.is_data_plus is NULL) THEN 0
             END as is_data_plus,
    CASE WHEN c.is_savings_rollover_subscription = TRUE THEN 1
         WHEN (c.is_savings_rollover_subscription = FALSE or c.is_savings_rollover_subscription is NULL) THEN 0
             END as is_savings_rollover_subscription,
    /*CASE WHEN c.is_ccu_account = TRUE THEN 1
         WHEN (c.is_ccu_account = FALSE or c.is_ccu_account is NULL) THEN 0
             END as is_ccu_account,
    CASE WHEN c.is_paid_ingest_eligible = TRUE THEN 1
         WHEN (c.is_paid_ingest_eligible = FALSE OR c.is_paid_ingest_eligible is NULL) THEN 0
             END as is_paid_ingest_eligible,
    CASE WHEN c.is_paid_users_eligible = TRUE THEN 1
         WHEN (c.is_paid_users_eligible = FALSE OR c.is_paid_users_eligible is NULL) THEN 0
             END as is_paid_users_eligible, */ 
    
FROM close_week_data c 
LEFT JOIN latest_close_week l
ON 1=1
LEFT JOIN churn_indicator
ON churn_indicator.EFFECTIVE_SUBSCRIPTION_ACCOUNT_ID = SUBSCRIPTION_RPM_ACCOUNT_ID
LEFT JOIN downgrades
ON downgrades.EFFECTIVE_SUBSCRIPTION_ACCOUNT_ID = SUBSCRIPTION_RPM_ACCOUNT_ID
AND downgrades.downgrade_close_week >= c.close_week
WHERE c.date = c.close_week -- this is to ensure only close_week data is captured
--AND days_in_NR >= 180 -- only accounts that have been with New Relic for more than 180 days
--AND churn_indicator.churn_date is not NULL -- this filter when included helps to view churn accounts
--AND cat is not null -- only include rows that have modeling, validation or forecast
ORDER BY c.close_week DESC),

 prod_caps AS (
            -- Old Query:
            -- SELECT 
            --     subscription_account_id,
            --     dateadd(day, 6,date_trunc('week',date)) AS usage_week,-- to come to the next Sunday for the duration of data -  Monday to Sunday
            --     COUNT(DISTINCT product_capability) AS weekly_prod_caps_count
            -- FROM ea_reporting.product_analytics.fct_ccu_usage_by_product_capability_daily
            -- WHERE date >= '2023-08-01'
            -- GROUP BY subscription_account_id, usage_week),

            -- New Query:
            SELECT 
                coalesce(dv_subscription_account_id, billable_rpm_account_id) as subscription_account_id,
                DATE(dateadd(day, 6, date_trunc('week', usage_date))) AS usage_week,-- to come to the next Sunday for the duration of data -  Monday to Sunday
                COUNT(DISTINCT product_capability) AS weekly_prod_caps_count
            FROM TRANSFORMED.product_consumption.ccu_consumption_by_dimension_d
            WHERE usage_date >= '2023-08-01'
            GROUP BY subscription_account_id, usage_week),

            product_caps_account AS (
            select accounts.SUBSCRIPTION_RPM_ACCOUNT_ID,
                accounts.close_week,
                prod_caps.* exclude (subscription_account_id, usage_week),
                prod_caps1.weekly_prod_caps_count as weekly_prod_caps_count_1,
                prod_caps2.weekly_prod_caps_count as weekly_prod_caps_count_2,
                prod_caps4.weekly_prod_caps_count as weekly_prod_caps_count_4,
                prod_caps8.weekly_prod_caps_count as weekly_prod_caps_count_8,
                prod_caps12.weekly_prod_caps_count as weekly_prod_caps_count_12,
                div0(prod_caps.weekly_prod_caps_count, prod_caps4.weekly_prod_caps_count) as prod_caps_delta_4,
                div0(prod_caps4.weekly_prod_caps_count, prod_caps8.weekly_prod_caps_count) as prod_caps_delta_4_8,
                div0(prod_caps8.weekly_prod_caps_count, prod_caps12.weekly_prod_caps_count) as prod_caps_delta_8_12

            from slg_accounts as accounts
            left join prod_caps
                on accounts.subscription_rpm_account_id = prod_caps.subscription_account_id and accounts.close_week = prod_caps.usage_week
            left join prod_caps as prod_caps1
                on accounts.subscription_rpm_account_id = prod_caps1.subscription_account_id and accounts.close_week = dateadd(week, 1,prod_caps1.usage_week) -- value one week ago
            left join prod_caps as prod_caps2
                on accounts.subscription_rpm_account_id = prod_caps2.subscription_account_id and accounts.close_week = dateadd(week, 2,prod_caps2.usage_week)
            left join prod_caps as prod_caps4
                on accounts.subscription_rpm_account_id = prod_caps4.subscription_account_id and accounts.close_week = dateadd(week, 4,prod_caps4.usage_week)
            left join prod_caps as prod_caps8
                on accounts.subscription_rpm_account_id = prod_caps8.subscription_account_id and accounts.close_week = dateadd(week, 8,prod_caps8.usage_week)
            left join prod_caps as prod_caps12
                on accounts.subscription_rpm_account_id = prod_caps12.subscription_account_id and accounts.close_week = dateadd(week, 12,prod_caps12.usage_week)),

-- Old Query
-- wafu AS (select a.subscription_account_id,
--         dateadd(day, 6,date_trunc('week',a.date)) as usage_week
--         , count(distinct a.user_monetization_id || a.data_center) as wau
--         from ea_reporting.product_analytics.fct_ccu_user_activity_by_product_capability_daily as a -- Kayla will be working on this 

--         left join ea_reporting.intermediate.int_subscription_account_flags_daily as b -- Kayla will be working on this 
--         on a.date = b.date and a.subscription_account_id = b.subscription_account_id
        
--         left join ea_reporting.product_analytics.agg_subscription_daily_metrics as c -- Kayla will be working on this 
--         on a.date = c.report_as_of_dt and a.subscription_account_id = c.subscription_account_id
        
--         where  daily_active
--         and okr_paid_account_flag
--         and (is_paid_user_analysis_eligible_pa or is_qbp_pa or c.compute_query_monetize_type is not null)         
--         and a.date >= '2023-08-01'
--         group by all
--         order by 1, 2, 3),

-- New Query
wafu as (
    select a.subscription_account_id,
        dateadd(day, 6,date_trunc('week',a.date)) as usage_week,
        count(distinct a.user_monetization_id || a.data_center) as wau
    from  ea_reporting.product_analytics.fct_ccu_user_activity_by_product_capability_daily as a
    left join reporting.consumption_metrics.consumption_daily_metrics as cdm
            on a.date = cdm.report_as_of_dt and a.subscription_account_id = cdm.subscription_account_id
    where a.daily_active
            and cdm.paid_account_flag
            and (cdm.is_paid_users_eligible or cdm.compute_query_monetize_type is not null) 
            and lower(metric) not ilike '%unbilled%'         
            and a.date >= '2023-08-01'
    group by all
    order by 1, 2, 3
)

            wafu_account AS (
            select accounts.subscription_rpm_account_id,
                accounts.close_week,
                wafu.* exclude (subscription_account_id, usage_week),
                wafu1.wau as wau_1,
                wafu2.wau as wau_2,
                wafu4.wau as wau_4,
                wafu8.wau as wau_8,
                wafu12.wau as wau_12,
                div0(wafu.wau, wafu4.wau) as wau_delta_4,
                div0(wafu4.wau, wafu8.wau) as wau_delta_4_8,
                div0(wafu4.wau, wafu12.wau) as wau_delta_8_12

            from slg_accounts as accounts
            left join wafu
                on accounts.subscription_rpm_account_id = wafu.subscription_account_id and accounts.close_week = wafu.usage_week
            left join wafu as wafu1
                on accounts.subscription_rpm_account_id = wafu1.subscription_account_id and accounts.close_week = dateadd(week, 1,wafu1.usage_week)
            left join wafu as wafu2
                on accounts.subscription_rpm_account_id = wafu2.subscription_account_id and accounts.close_week = dateadd(week, 2,wafu2.usage_week)
            left join wafu as wafu4
                on accounts.subscription_rpm_account_id = wafu4.subscription_account_id and accounts.close_week = dateadd(week, 4,wafu4.usage_week)
            left join wafu as wafu8
                on accounts.subscription_rpm_account_id = wafu8.subscription_account_id and accounts.close_week = dateadd(week, 8,wafu8.usage_week)
            left join wafu as wafu12
                on accounts.subscription_rpm_account_id = wafu12.subscription_account_id and accounts.close_week = dateadd(week, 12,wafu12.usage_week)),
 bcm as (

select effective_subscription_account_id, 
dateadd(day, 6,date_trunc('week',report_as_of_dt)) as usage_week,
max(bcm_run_rate) as bcm_run_rate
from reporting.consumption_metrics.consumption_daily_metrics
where report_as_of_dt = usage_week
and subscription_account_id = EFFECTIVE_SUBSCRIPTION_ACCOUNT_ID
group by all),

bcm_run_rate as (

select accounts.subscription_rpm_account_id,
                accounts.close_week,
                bcm.bcm_run_rate as bcm_rr
                ,bcm4.bcm_run_rate as bcm_rr_4
                ,bcm8.bcm_run_rate as bcm_rr_8
                ,bcm12.bcm_run_rate as bcm_rr_12
                ,bcm24.bcm_run_rate as bcm_rr_24
                ,div0(bcm.bcm_run_rate, bcm4.bcm_run_rate) as bcm_rr_delta_4
                ,div0(bcm4.bcm_run_rate, bcm8.bcm_run_rate) as bcm_rr_delta_4_8
                ,div0(bcm8.bcm_run_rate, bcm12.bcm_run_rate) as bcm_rr_delta_8_12
                ,div0(bcm12.bcm_run_rate, bcm24.bcm_run_rate) as bcm_rr_delta_12_24
            from slg_accounts as accounts
            left join bcm
            ON accounts.subscription_rpm_account_id = bcm.effective_subscription_account_id and accounts.close_week = bcm.usage_week
            left join bcm as bcm4
            ON accounts.subscription_rpm_account_id = bcm4.effective_subscription_account_id and accounts.close_week = dateadd(week,4,bcm4.usage_week)
            left join bcm as bcm8
            ON accounts.subscription_rpm_account_id = bcm8.effective_subscription_account_id and accounts.close_week = dateadd(week,8,bcm8.usage_week)
            left join bcm as bcm12
            ON accounts.subscription_rpm_account_id = bcm12.effective_subscription_account_id and accounts.close_week = dateadd(week,12,bcm12.usage_week)
            left join bcm as bcm24
            ON accounts.subscription_rpm_account_id = bcm24.effective_subscription_account_id and accounts.close_week = dateadd(week,24,bcm24.usage_week) 
),    

-- Old Query
-- ingest as (select subscription_account_id, 
-- dateadd(day,6,date_trunc('week',report_as_of_dt)) as usage_week,
-- max(rolling_7_day_avg_total_consumption_gb) as rolling_7_day_avg_total_consumption_gb
-- from transformed.product_consumption.mt_rolling_tdp_consumption_sub_acct_daily_aggr -- This is going away and needs to be changed
-- where report_as_of_dt = usage_week
-- and metric = 'GigabytesIngested'
-- group by all
-- ),

-- New Query
 daily_consumption AS (
    SELECT 
        coalesce(dv_subscription_account_id, BILLABLE_RPM_ACCOUNT_ID) as subscription_account_id,
        USAGE_DATE,
        SUM(TOTAL_CONSUMPTION_GB) as daily_consumption_gb
    FROM TRANSFORMED.PRODUCT_CONSUMPTION.TDP_CONSUMPTION_BY_USAGE_SOURCE_DAILY_F
    WHERE METRIC = 'GigabytesIngested'
    GROUP BY subscription_account_id, USAGE_DATE
),
rolling_avg AS (
    SELECT 
        subscription_account_id,
        USAGE_DATE as report_as_of_dt,
        AVG(daily_consumption_gb) OVER (
            PARTITION BY subscription_account_id 
            ORDER BY USAGE_DATE 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as rolling_7_day_avg_total_consumption_gb,
        AVG(daily_consumption_gb) OVER (
            PARTITION BY subscription_account_id 
            ORDER BY USAGE_DATE 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as rolling_30_day_avg_billable_consumption_gb
    FROM daily_consumption
),

-- Needs to be fixed completely. Has missing days and this needs to be handled.
ingest AS (
    SELECT subscription_account_id, 
    DATEADD(day, 6, DATE_TRUNC('week', report_as_of_dt)) as usage_week,
    MAX(rolling_7_day_avg_total_consumption_gb) as rolling_7_day_avg_total_consumption_gb
    FROM rolling_avg
    GROUP BY all
),

ingest_table as (

select accounts.subscription_rpm_account_id,
                accounts.close_week,
                ingest.rolling_7_day_avg_total_consumption_gb as rolling_7_day_avg_total_consumption_gb
                ,ingest_4.rolling_7_day_avg_total_consumption_gb as rolling_7_day_avg_total_consumption_gb_4
                ,ingest_8.rolling_7_day_avg_total_consumption_gb as rolling_7_day_avg_total_consumption_gb_8
                ,ingest_12.rolling_7_day_avg_total_consumption_gb as rolling_7_day_avg_total_consumption_gb_12
                ,ingest_24.rolling_7_day_avg_total_consumption_gb as rolling_7_day_avg_total_consumption_gb_24
                ,div0(ingest.rolling_7_day_avg_total_consumption_gb, ingest_4.rolling_7_day_avg_total_consumption_gb) as rolling_7_day_avg_total_consumption_gb_delta_4
                ,div0(ingest_4.rolling_7_day_avg_total_consumption_gb, ingest_8.rolling_7_day_avg_total_consumption_gb) as rolling_7_day_avg_total_consumption_gb_4_8
                ,div0(ingest_8.rolling_7_day_avg_total_consumption_gb, ingest_12.rolling_7_day_avg_total_consumption_gb) as rolling_7_day_avg_total_consumption_gb_8_12
                ,div0(ingest_12.rolling_7_day_avg_total_consumption_gb, ingest_24.rolling_7_day_avg_total_consumption_gb) as rolling_7_day_avg_total_consumption_gb_12_24
                
            from slg_accounts as accounts
            
            left join ingest
            ON accounts.subscription_rpm_account_id = ingest.subscription_account_id and accounts.close_week = ingest.usage_week
            left join ingest as ingest_4
            ON accounts.subscription_rpm_account_id = ingest_4.subscription_account_id and accounts.close_week = dateadd(week,4,ingest_4.usage_week)
            left join ingest as ingest_8
            ON accounts.subscription_rpm_account_id = ingest_8.subscription_account_id and accounts.close_week = dateadd(week,8,ingest_8.usage_week)
            left join ingest as ingest_12
            ON accounts.subscription_rpm_account_id = ingest_12.subscription_account_id and accounts.close_week = dateadd(week,12,ingest_12.usage_week)
            left join ingest as ingest_24
            ON accounts.subscription_rpm_account_id = ingest_24.subscription_account_id and accounts.close_week = dateadd(week,24,ingest_24.usage_week) 
), 
-- Old Query
-- ingest_30 as (select subscription_account_id, 
-- dateadd(day,6,date_trunc('week',report_as_of_dt)) as usage_week,
-- max(rolling_30_day_avg_billable_consumption_gb) as rolling_30_day_avg_billable_consumption_gb
-- from transformed.product_consumption.mt_rolling_tdp_consumption_sub_acct_daily_aggr -- Check Arunita slack message
-- where report_as_of_dt = usage_week
-- and metric = 'GigabytesIngested'
-- group by all),

-- New Query
ingest_30 AS (
    SELECT subscription_account_id, 
    DATEADD(day, 6, DATE_TRUNC('week', report_as_of_dt)) as usage_week,
    MAX(rolling_30_day_avg_billable_consumption_gb) as rolling_30_day_avg_billable_consumption_gb
    FROM rolling_avg
    GROUP BY all
)

ingest_30_table as (

select accounts.subscription_rpm_account_id,
                accounts.close_week,
                ingest_30.rolling_30_day_avg_billable_consumption_gb as rolling_30_day_avg_billable_consumption_gb
                ,ingest_30_4.rolling_30_day_avg_billable_consumption_gb as rolling_30_day_avg_billable_consumption_gb_4
                ,ingest_30_8.rolling_30_day_avg_billable_consumption_gb as rolling_30_day_avg_billable_consumption_gb_8
                ,ingest_30_12.rolling_30_day_avg_billable_consumption_gb as rolling_30_day_avg_billable_consumption_gb_12
                ,ingest_30_24.rolling_30_day_avg_billable_consumption_gb as rolling_30_day_avg_billable_consumption_gb_24
                ,div0(ingest_30.rolling_30_day_avg_billable_consumption_gb, ingest_30_4.rolling_30_day_avg_billable_consumption_gb) as rolling_30_day_avg_billable_consumption_gb_delta_4
                ,div0(ingest_30_4.rolling_30_day_avg_billable_consumption_gb, ingest_30_8.rolling_30_day_avg_billable_consumption_gb) as rolling_30_day_avg_billable_consumption_gb_4_8
                ,div0(ingest_30_8.rolling_30_day_avg_billable_consumption_gb, ingest_30_12.rolling_30_day_avg_billable_consumption_gb) as rolling_30_day_avg_billable_consumption_gb_8_12
                ,div0(ingest_30_12.rolling_30_day_avg_billable_consumption_gb, ingest_30_24.rolling_30_day_avg_billable_consumption_gb) as rolling_30_day_avg_billable_consumption_gb_12_24
                
            from slg_accounts as accounts
            
            left join ingest_30
            ON accounts.subscription_rpm_account_id = ingest_30.subscription_account_id and accounts.close_week = ingest_30.usage_week
            left join ingest_30 as ingest_30_4
            ON accounts.subscription_rpm_account_id = ingest_30_4.subscription_account_id and accounts.close_week = dateadd(week,4,ingest_30_4.usage_week)
            left join ingest_30 as ingest_30_8
            ON accounts.subscription_rpm_account_id = ingest_30_8.subscription_account_id and accounts.close_week = dateadd(week,8,ingest_30_8.usage_week)
            left join ingest_30 as ingest_30_12
            ON accounts.subscription_rpm_account_id = ingest_30_12.subscription_account_id and accounts.close_week = dateadd(week,12,ingest_30_12.usage_week)
            left join ingest_30 as ingest_30_24
            ON accounts.subscription_rpm_account_id = ingest_30_24.subscription_account_id and accounts.close_week = dateadd(week,24,ingest_30_24.usage_week) 
),

prod_families as (
-- Old Query
-- (select distinct subscription_account_id, dateadd(day, 6,date_trunc('week',date)) AS usage_week,
-- product_capability as prod_cap,
-- case 
--     when prod_cap IN ('Alert Conditions','AIOps Alerts','NR AI','NRAI','AIOps','Model Performance','Change Tracking','Alerts','Lookout','ML Ops','Grok','Detection') then 'Alerts_AIOps'
--     when prod_cap IN ('RPM API', 'Nerdgraph','Insights API') then 'APIs'
--     when prod_cap IN ('APM','Errors Inbox','Serverless','Infinite Tracing','Recommendations','Catalog Apps','Traces','AI Monitoring','AWS Lambda Setup','Custom Applications','Codestream','Key Transactions','Distrubuted Tracing','Open Telemetry','Product Notifications') then 'APM'
--     when prod_cap IN ('Query Your Data','Data Explorer','Public Dashboards','Public Charts','Dashboards','Legacy Charts') then 'Dashboards'
--     when prod_cap IN ('Mobile','Browser','Synthetics','Automated Testing','Website Performance Monitoring') then 'DEM'
--     when prod_cap IN ('Teams','Events to Metrics','Entities & Relationships','Explorer','Service Level Management','Event to Metrics','Entities & Relationships API','All Entities','Workloads') then 'Developer_Portal'
--     when prod_cap IN ('K8s Cluster Explorer','Prometheus API','Network','Infrastructure','Fleet','Kubernetes') then 'Infra'
--     when prod_cap IN ('Logs') then 'Logs'
--     when prod_cap IN ('Vulnerability Management','IAST','Security') then 'Security'
--     when prod_cap IN ('Other','Add Data','Internal Monitoring','View Your Usage','API Keys','All Capabilities','User Preferences',	'Terraform','Saved Views','Discussions','Manage Your Plan','User Migration','Manage Your Data Standalone','Account Settings','Administration','Account Management','New Relic Web UI') then 'Shared_Platforms'
--     when prod_cap IN ('Unattributed') OR prod_cap is NULL then 'Unattributed'
-- ELSE  'Unattributed'
-- END as prod_fam
-- from ea_reporting.product_analytics.fct_ccu_usage_by_product_capability_daily --This source will be changed with TRANSFORMED.product_consumption.ccu_consumption_by_dimension_d
-- WHERE date >= '2023-08-01'
-- ORDER BY usage_week DESC, subscription_account_id),

-- New Query
select distinct coalesce(dv_subscription_account_id, billable_rpm_account_id) as subscription_account_id,
DATE(dateadd(day, 6, date_trunc('week', usage_date))) AS usage_week,
product_capability as prod_cap,
case 
    when prod_cap IN ('Alert Conditions','AIOps Alerts','NR AI','NRAI','AIOps','Model Performance','Change Tracking','Alerts','Lookout','ML Ops','Grok','Detection') then 'Alerts_AIOps'
    when prod_cap IN ('RPM API', 'Nerdgraph','Insights API') then 'APIs'
    when prod_cap IN ('APM','Errors Inbox','Serverless','Infinite Tracing','Recommendations','Catalog Apps','Traces','AI Monitoring','AWS Lambda Setup','Custom Applications','Codestream','Key Transactions','Distrubuted Tracing','Open Telemetry','Product Notifications') then 'APM'
    when prod_cap IN ('Query Your Data','Data Explorer','Public Dashboards','Public Charts','Dashboards','Legacy Charts') then 'Dashboards'
    when prod_cap IN ('Mobile','Browser','Synthetics','Automated Testing','Website Performance Monitoring') then 'DEM'
    when prod_cap IN ('Teams','Events to Metrics','Entities & Relationships','Explorer','Service Level Management','Event to Metrics','Entities & Relationships API','All Entities','Workloads') then 'Developer_Portal'
    when prod_cap IN ('K8s Cluster Explorer','Prometheus API','Network','Infrastructure','Fleet','Kubernetes') then 'Infra'
    when prod_cap IN ('Logs') then 'Logs'
    when prod_cap IN ('Vulnerability Management','IAST','Security') then 'Security'
    when prod_cap IN ('Other','Add Data','Internal Monitoring','View Your Usage','API Keys','All Capabilities','User Preferences',	'Terraform','Saved Views','Discussions','Manage Your Plan','User Migration','Manage Your Data Standalone','Account Settings','Administration','Account Management','New Relic Web UI') then 'Shared_Platforms'
    when prod_cap IN ('Unattributed') OR prod_cap is NULL then 'Unattributed'
ELSE  'Unattributed'
END as prod_fam
FROM TRANSFORMED.product_consumption.ccu_consumption_by_dimension_d
WHERE usage_date >= '2023-08-01'
ORDER BY usage_week DESC, subscription_account_id),

prod_fam_agg as 

(select DISTINCT usage_week,subscription_account_id,
LISTAGG(DISTINCT prod_fam, ', ') WITHIN GROUP (ORDER BY prod_fam) AS concatenated_prod_fam
from prod_families
GROUP BY usage_week, subscription_account_id
order by usage_week DESC, subscription_account_id),

prod_fam_binary as

(select accounts.subscription_rpm_account_id, accounts.close_week, 
case when pf.concatenated_prod_fam like '%APM%' then 1 else 0 end as is_apm,
case when pf.concatenated_prod_fam like '%Alerts_AIOps%' then 1 else 0 end as is_aiops,
case when pf.concatenated_prod_fam like '%APIs%' then 1 else 0 end as is_api,
case when pf.concatenated_prod_fam like '%Dashboard%' then 1 else 0 end as is_dash,
case when pf.concatenated_prod_fam like '%DEM%' then 1 else 0 end as is_dem,
--case when concatenated_prod_fam like '%Developer_Portal%' then 1 else 0 end as is_dev_portal,
case when pf.concatenated_prod_fam like '%Infra%' then 1 else 0 end as is_infra,
case when pf.concatenated_prod_fam like '%Logs%' then 1 else 0 end as is_logs,
case when pf.concatenated_prod_fam like '%Security%' then 1 else 0 end as is_security,
case when pf.concatenated_prod_fam like '%Shared_Platforms%' then 1 else 0 end as is_shared_platform

from slg_accounts as accounts 
left join  prod_fam_agg pf 
ON pf.usage_week = accounts.close_week AND accounts.subscription_rpm_account_id = pf.subscription_account_id),


temp_t AS (
select effective_subscription_account_id, usage_week, temperature
from (
select effective_subscription_account_id, 
dateadd(day, 6,date_trunc('week',report_as_of_dt)) as usage_week,
ROW_NUMBER() OVER (PARTITION BY usage_week, effective_subscription_account_id ORDER BY temperature) as rank,
CASE 
        WHEN (temperature is NULL OR temperature = 'NOT APPLICABLE') then 'NA'
        ELSE temperature
        END AS temperature
        
from reporting.consumption_metrics.consumption_daily_metrics
where report_as_of_dt = usage_week
and subscription_account_id = EFFECTIVE_SUBSCRIPTION_ACCOUNT_ID
) temp
where temp.rank = 1),

temp_tbl as (

select accounts.subscription_rpm_account_id,
                accounts.close_week,
                 CASE WHEN temp_t.temperature is NULL then 'NA' else temp_t.temperature end as temperature
                ,CASE WHEN temp_t_4.temperature is NULL then 'NA' else temp_t_4.temperature end as temperature_4
                ,CASE WHEN temp_t_8.temperature is NULL then 'NA' else temp_t_8.temperature end as temperature_8
                ,CASE WHEN temp_t_12.temperature is NULL then 'NA' else temp_t_12.temperature end as temperature_12
                ,CASE WHEN temp_t_24.temperature is NULL then 'NA' else temp_t_24.temperature end as temperature_24
               
            from slg_accounts as accounts
            left join temp_t
            ON accounts.subscription_rpm_account_id = temp_t.effective_subscription_account_id and accounts.close_week = temp_t.usage_week
            left join temp_t as temp_t_4
            ON accounts.subscription_rpm_account_id = temp_t_4.effective_subscription_account_id and accounts.close_week = dateadd(week,4,temp_t_4.usage_week)
            left join temp_t as temp_t_8
            ON accounts.subscription_rpm_account_id = temp_t_8.effective_subscription_account_id and accounts.close_week = dateadd(week,8,temp_t_8.usage_week)
            left join temp_t as temp_t_12
            ON accounts.subscription_rpm_account_id = temp_t_12.effective_subscription_account_id and accounts.close_week = dateadd(week,12,temp_t_12.usage_week)
            left join temp_t as temp_t_24
            ON accounts.subscription_rpm_account_id = temp_t_24.effective_subscription_account_id and accounts.close_week = dateadd(week,24,temp_t_24.usage_week) 
), 

sentiment AS (
-- Old Query
-- select effective_subscription_account_id, usage_week, customer_sentiment_rating as sentiment_rating
-- from (
-- select effective_subscription_account_id, 
-- dateadd(day, 6,date_trunc('week',report_as_of_dt)) as usage_week,
-- ROW_NUMBER() OVER (PARTITION BY usage_week, effective_subscription_account_id ORDER BY customer_sentiment_rating) as rank,
-- CASE 
--         WHEN (customer_sentiment_rating is NULL OR customer_sentiment_rating = 'NOT UPDATED' OR customer_sentiment_rating = 'Unknown') then 'NA'
--         ELSE customer_sentiment_rating
--         END AS customer_sentiment_rating       
-- from reporting.nrem.subscription_daily_metrics -- Can be replaced with reporting.consumption_metrics.consumption_daily_metrics
-- where report_as_of_dt = usage_week
-- and subscription_rpm_account_id = EFFECTIVE_SUBSCRIPTION_ACCOUNT_ID
-- ) senti_inner
-- where senti_inner.rank = 1

-- New Query
select effective_subscription_account_id, usage_week, customer_sentiment_rating as sentiment_rating
from (
select effective_subscription_account_id, 
dateadd(day, 6,date_trunc('week',report_as_of_dt)) as usage_week,
ROW_NUMBER() OVER (PARTITION BY usage_week, effective_subscription_account_id ORDER BY customer_sentiment_rating) as rank,
CASE 
        WHEN (customer_sentiment_rating is NULL OR customer_sentiment_rating = 'NOT UPDATED' OR customer_sentiment_rating = 'Unknown') then 'NA'
        ELSE customer_sentiment_rating
        END AS customer_sentiment_rating       
from reporting.consumption_metrics.consumption_daily_metrics
where report_as_of_dt = usage_week
and subscription_account_id = EFFECTIVE_SUBSCRIPTION_ACCOUNT_ID
) senti_inner
where senti_inner.rank = 1
),

sentiment_tbl as (

select accounts.subscription_rpm_account_id,
                accounts.close_week,
                 CASE WHEN sentiment.sentiment_rating is NULL then 'NA' else sentiment.sentiment_rating end as sentiment_rating
                ,CASE WHEN sentiment_4.sentiment_rating is NULL then 'NA' else sentiment_4.sentiment_rating  end as sentiment_rating_4
                ,CASE WHEN sentiment_8.sentiment_rating is NULL then 'NA' else sentiment_8.sentiment_rating  end as sentiment_rating_8
                ,CASE WHEN sentiment_12.sentiment_rating is NULL then 'NA' else sentiment_12.sentiment_rating  end as sentiment_rating_12
                ,CASE WHEN sentiment_24.sentiment_rating is NULL then 'NA' else sentiment_24.sentiment_rating  end as sentiment_rating_24
               
            from slg_accounts as accounts
            left join sentiment
            ON accounts.subscription_rpm_account_id = sentiment.effective_subscription_account_id and accounts.close_week = sentiment.usage_week
            left join sentiment as sentiment_4
            ON accounts.subscription_rpm_account_id = sentiment_4.effective_subscription_account_id and accounts.close_week = dateadd(week,4,sentiment_4.usage_week)
            left join sentiment as sentiment_8
            ON accounts.subscription_rpm_account_id = sentiment_8.effective_subscription_account_id and accounts.close_week = dateadd(week,8,sentiment_8.usage_week)
            left join sentiment as sentiment_12
            ON accounts.subscription_rpm_account_id = sentiment_12.effective_subscription_account_id and accounts.close_week = dateadd(week,12,sentiment_12.usage_week)
            left join sentiment as sentiment_24
            ON accounts.subscription_rpm_account_id = sentiment_24.effective_subscription_account_id and accounts.close_week = dateadd(week,24,sentiment_24.usage_week) 
), 

flag as 

(select subscription_account_id, dateadd(day, 6,date_trunc('week',report_as_of_dt)) as usage_week,
is_paid_ingest_eligible,is_paid_users_eligible, is_paid_account_eligible, is_tiered_pricing,is_paid_synthetics_eligible, is_paid_qbp_eligible, is_ccu_account
from transformed.finance.subscription_daily_derivations_d -- check again later
where report_as_of_dt > '2023-08-01'
and report_as_of_dt = usage_week),

flag_tbl as (select accounts.subscription_rpm_account_id,
                accounts.close_week,
                CASE WHEN flag.is_paid_ingest_eligible = TRUE THEN 1
                     WHEN (flag.is_paid_ingest_eligible = FALSE OR flag.is_paid_ingest_eligible is NULL) THEN 0
                END as is_paid_ingest_eligible,
                CASE WHEN flag.is_paid_users_eligible = TRUE THEN 1
                     WHEN (flag.is_paid_users_eligible = FALSE OR flag.is_paid_users_eligible is NULL) THEN 0
                END as is_paid_users_eligible,
                CASE WHEN flag.is_paid_account_eligible = TRUE THEN 1
                     WHEN (flag.is_paid_account_eligible = FALSE OR flag.is_paid_account_eligible is NULL) THEN 0
                END as is_paid_account_eligible,
                CASE WHEN flag.is_tiered_pricing = TRUE THEN 1
                     WHEN (flag.is_tiered_pricing = FALSE OR flag.is_tiered_pricing is NULL) THEN 0
                END as is_tiered_pricing,
                CASE WHEN flag.is_paid_synthetics_eligible = TRUE THEN 1
                     WHEN (flag.is_paid_synthetics_eligible = FALSE OR flag.is_paid_synthetics_eligible is NULL) THEN 0
                END as is_paid_synthetics_eligible,
                CASE WHEN flag.is_paid_qbp_eligible = TRUE THEN 1
                     WHEN (flag.is_paid_qbp_eligible = FALSE OR flag.is_paid_qbp_eligible is NULL) THEN 0
                END as is_paid_qbp_eligible,
                CASE WHEN flag.is_ccu_account = TRUE THEN 1
                     WHEN (flag.is_ccu_account = FALSE OR flag.is_ccu_account is NULL) THEN 0
                END as is_ccu_account

            from slg_accounts as accounts
            left join flag
            ON accounts.subscription_rpm_account_id = flag.subscription_account_id and accounts.close_week = flag.usage_week), 

change_tracking_weekly AS (
    select  distinct btah.subscription_account_id, DATEADD(day ,6, date_trunc('week', ct.DATE)) as usage_week 
    from conformed_staging.nrdb_ingest_prod.stg_nrdb_ingest_prod_change_tracking_deployment ct --Need to change this to conformed from conformed_staging
    left join transformed.common.bt_account_hierarchy_d_tv btah on 1=1
        and ct.ACCOUNT_ID = btah.account_id
        and btah.effective_start_date <= DATEADD(day ,6, date_trunc('week', ct.DATE))
        and btah.effective_end_date >= date_trunc('week', ct.DATE)
    group by all
),

change_tracking AS (
select accounts.subscription_rpm_account_id, accounts.close_week,
case when change_tracking_weekly.usage_week is NULL then 0 ELSE 1 END AS is_change_tracking,
case when change_tracking_weekly_4.usage_week is NULL then 0 ELSE 1 END AS is_change_tracking_4,
case when change_tracking_weekly_8.usage_week is NULL then 0 ELSE 1 END AS is_change_tracking_8,
case when change_tracking_weekly_12.usage_week is NULL then 0 ELSE 1 END AS is_change_tracking_12,
case when change_tracking_weekly_24.usage_week is NULL then 0 ELSE 1 END AS is_change_tracking_24

from slg_accounts as accounts
left join change_tracking_weekly 
ON accounts.subscription_rpm_account_id = change_tracking_weekly.subscription_account_id 
and accounts.close_week = change_tracking_weekly.usage_week

left join change_tracking_weekly as change_tracking_weekly_4
ON accounts.subscription_rpm_account_id = change_tracking_weekly_4.subscription_account_id 
and accounts.close_week = dateadd(week,4,change_tracking_weekly_4.usage_week)
left join change_tracking_weekly as change_tracking_weekly_8
ON accounts.subscription_rpm_account_id = change_tracking_weekly_8.subscription_account_id 
and accounts.close_week = dateadd(week,8,change_tracking_weekly_8.usage_week)
left join change_tracking_weekly as change_tracking_weekly_12
ON accounts.subscription_rpm_account_id = change_tracking_weekly_12.subscription_account_id 
and accounts.close_week = dateadd(week,12,change_tracking_weekly_12.usage_week)
left join change_tracking_weekly as change_tracking_weekly_24
ON accounts.subscription_rpm_account_id = change_tracking_weekly_24.subscription_account_id 
and accounts.close_week = dateadd(week,24,change_tracking_weekly_24.usage_week)
),

advance_nrql_weekly as (
    select distinct btah.subscription_account_id, DATEADD(day ,6, date_trunc('week', ct.DATE)) as usage_week
    from CONFORMED_STAGING.NRDB_INGEST_PROD.STG_NRDB_INGEST_PROD_ADVANCE_NRQL_ACCOUNTS ct --Need to change this to conformed from conformed_staging
    left join transformed.common.bt_account_hierarchy_d_tv btah 
    on 1=1
    and ct.ACCOUNT_ID = btah.account_id
    and btah.effective_start_date <= DATEADD(day ,6, date_trunc('week', ct.DATE))
    and btah.effective_end_date >= date_trunc('week', ct.DATE)
    group by all
), 

advance_nrql AS (
select accounts.subscription_rpm_account_id, accounts.close_week,
case when advance_nrql_weekly.usage_week is NULL then 0 ELSE 1 END AS is_advance_nrql,
case when advance_nrql_weekly_4.usage_week is NULL then 0 ELSE 1 END AS is_advance_nrql_4,
case when advance_nrql_weekly_8.usage_week is NULL then 0 ELSE 1 END AS is_advance_nrql_8,
case when advance_nrql_weekly_12.usage_week is NULL then 0 ELSE 1 END AS is_advance_nrql_12,
case when advance_nrql_weekly_24.usage_week is NULL then 0 ELSE 1 END AS is_advance_nrql_24

from slg_accounts as accounts
left join advance_nrql_weekly 
ON accounts.subscription_rpm_account_id = advance_nrql_weekly.subscription_account_id 
and accounts.close_week = advance_nrql_weekly.usage_week

left join advance_nrql_weekly as advance_nrql_weekly_4
ON accounts.subscription_rpm_account_id = advance_nrql_weekly_4.subscription_account_id and accounts.close_week = dateadd(week,4,advance_nrql_weekly_4.usage_week)

left join advance_nrql_weekly as advance_nrql_weekly_8
ON accounts.subscription_rpm_account_id = advance_nrql_weekly_8.subscription_account_id and accounts.close_week = dateadd(week,8,advance_nrql_weekly_8.usage_week)

left join advance_nrql_weekly as advance_nrql_weekly_12
ON accounts.subscription_rpm_account_id = advance_nrql_weekly_12.subscription_account_id and accounts.close_week = dateadd(week,12,advance_nrql_weekly_12.usage_week)

left join advance_nrql_weekly as advance_nrql_weekly_24
ON accounts.subscription_rpm_account_id = advance_nrql_weekly_24.subscription_account_id and accounts.close_week = dateadd(week,24,advance_nrql_weekly_24.usage_week)
),

apm_logs_weekly as 
(select DATEADD(day,6,week) AS usage_week,   
subscription_account_id, 
(API_LOGS_WEEKLY_GB + LOGS_APM_WEEKLY_GB)  as apm_logs_gb
from EA_REPORTING.PRODUCT_ANALYTICS.AGG_APM_LOGS_ATTACH_WEEKLY --Need to change this 
group by all
),

apm_logs_table as (

select accounts.subscription_rpm_account_id,
                accounts.close_week
                ,case when apm_logs_weekly.apm_logs_gb is null then 0 else apm_logs_weekly.apm_logs_gb end as apm_logs_gb
                ,case when apm_logs_weekly_4.apm_logs_gb is null then 0 else apm_logs_weekly_4.apm_logs_gb end as apm_logs_gb_4
                ,case when apm_logs_weekly_8.apm_logs_gb is null then 0 else apm_logs_weekly_8.apm_logs_gb end as apm_logs_gb_8
                ,case when apm_logs_weekly_12.apm_logs_gb is null then 0 else apm_logs_weekly_12.apm_logs_gb end as apm_logs_gb_12
                ,case when apm_logs_weekly_24.apm_logs_gb is null then 0 else apm_logs_weekly_24.apm_logs_gb end as apm_logs_gb_24
                ,div0(apm_logs_weekly.apm_logs_gb, apm_logs_weekly_4.apm_logs_gb) as apm_logs_gb_delta_4
                ,div0(apm_logs_weekly_4.apm_logs_gb, apm_logs_weekly_8.apm_logs_gb) as apm_logs_gb_4_8
                ,div0(apm_logs_weekly_8.apm_logs_gb, apm_logs_weekly_12.apm_logs_gb) as apm_logs_gb_8_12
                ,div0(apm_logs_weekly_12.apm_logs_gb, apm_logs_weekly_24.apm_logs_gb) as apm_logs_gb_12_24
                
            from slg_accounts as accounts
            
            left join apm_logs_weekly
            ON accounts.subscription_rpm_account_id = apm_logs_weekly.subscription_account_id and accounts.close_week = 
            apm_logs_weekly.usage_week
            left join apm_logs_weekly as apm_logs_weekly_4
            ON accounts.subscription_rpm_account_id = apm_logs_weekly_4.subscription_account_id and accounts.close_week = dateadd(week,4,apm_logs_weekly_4.usage_week)
            left join apm_logs_weekly as apm_logs_weekly_8
            ON accounts.subscription_rpm_account_id = apm_logs_weekly_8.subscription_account_id and accounts.close_week = dateadd(week,8,apm_logs_weekly_8.usage_week)
            left join apm_logs_weekly as apm_logs_weekly_12
            ON accounts.subscription_rpm_account_id = apm_logs_weekly_12.subscription_account_id and accounts.close_week = dateadd(week,12,apm_logs_weekly_12.usage_week)
            left join apm_logs_weekly as apm_logs_weekly_24
            ON accounts.subscription_rpm_account_id = apm_logs_weekly_24.subscription_account_id and accounts.close_week = dateadd(week,24,apm_logs_weekly_24.usage_week) 
),
competitor_agents_us AS (
    SELECT 
        account_id, date, AppDynamics, DataDog, Dynatrace, ElasticAgent, Grafana, LogicMonitor, OpenTelemetry, SignalFX
        , Splunk, SumoLogic, ZeroAgent, total_competitor_agents
    FROM GROWTH_MARKETING_REPORTING.competitor_agents.competitor_agents_us
),
competitor_agents_eu AS ( -- Grab eu competitor agent counts --
    SELECT 
        account_id, date, AppDynamics, DataDog, Dynatrace, ElasticAgent, Grafana, LogicMonitor, OpenTelemetry, SignalFX
        , Splunk, SumoLogic, ZeroAgent, total_competitor_agents
    FROM GROWTH_MARKETING_REPORTING.competitor_agents.competitor_agents_eu
),
competitor_agents_combined AS ( -- Combine competitor agent CTE's --
    SELECT * FROM competitor_agents_us 
    UNION 
    SELECT * FROM competitor_agents_eu
),
-- Aggregate competitor agent to the current_subscription_rpm_account_id --
comp_agents_weekly AS (
    SELECT  
        btah.subscription_account_id
        , DATEADD('day',6,DATE_TRUNC('week',comp.date)) as usage_week
        --, COALESCE(sum(comp.total_competitor_agents), 0) as total_competitor_agents
        --, COALESCE(sum(comp.AppDynamics), 0) as AppDynamics
        , CASE WHEN COALESCE(sum(comp.AppDynamics), 0) != 0 then 1 else 0 end as is_comp_agent_appdynamics
        --, COALESCE(sum(comp.DataDog), 0) as DataDog
        , CASE WHEN COALESCE(sum(comp.DataDog), 0) != 0 then 1 else 0 end as is_comp_agent_datadog
        --, COALESCE(sum(comp.Dynatrace), 0) as Dynatrace
        , CASE WHEN COALESCE(sum(comp.Dynatrace), 0) != 0 then 1 else 0 end as is_comp_agent_dynatrace
        , CASE WHEN COALESCE(sum(comp.ElasticAgent), 0) != 0 then 1 else 0 end as is_comp_agent_elasticagent
        , CASE WHEN COALESCE(sum(comp.Grafana), 0) != 0 then 1 else 0 end as is_comp_agent_grafana
        , CASE WHEN COALESCE(sum(comp.LogicMonitor), 0) != 0 then 1 else 0 end as is_comp_agent_logicmonitor
        , CASE WHEN COALESCE(sum(comp.OpenTelemetry), 0) != 0 then 1 else 0 end as is_comp_agent_opentelemetry
        , CASE WHEN COALESCE(sum(comp.SignalFX), 0) != 0 then 1 else 0 end as is_comp_agent_signalfx
        , CASE WHEN COALESCE(sum(comp.Splunk), 0) != 0 then 1 else 0 end as is_comp_agent_splunk
        , CASE WHEN COALESCE(sum(comp.SumoLogic), 0) != 0 then 1 else 0 end as is_comp_agent_sumologic
        , CASE WHEN COALESCE(sum(comp.ZeroAgent), 0) != 0 then 1 else 0 end as is_comp_agent_zeroagent
   
    FROM competitor_agents_combined comp
    left join transformed.common.bt_account_hierarchy_d_tv btah ON
    btah.account_id = comp.account_id
    WHERE 1=1
        AND comp.date >= '2023-08-01' -- first date for competitor agents data
    GROUP BY ALL),

comp_agents_table AS (
select accounts.subscription_rpm_account_id
                ,accounts.close_week
                ,case when comp_agents_weekly.is_comp_agent_appdynamics is null then 0 else comp_agents_weekly.is_comp_agent_appdynamics
                end as is_comp_agent_appdynamics
                ,case when comp_agents_weekly.is_comp_agent_datadog is null then 0 else comp_agents_weekly.is_comp_agent_datadog
                end as is_comp_agent_datadog
                ,case when comp_agents_weekly.is_comp_agent_dynatrace is null then 0 else comp_agents_weekly.is_comp_agent_dynatrace
                end as is_comp_agent_dynatrace
                ,case when comp_agents_weekly.is_comp_agent_elasticagent is null then 0 else comp_agents_weekly.is_comp_agent_elasticagent
                end as is_comp_agent_elasticagent
                ,case when comp_agents_weekly.is_comp_agent_grafana is null then 0 else comp_agents_weekly.is_comp_agent_grafana
                end as is_comp_agent_grafana
                ,case when comp_agents_weekly.is_comp_agent_logicmonitor is null then 0 else comp_agents_weekly.is_comp_agent_logicmonitor
                end as is_comp_agent_logicmonitor
                ,case when comp_agents_weekly.is_comp_agent_opentelemetry is null then 0 else comp_agents_weekly.is_comp_agent_opentelemetry
                end as is_comp_agent_opentelemetry
                ,case when comp_agents_weekly.is_comp_agent_signalfx is null then 0 else comp_agents_weekly.is_comp_agent_signalfx
                end as is_comp_agent_signalfx
                ,case when comp_agents_weekly.is_comp_agent_splunk is null then 0 else comp_agents_weekly.is_comp_agent_splunk
                end as is_comp_agent_splunk
                ,case when comp_agents_weekly.is_comp_agent_sumologic is null then 0 else comp_agents_weekly.is_comp_agent_sumologic
                end as is_comp_agent_sumologic
                ,case when comp_agents_weekly.is_comp_agent_zeroagent is null then 0 else comp_agents_weekly.is_comp_agent_zeroagent
                end as is_comp_agent_zeroagent
                
                , case when (comp_agents_weekly.is_comp_agent_appdynamics + comp_agents_weekly.is_comp_agent_datadog + comp_agents_weekly.is_comp_agent_dynatrace + comp_agents_weekly.is_comp_agent_elasticagent + comp_agents_weekly.is_comp_agent_grafana +
comp_agents_weekly.is_comp_agent_logicmonitor +  comp_agents_weekly.is_comp_agent_opentelemetry + comp_agents_weekly.is_comp_agent_signalfx
+ comp_agents_weekly.is_comp_agent_splunk + comp_agents_weekly.is_comp_agent_sumologic + comp_agents_weekly.is_comp_agent_zeroagent) is null then 0

else (comp_agents_weekly.is_comp_agent_appdynamics + comp_agents_weekly.is_comp_agent_datadog + comp_agents_weekly.is_comp_agent_dynatrace + comp_agents_weekly.is_comp_agent_elasticagent + comp_agents_weekly.is_comp_agent_grafana +
comp_agents_weekly.is_comp_agent_logicmonitor +  comp_agents_weekly.is_comp_agent_opentelemetry + comp_agents_weekly.is_comp_agent_signalfx
+ comp_agents_weekly.is_comp_agent_splunk + comp_agents_weekly.is_comp_agent_sumologic + comp_agents_weekly.is_comp_agent_zeroagent)  end as weekly_comps_count
                
            from slg_accounts as accounts
            left join comp_agents_weekly
            ON accounts.subscription_rpm_account_id = comp_agents_weekly.subscription_account_id and accounts.close_week = 
            comp_agents_weekly.usage_week
),
  
 result as (
            select a.*,
            --exclude(churn_date,churn_close_week), 
            product_caps_account.* exclude(subscription_rpm_account_id,close_week),
            wafu_account.* exclude(subscription_rpm_account_id,close_week),
            bcm_run_rate.* exclude(subscription_rpm_account_id,close_week),
            ingest_table.* exclude(subscription_rpm_account_id,close_week),
            ingest_30_table.* exclude(subscription_rpm_account_id,close_week),
            prod_fam_binary.* exclude(subscription_rpm_account_id,close_week),
            temp_tbl.* exclude (subscription_rpm_account_id,close_week),
            sentiment_tbl.* exclude (subscription_rpm_account_id,close_week),
            flag_tbl.* exclude (subscription_rpm_account_id,close_week),
            change_tracking.* exclude (subscription_rpm_account_id,close_week),
            advance_nrql.* exclude (subscription_rpm_account_id,close_week),
            apm_logs_table.* exclude (subscription_rpm_account_id,close_week),
            comp_agents_table.* exclude (subscription_rpm_account_id,close_week)
            from slg_accounts a
            left join product_caps_account
                on a.subscription_rpm_account_id = product_caps_account.subscription_rpm_account_id 
                and a.close_week = product_caps_account.close_week
            left join wafu_account
                on a.subscription_rpm_account_id = wafu_account.subscription_rpm_account_id 
                and a.close_week = wafu_account.close_week 
            left join bcm_run_rate
                on a.subscription_rpm_account_id = bcm_run_rate.subscription_rpm_account_id 
                and a.close_week = bcm_run_rate.close_week
            left join ingest_table
                on a.subscription_rpm_account_id = ingest_table.subscription_rpm_account_id 
                and a.close_week = ingest_table.close_week
            left join ingest_30_table
                ON a.subscription_rpm_account_id = ingest_30_table.subscription_rpm_account_id 
                and a.close_week = ingest_30_table.close_week
            left join prod_fam_binary
                ON a.subscription_rpm_account_id = prod_fam_binary.subscription_rpm_account_id 
                and a.close_week = prod_fam_binary.close_week
            left join temp_tbl
                ON a.subscription_rpm_account_id = temp_tbl.subscription_rpm_account_id 
                and a.close_week = temp_tbl.close_week
            left join sentiment_tbl
                ON a.subscription_rpm_account_id = sentiment_tbl.subscription_rpm_account_id 
                and a.close_week = sentiment_tbl.close_week
             left join flag_tbl
                ON a.subscription_rpm_account_id = flag_tbl.subscription_rpm_account_id 
                and a.close_week = flag_tbl.close_week
            left join change_tracking
                ON a.subscription_rpm_account_id = change_tracking.subscription_rpm_account_id 
                and a.close_week = change_tracking.close_week
            left join advance_nrql
                ON a.subscription_rpm_account_id = advance_nrql.subscription_rpm_account_id 
                and a.close_week = advance_nrql.close_week
            left join apm_logs_table
                ON a.subscription_rpm_account_id = apm_logs_table.subscription_rpm_account_id 
                and a.close_week = apm_logs_table.close_week
            left join comp_agents_table
                ON a.subscription_rpm_account_id = comp_agents_table.subscription_rpm_account_id 
                and a.close_week = comp_agents_table.close_week
        ),

close_week_final as (select max(close_week) as latest_close_week_final 
                     from result)

select * exclude(latest_close_week_final)
from result
cross join close_week_final
where close_week = close_week_final.latest_close_week_final
ORDER BY close_week DESC