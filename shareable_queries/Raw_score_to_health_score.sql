CREATE OR REPLACE TABLE dev.gtm.customer_360_v5_hscores AS

with base as 
(
select * 
from dev.gtm.spreetham_customer_360_v5 
unpivot INCLUDE NULLS (
    metric_value for metric_name in (
       product_stickiness_ratio,
       user_stickiness_ratio,
       user_activation_percentage, 
       product_utilization_rate,
       revenue_growth,
       renewal_urgency_months,
       overage_score,
       contract_length_score, 
       pct_P1_cases,
       pct_P1P2_cases,
       resolution_rate,
       open_tickets,
       avg_csat_score,
       mthly_fttr_50,
       churn_risk_score,
       sentiment_score,
       engagement_score, 
       MAXIMUM_DAYS_PAST_DUE, 
       SUM_TOTAL_AMOUNT_DUE 
   )
)),

base_2 as 
(select *, 
case when metric_name in ('PRODUCT_STICKINESS_RATIO',
                          'USER_STICKINESS_RATIO',
                          'USER_ACTIVATION_PERCENTAGE', 
                          'PRODUCT_UTILIZATION_RATE') 
then 'Adoption_score'
when metric_name in ('AVG_CSAT_SCORE',      
                     'OPEN_TICKETS',
                     'ENGAGEMENT_SCORE') 
then 'Engagement_score'
 when metric_name in ('REVENUE_GROWTH',
       'RENEWAL_URGENCY_MONTHS',
       'OVERAGE_SCORE',
       'CONTRACT_LENGTH_SCORE', 
       'MAXIMUM_DAYS_PAST_DUE', 
       'SUM_TOTAL_AMOUNT_DUE' ) 
then 'Financial_health_score'
 when metric_name in ('PCT_P1_CASES',
                      'PCT_P1P2_CASES',
                      'RESOLUTION_RATE',
                      'MTHLY_FTTR_50', 
                      'CHURN_RISK_SCORE',
                      'SENTIMENT_SCORE') 
then 'Risk_score' end as score_type      
from base 
), 

percentiles AS (
    -- Calculate 1st and 99th percentiles for specified metrics within the date range
    SELECT
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY USER_ACTIVATION_PERCENTAGE) AS p1_user_activation,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY USER_ACTIVATION_PERCENTAGE) AS p99_user_activation,
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY PRODUCT_STICKINESS_RATIO) AS p1_prod_stickiness,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY PRODUCT_STICKINESS_RATIO) AS p99_prod_stickiness,
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY USER_STICKINESS_RATIO) AS p1_user_stickiness,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY USER_STICKINESS_RATIO) AS p99_user_stickiness,
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY PRODUCT_UTILIZATION_RATE) AS p1_prod_utilization,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY PRODUCT_UTILIZATION_RATE) AS p99_prod_utilization,
        -- PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY RENEWAL_URGENCY_MONTHS) AS p1_renewal_urgency, -- Assuming higher months = better score
        -- PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY RENEWAL_URGENCY_MONTHS) AS p99_renewal_urgency,
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY OVERAGE_SCORE) AS p1_overage,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY OVERAGE_SCORE) AS p99_overage,
        -- PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY CONTRACT_LENGTH_SCORE) AS p1_contract_length,
        -- PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY CONTRACT_LENGTH_SCORE) AS p99_contract_length,
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY PCT_P1_CASES) AS p1_pct_p1_cases,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY PCT_P1_CASES) AS p99_pct_p1_cases,
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY PCT_P1P2_CASES) AS p1_pct_p1p2_cases,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY PCT_P1P2_CASES) AS p99_pct_p1p2_cases,
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY RESOLUTION_RATE) AS p1_resolution_rate,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY RESOLUTION_RATE) AS p99_resolution_rate,
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY OPEN_TICKETS) AS p1_open_tickets,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY OPEN_TICKETS) AS p99_open_tickets,
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY AVG_CSAT_SCORE) AS p1_avg_csat,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY AVG_CSAT_SCORE) AS p99_avg_csat,
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY MTHLY_FTTR_50) AS p1_mthly_fttr,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY MTHLY_FTTR_50) AS p99_mthly_fttr,
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY MAXIMUM_DAYS_PAST_DUE) AS p1_max_days_past_due,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY MAXIMUM_DAYS_PAST_DUE) AS p99_max_days_past_due,
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY SUM_TOTAL_AMOUNT_DUE) AS p1_sum_total_due,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY SUM_TOTAL_AMOUNT_DUE) AS p99_sum_total_due
    FROM dev.gtm.spreetham_customer_360_v5 -- Replace with your actual source table name if different
    WHERE REPORT_AS_OF_DT >= '2024-04-01' AND REPORT_AS_OF_DT <= '2025-03-31' -- Date range for percentile calculation
), 

scores_calculation as 
(
SELECT
    s.subscription_account_id,
    -- Selects all columns from the source table
    s.report_as_of_dt, 
    -- Metrics multiplied by 100
    (1 - s.churn_risk_score)* 100 AS CHURN_RISK_SCORE_HSCORE,
    s.sentiment_score * 100 AS SENTIMENT_SCORE_HSCORE,
    s.engagement_score * 100 AS ENGAGEMENT_SCORE_HSCORE,
    s.renewal_urgency_months as renewal_urgency_months_hscore, 
    s.revenue_growth_score as revenue_growth_hscore, 
    s.contract_length_score as contract_length_score_hscore, 

    -- Corresponding weights for the above (all 0)
   --  0 AS CHURN_RISK_SCORE_HSCORE_WEIGHTS,
   --  0 AS SENTIMENT_SCORE_HSCORE_WEIGHTS,
  --  0 AS ENGAGEMENT_SCORE_HSCORE_WEIGHTS,

    -- Percentile-based hscores (Direct scaling: higher metric value = higher score)
    CASE
        WHEN s.USER_ACTIVATION_PERCENTAGE IS NULL THEN NULL
        WHEN p.p1_user_activation IS NULL OR p.p99_user_activation IS NULL THEN 50 -- Percentiles not available
        WHEN s.USER_ACTIVATION_PERCENTAGE <= p.p1_user_activation THEN 0
        WHEN s.USER_ACTIVATION_PERCENTAGE >= p.p99_user_activation THEN 100
        ELSE IFF(p.p99_user_activation - p.p1_user_activation = 0, 50, GREATEST(0, LEAST(100, (s.USER_ACTIVATION_PERCENTAGE - p.p1_user_activation) * 100.0 / (p.p99_user_activation - p.p1_user_activation))))
    END AS USER_ACTIVATION_PERCENTAGE_HSCORE,


    CASE
        WHEN s.PRODUCT_STICKINESS_RATIO IS NULL THEN NULL
        WHEN p.p1_prod_stickiness IS NULL OR p.p99_prod_stickiness IS NULL THEN 50
        WHEN s.PRODUCT_STICKINESS_RATIO <= p.p1_prod_stickiness THEN 0
        WHEN s.PRODUCT_STICKINESS_RATIO >= p.p99_prod_stickiness THEN 100
        ELSE IFF(p.p99_prod_stickiness - p.p1_prod_stickiness = 0, 50, GREATEST(0, LEAST(100, (s.PRODUCT_STICKINESS_RATIO - p.p1_prod_stickiness) * 100.0 / (p.p99_prod_stickiness - p.p1_prod_stickiness))))
    END AS PRODUCT_STICKINESS_RATIO_HSCORE,

    CASE
        WHEN s.USER_STICKINESS_RATIO IS NULL THEN NULL
        WHEN p.p1_user_stickiness IS NULL OR p.p99_user_stickiness IS NULL THEN 50
        WHEN s.USER_STICKINESS_RATIO <= p.p1_user_stickiness THEN 0
        WHEN s.USER_STICKINESS_RATIO >= p.p99_user_stickiness THEN 100
        ELSE IFF(p.p99_user_stickiness - p.p1_user_stickiness = 0, 50, GREATEST(0, LEAST(100, (s.USER_STICKINESS_RATIO - p.p1_user_stickiness) * 100.0 / (p.p99_user_stickiness - p.p1_user_stickiness))))
    END AS USER_STICKINESS_RATIO_HSCORE,

    CASE
        WHEN s.PRODUCT_UTILIZATION_RATE IS NULL THEN NULL
        WHEN p.p1_prod_utilization IS NULL OR p.p99_prod_utilization IS NULL THEN 50
        WHEN s.PRODUCT_UTILIZATION_RATE <= p.p1_prod_utilization THEN 0
        WHEN s.PRODUCT_UTILIZATION_RATE >= p.p99_prod_utilization THEN 100
        ELSE IFF(p.p99_prod_utilization - p.p1_prod_utilization = 0, 50, GREATEST(0, LEAST(100, (s.PRODUCT_UTILIZATION_RATE - p.p1_prod_utilization) * 100.0 / (p.p99_prod_utilization - p.p1_prod_utilization))))
    END AS PRODUCT_UTILIZATION_RATE_HSCORE,

    -- CASE -- Renewal Urgency: Higher months until renewal = higher score (direct)
    --     WHEN s.RENEWAL_URGENCY_MONTHS IS NULL THEN NULL
    --     WHEN p.p1_renewal_urgency IS NULL OR p.p99_renewal_urgency IS NULL THEN NULL
    --     WHEN s.RENEWAL_URGENCY_MONTHS <= p.p1_renewal_urgency THEN 0
    --     WHEN s.RENEWAL_URGENCY_MONTHS >= p.p99_renewal_urgency THEN 100
    --     ELSE IFF(p.p99_renewal_urgency - p.p1_renewal_urgency = 0, 50, GREATEST(0, LEAST(100, (s.RENEWAL_URGENCY_MONTHS - p.p1_renewal_urgency) * 100.0 / (p.p99_renewal_urgency - p.p1_renewal_urgency))))
    -- END AS RENEWAL_URGENCY_MONTHS_HSCORE,

    CASE
        WHEN s.OVERAGE_SCORE IS NULL THEN NULL
        WHEN p.p1_overage IS NULL OR p.p99_overage IS NULL THEN 50
        WHEN s.OVERAGE_SCORE <= p.p1_overage THEN 0
        WHEN s.OVERAGE_SCORE >= p.p99_overage THEN 100
        ELSE IFF(p.p99_overage - p.p1_overage = 0, 50, GREATEST(0, LEAST(100, (s.OVERAGE_SCORE - p.p1_overage) * 100.0 / (p.p99_overage - p.p1_overage))))
    END AS OVERAGE_SCORE_HSCORE,

    /*
    CASE
        WHEN s.CONTRACT_LENGTH_SCORE IS NULL THEN NULL
        WHEN p.p1_contract_length IS NULL OR p.p99_contract_length IS NULL THEN NULL
        WHEN s.CONTRACT_LENGTH_SCORE <= p.p1_contract_length THEN 0
        WHEN s.CONTRACT_LENGTH_SCORE >= p.p99_contract_length THEN 100
        ELSE IFF(p.p99_contract_length - p.p1_contract_length = 0, 50, GREATEST(0, LEAST(100, (s.CONTRACT_LENGTH_SCORE - p.p1_contract_length) * 100.0 / (p.p99_contract_length - p.p1_contract_length))))
    END AS CONTRACT_LENGTH_SCORE_HSCORE,
    */

    CASE
        WHEN s.RESOLUTION_RATE IS NULL THEN 100
        WHEN p.p1_resolution_rate IS NULL OR p.p99_resolution_rate IS NULL THEN 50
        WHEN s.RESOLUTION_RATE <= p.p1_resolution_rate THEN 0
        WHEN s.RESOLUTION_RATE >= p.p99_resolution_rate THEN 100
        ELSE IFF(p.p99_resolution_rate - p.p1_resolution_rate = 0, 50, GREATEST(0, LEAST(100, (s.RESOLUTION_RATE - p.p1_resolution_rate) * 100.0 / (p.p99_resolution_rate - p.p1_resolution_rate))))
    END AS RESOLUTION_RATE_HSCORE,

    CASE -- Open Tickets: Higher number of tickets = higher score (as per specific instruction)
        WHEN s.OPEN_TICKETS IS NULL THEN 0
        WHEN p.p1_open_tickets IS NULL OR p.p99_open_tickets IS NULL THEN 50
        WHEN s.OPEN_TICKETS <= p.p1_open_tickets THEN 0
        WHEN s.OPEN_TICKETS >= p.p99_open_tickets THEN 100
        ELSE IFF(p.p99_open_tickets - p.p1_open_tickets = 0, 50, GREATEST(0, LEAST(100, (s.OPEN_TICKETS - p.p1_open_tickets) * 100.0 / (p.p99_open_tickets - p.p1_open_tickets))))
    END AS OPEN_TICKETS_HSCORE,

    CASE
        WHEN s.AVG_CSAT_SCORE IS NULL THEN 0
        WHEN p.p1_avg_csat IS NULL OR p.p99_avg_csat IS NULL THEN 0
        WHEN s.AVG_CSAT_SCORE <= p.p1_avg_csat THEN 0
        WHEN s.AVG_CSAT_SCORE >= p.p99_avg_csat THEN 100
        ELSE IFF(p.p99_avg_csat - p.p1_avg_csat = 0, 50, GREATEST(0, LEAST(100, (s.AVG_CSAT_SCORE - p.p1_avg_csat) * 100.0 / (p.p99_avg_csat - p.p1_avg_csat))))
    END AS AVG_CSAT_SCORE_HSCORE,

    -- Percentile-based hscores (Inverse scaling: higher metric value = lower score)
    CASE
        WHEN s.PCT_P1_CASES IS NULL THEN 100
        WHEN p.p1_pct_p1_cases IS NULL OR p.p99_pct_p1_cases IS NULL THEN 50
        WHEN s.PCT_P1_CASES <= p.p1_pct_p1_cases THEN 100 -- Lower actual value gets higher score
        WHEN s.PCT_P1_CASES >= p.p99_pct_p1_cases THEN 0   -- Higher actual value gets lower score
        ELSE IFF(p.p99_pct_p1_cases - p.p1_pct_p1_cases = 0, 50, GREATEST(0, LEAST(100, 100 - ( (s.PCT_P1_CASES - p.p1_pct_p1_cases) * 100.0 / (p.p99_pct_p1_cases - p.p1_pct_p1_cases) ))))
    END AS PCT_P1_CASES_HSCORE,

    CASE
        WHEN s.PCT_P1P2_CASES IS NULL THEN 100
        WHEN p.p1_pct_p1p2_cases IS NULL OR p.p99_pct_p1p2_cases IS NULL THEN 50
        WHEN s.PCT_P1P2_CASES <= p.p1_pct_p1p2_cases THEN 100
        WHEN s.PCT_P1P2_CASES >= p.p99_pct_p1p2_cases THEN 0
        ELSE IFF(p.p99_pct_p1p2_cases - p.p1_pct_p1p2_cases = 0, 50, GREATEST(0, LEAST(100, 100 - ( (s.PCT_P1P2_CASES - p.p1_pct_p1p2_cases) * 100.0 / (p.p99_pct_p1p2_cases - p.p1_pct_p1p2_cases) ))))
    END AS PCT_P1P2_CASES_HSCORE,

    CASE
        WHEN s.MTHLY_FTTR_50 IS NULL THEN 100
        WHEN p.p1_mthly_fttr IS NULL OR p.p99_mthly_fttr IS NULL THEN 50
        WHEN s.MTHLY_FTTR_50 <= p.p1_mthly_fttr THEN 100
        WHEN s.MTHLY_FTTR_50 >= p.p99_mthly_fttr THEN 0
        ELSE IFF(p.p99_mthly_fttr - p.p1_mthly_fttr = 0, 50, GREATEST(0, LEAST(100, 100 - ( (s.MTHLY_FTTR_50 - p.p1_mthly_fttr) * 100.0 / (p.p99_mthly_fttr - p.p1_mthly_fttr) ))))
    END AS MTHLY_FTTR_50_HSCORE,

    CASE
        WHEN s.MAXIMUM_DAYS_PAST_DUE IS NULL THEN 100
        WHEN p.p1_max_days_past_due IS NULL OR p.p99_max_days_past_due IS NULL THEN 50
        WHEN s.MAXIMUM_DAYS_PAST_DUE <= p.p1_max_days_past_due THEN 100
        WHEN s.MAXIMUM_DAYS_PAST_DUE >= p.p99_max_days_past_due THEN 0
        ELSE IFF(p.p99_max_days_past_due - p.p1_max_days_past_due = 0, 50, GREATEST(0, LEAST(100, 100 - ( (s.MAXIMUM_DAYS_PAST_DUE - p.p1_max_days_past_due) * 100.0 / (p.p99_max_days_past_due - p.p1_max_days_past_due) ))))
    END AS MAXIMUM_DAYS_PAST_DUE_HSCORE,

    CASE
        WHEN s.SUM_TOTAL_AMOUNT_DUE IS NULL THEN 100
        WHEN p.p1_sum_total_due IS NULL OR p.p99_sum_total_due IS NULL THEN 50
        WHEN s.SUM_TOTAL_AMOUNT_DUE <= p.p1_sum_total_due THEN 100
        WHEN s.SUM_TOTAL_AMOUNT_DUE >= p.p99_sum_total_due THEN 0
        ELSE IFF(p.p99_sum_total_due - p.p1_sum_total_due = 0, 50, GREATEST(0, LEAST(100, 100 - ( (s.SUM_TOTAL_AMOUNT_DUE - p.p1_sum_total_due) * 100.0 / (p.p99_sum_total_due - p.p1_sum_total_due) ))))
    END AS SUM_TOTAL_AMOUNT_DUE_HSCORE,

/*    -- Weights for percentile-based hscores (all 0)
    0 AS USER_ACTIVATION_PERCENTAGE_HSCORE_WEIGHTS,
    0 AS PRODUCT_STICKINESS_RATIO_HSCORE_WEIGHTS,
    0 AS USER_STICKINESS_RATIO_HSCORE_WEIGHTS,
    0 AS PRODUCT_UTILIZATION_RATE_HSCORE_WEIGHTS,
    0 AS RENEWAL_URGENCY_MONTHS_HSCORE_WEIGHTS,
    0 AS OVERAGE_SCORE_HSCORE_WEIGHTS,
    0 AS CONTRACT_LENGTH_SCORE_HSCORE_WEIGHTS,
    0 AS PCT_P1_CASES_HSCORE_WEIGHTS,
    0 AS PCT_P1P2_CASES_HSCORE_WEIGHTS,
    0 AS RESOLUTION_RATE_HSCORE_WEIGHTS,
    0 AS OPEN_TICKETS_HSCORE_WEIGHTS,
    0 AS AVG_CSAT_SCORE_HSCORE_WEIGHTS,
    0 AS MTHLY_FTTR_50_HSCORE_WEIGHTS,
    0 AS MAXIMUM_DAYS_PAST_DUE_HSCORE_WEIGHTS,
    0 AS SUM_TOTAL_AMOUNT_DUE_HSCORE_WEIGHTS */
from dev.gtm.spreetham_customer_360_v5 s
cross join percentiles p 
), 

intermediate as (
SELECT subscription_account_id,
    report_as_of_dt, 
    CAST(product_stickiness_ratio_hscore AS DECIMAL) AS product_stickiness_ratio_hscore,
    CAST(user_stickiness_ratio_hscore AS DECIMAL) AS user_stickiness_ratio_hscore,
    CAST(user_activation_percentage_hscore AS DECIMAL) AS user_activation_percentage_hscore,
    CAST(product_utilization_rate_hscore AS DECIMAL) AS product_utilization_rate_hscore,
    CAST(revenue_growth_hscore AS DECIMAL) AS revenue_growth_hscore,
    CAST(renewal_urgency_months_hscore AS DECIMAL) AS renewal_urgency_months_hscore,
    CAST(overage_score_hscore AS DECIMAL) AS overage_score_hscore,
    CAST(contract_length_score_hscore AS DECIMAL) AS contract_length_score_hscore,
    CAST(pct_P1_cases_hscore AS DECIMAL) AS pct_P1_cases_hscore,
    CAST(pct_P1P2_cases_hscore AS DECIMAL) AS pct_P1P2_cases_hscore,
    CAST(resolution_rate_hscore AS DECIMAL) AS resolution_rate_hscore,
    CAST(open_tickets_hscore AS DECIMAL) AS open_tickets_hscore,
    CAST(avg_csat_score_hscore AS DECIMAL) AS avg_csat_score_hscore,
    CAST(mthly_fttr_50_hscore AS DECIMAL) AS mthly_fttr_50_hscore,
    CAST(churn_risk_score_hscore AS DECIMAL) AS churn_risk_score_hscore,
    CAST(sentiment_score_hscore AS DECIMAL) AS sentiment_score_hscore,
    CAST(engagement_score_hscore AS DECIMAL) AS engagement_score_hscore,
    CAST(MAXIMUM_DAYS_PAST_DUE_hscore AS DECIMAL) AS MAXIMUM_DAYS_PAST_DUE_hscore,
    CAST(SUM_TOTAL_AMOUNT_DUE_hscore AS DECIMAL) AS SUM_TOTAL_AMOUNT_DUE_hscore
FROM scores_calculation
), 

intermediate_2 as 
(
select *, 
-- Adoption Score (handles division by zero and all nulls = 0)
CASE 
  WHEN (CASE WHEN product_stickiness_ratio_hscore IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN user_stickiness_ratio_hscore IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN user_activation_percentage_hscore IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN product_utilization_rate_hscore IS NOT NULL THEN 1 ELSE 0 END) = 0 
  THEN 0
  ELSE round((
    COALESCE(product_stickiness_ratio_hscore, 0) + 
    COALESCE(user_stickiness_ratio_hscore, 0) + 
    COALESCE(user_activation_percentage_hscore, 0) + 
    COALESCE(product_utilization_rate_hscore, 0)
  ) / (
    CASE WHEN product_stickiness_ratio_hscore IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN user_stickiness_ratio_hscore IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN user_activation_percentage_hscore IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN product_utilization_rate_hscore IS NOT NULL THEN 1 ELSE 0 END
  ))
END as adoption_score_cal,

-- Engagement Score (handles division by zero and all nulls = 0)
CASE 
  WHEN (CASE WHEN avg_csat_score_hscore IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN open_tickets_hscore IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN engagement_score_hscore IS NOT NULL THEN 1 ELSE 0 END) = 0 
  THEN 0
  ELSE round((
    COALESCE(avg_csat_score_hscore, 0) + 
    COALESCE(open_tickets_hscore, 0) + 
    COALESCE(engagement_score_hscore, 0)
  ) / (
    CASE WHEN avg_csat_score_hscore IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN open_tickets_hscore IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN engagement_score_hscore IS NOT NULL THEN 1 ELSE 0 END
  ))
END as engagement_score_cal,

-- Financial Health Score (handles division by zero and all nulls = 0)
CASE 
  WHEN (CASE WHEN revenue_growth_hscore IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN renewal_urgency_months_hscore IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN overage_score_hscore IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN contract_length_score_hscore IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN MAXIMUM_DAYS_PAST_DUE_hscore IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN SUM_TOTAL_AMOUNT_DUE_hscore IS NOT NULL THEN 1 ELSE 0 END) = 0 
  THEN 0
  ELSE round((
    COALESCE(revenue_growth_hscore, 0) + 
    COALESCE(renewal_urgency_months_hscore, 0) + 
    COALESCE(overage_score_hscore, 0) + 
    COALESCE(contract_length_score_hscore, 0) + 
    COALESCE(MAXIMUM_DAYS_PAST_DUE_hscore, 0) + 
    COALESCE(SUM_TOTAL_AMOUNT_DUE_hscore, 0)
  ) / (
    CASE WHEN revenue_growth_hscore IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN renewal_urgency_months_hscore IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN overage_score_hscore IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN contract_length_score_hscore IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN MAXIMUM_DAYS_PAST_DUE_hscore IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN SUM_TOTAL_AMOUNT_DUE_hscore IS NOT NULL THEN 1 ELSE 0 END
  ))
END as Financial_health_score_cal,

-- Risk Score (handles division by zero and all nulls = 0)
CASE 
  WHEN (CASE WHEN pct_P1_cases_hscore IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN pct_P1P2_cases_hscore IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN resolution_rate_hscore IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN mthly_fttr_50_hscore IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN churn_risk_score_hscore IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN sentiment_score_hscore IS NOT NULL THEN 1 ELSE 0 END) = 0 
  THEN 0
  ELSE round((
    COALESCE(pct_P1_cases_hscore, 0) + 
    COALESCE(pct_P1P2_cases_hscore, 0) + 
    COALESCE(resolution_rate_hscore, 0) + 
    COALESCE(mthly_fttr_50_hscore, 0) + 
    COALESCE(churn_risk_score_hscore, 0) + 
    COALESCE(sentiment_score_hscore, 0)
  ) / (
    CASE WHEN pct_P1_cases_hscore IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN pct_P1P2_cases_hscore IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN resolution_rate_hscore IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN mthly_fttr_50_hscore IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN churn_risk_score_hscore IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN sentiment_score_hscore IS NOT NULL THEN 1 ELSE 0 END
  ))
END as Risk_score_cal
from intermediate 
), 

-- select * from intermediate_2 where subscription_account_id = '1709707' and report_as_of_dt = '2025-04-30'; 

final_query as 
(
select * from intermediate_2
unpivot INCLUDE NULLS(
    metric_score for metric_name_score in (
       product_stickiness_ratio_hscore,
       user_stickiness_ratio_hscore,
       user_activation_percentage_hscore, 
       product_utilization_rate_hscore,
       revenue_growth_hscore,
       renewal_urgency_months_hscore,
       overage_score_hscore,
       contract_length_score_hscore, 
       pct_P1_cases_hscore,
       pct_P1P2_cases_hscore,
       resolution_rate_hscore,
       open_tickets_hscore,
       avg_csat_score_hscore,
       mthly_fttr_50_hscore,
       churn_risk_score_hscore,
       sentiment_score_hscore,
       engagement_score_hscore, 
       MAXIMUM_DAYS_PAST_DUE_hscore, 
       SUM_TOTAL_AMOUNT_DUE_hscore 
   )
)) 

select b.*, f.metric_score, f.metric_name_score, 0 as weights, 
       f.adoption_score_cal, 
       f.engagement_score_cal, 
       f.financial_health_score_cal, 
       f.risk_score_cal, 
       (f.adoption_score_cal + f.engagement_score_cal + f.financial_health_score_cal + f.risk_score_cal)/4 as health_score 
from base_2 b 
left join final_query f on b.subscription_account_id = f.subscription_account_id and 
b.report_as_of_dt = f.report_as_of_dt and 
b.metric_name = left(f.metric_name_score, len(f.metric_name_score) - LEN('_HSCORE'));  
-- where b.subscription_account_id = '1709707' and b.report_as_of_dt = '2025-04-30'; 

/* 
SELECT 
    typeof(product_stickiness_ratio_hscore) AS psr,
    typeof(user_stickiness_ratio_hscore) AS usr,
    typeof(user_activation_percentage_hscore) AS uap,
    typeof(product_utilization_rate_hscore) AS pur,
    typeof(revenue_growth_hscore) AS rg,
    typeof(renewal_urgency_months_hscore) AS rum,
    typeof(overage_score_hscore) AS os,
    typeof(contract_length_score_hscore) AS cls, 
    typeof(pct_P1_cases_hscore) as pp1,
    typeof(pct_P1P2_cases_hscore) as pp2,
    typeof(resolution_rate_hscore) as rrh,
    typeof(open_tickets_hscore) as oth,
    typeof(avg_csat_score_hscore) as acs,
    typeof(mthly_fttr_50_hscore) as mft,
    typeof(churn_risk_score_hscore) as crs,
    typeof(sentiment_score_hscore) as ssh,
    typeof(engagement_score_hscore) as esh, 
    typeof(MAXIMUM_DAYS_PAST_DUE_hscore) as mdph, 
    typeof(SUM_TOTAL_AMOUNT_DUE_hscore) as stah 
FROM scores_calculation
LIMIT 10;
*/

-- show grants on table dev.gtm.spreetham_customer_360_v1;


select subscription_account_id, report_as_of_dt, metric_name, metric_score, adoption_score_cal, 
engagement_score_cal, financial_health_score_cal, risk_score_cal,
(adoption_score_cal + engagement_score_cal + financial_health_score_cal + risk_score_cal) / 4.0 AS overall_health_score
from dev.gtm.customer_360_v5_hscores 
where subscription_account_id = '2416082' ;

and report_as_of_dt = '2025-02-28';  

select subscription_account_id, report_as_of_dt, metric_name
from dev.gtm.spreetham_customer_360_v5 where subscription_account_id = '1709707' and report_as_of_dt = '2025-02-28';
