-- For use with this template: https://docs.google.com/spreadsheets/d/1O-CTFnCiXs9y9HgE6BrUoqSpo5PBI_2N/edit#gid=75149427

/*

Only grab orders from certain card brands

SELECT transaction.order_id
    FROM phoebe.api_grains
    WHERE date >= '20210501'
            AND date <= '20210531' -- Input day after end date 
            AND customer_id = '5a0b5eeb4f0c3fee48400b5b'
            AND type = '$transaction'
            AND transaction.billing_bin != ''
            AND cast(transaction.billing_bin as int) between 400000 and 499999)

*/

with scored_grains AS (
  WITH grains AS 
    (SELECT date,
         customer_id,
         type,
         user_id, create_order.order_id,
         time_millis
    FROM phoebe.api_grains
    WHERE date >= '20210501'
            AND date <= '20210531' -- Input day after end date 
            AND customer_id = '5a0b5eeb4f0c3fee48400b5b'
            AND type = '$create_order'
    --         AND create_order.order_id in (SELECT transaction.order_id
    -- FROM phoebe.api_grains
    -- WHERE date >= '20210501'
    --         AND date <= '20210531' -- Input day after end date 
    --         AND customer_id = '5a0b5eeb4f0c3fee48400b5b'
    --         AND type = '$transaction'
    --         AND transaction.billing_bin != ''
    --         AND cast(transaction.billing_bin as int) between 400000 and 499999)
    ), 
  scores AS 
        (SELECT date,
         customer_id,
         key,
         -- for Legacy or Account Abuse, use Fraud with captial F, otherwise abuse_type with $
         scoring_result.scores['$payment_abuse'] AS score, scoring_result, occurred_at, created_time
        FROM phoebe.scores
        CROSS JOIN unnest(scoring_results) AS t(model_name, scoring_result)
        WHERE date >= '20210501'
                AND date <= '20210531' -- Input day after end date 
                AND customer_id = '5a0b5eeb4f0c3fee48400b5b'
                AND source.source_type='GRAIN'
                AND scoring_result.is_primary_score=true
                -- for Legacy or Account Abuse, use Fraud with captial F, otherwise abuse_type without $
                AND scoring_result.task='payment_abuse'
    ), 
  ranked_scored_grains AS 
            (SELECT grains.*,
         cast(round(scores.score*100,
        0) AS tinyint) AS score,
         rank()
                OVER (partition by user_id, grains.type, scores.occurred_at
            ORDER BY  created_time asc) AS rank
            FROM grains
            JOIN scores
                ON grains.user_id = scores.key
                    AND grains.time_millis = scores.occurred_at)
            SELECT *
            FROM ranked_scored_grains
            WHERE rank=1 
  ), 
decisions AS (
  WITH raw_decisions AS 
                (SELECT date,
         customer_id,
         associated_user_id,
         entity_id,
         decision_config.slug AS decision_id,
         decision_config.type AS decision_type,
         reason,
         origin,
         analyst_id,
         time,
         created_at
                FROM phoebe.decisions
                WHERE date >= '20210501'
                        AND customer_id = '5a0b5eeb4f0c3fee48400b5b'
                 -- for Legacy, use lowercase legacy, otherwise abuse_type without $ (incl account abuse) 
                        AND abuse_type=upper('payment_abuse')
                 -- You can limit to a certain decision type, decision config(s), or source (or more!)                         -- AND entity_type=upper('user')
                        -- AND (decision_config.slug = 'shadowban_user_sf_content_abuse'
                        -- OR decision_config.slug = 'suspend_user_sf_content_abuse')
                        -- AND reason = 'MANUAL_REVIEW' --> You 
    ), 
  ranked_decisions AS 
                    (SELECT *, rank() OVER (partition by associated_user_id , entity_id
                    ORDER BY  time desc) AS rank
                    FROM raw_decisions )
                    SELECT *
                    FROM ranked_decisions
                    WHERE rank = 1 
  )
SELECT scored_grains.score as score, count(*) as event_count,
      SUM(CASE
     WHEN decision_type = 'ACCEPT' THEN 1 ELSE 0 END) AS total_accept,
      SUM(CASE
     WHEN decision_type = 'WATCH' THEN 1 ELSE 0 END) AS total_watch,
      SUM(CASE
     WHEN decision_type = 'BLOCK' THEN 1 ELSE 0 END) AS total_block,
      SUM(CASE
     WHEN reason = 'MANUAL_REVIEW' AND (decision_type = 'ACCEPT' OR decision_type = 'BLOCK') THEN 1 ELSE 0 END) AS sample_size,
      SUM(CASE
     WHEN reason = 'MANUAL_REVIEW' AND decision_type = 'ACCEPT' THEN 1 ELSE 0 END) AS manual_accept,
     SUM(CASE
     WHEN reason = 'MANUAL_REVIEW' AND decision_type = 'WATCH' THEN 1 ELSE 0 END) AS manual_watch,
     SUM(CASE
     WHEN reason = 'MANUAL_REVIEW' AND decision_type = 'BLOCK' THEN 1 ELSE 0 END) AS manual_block,
     SUM(CASE
     WHEN reason = 'AUTOMATED_RULE' AND decision_type = 'BLOCK' THEN 1 ELSE 0 END) AS auto_block
FROM scored_grains
LEFT JOIN decisions
ON decisions.entity_id = scored_grains.user_id
GROUP BY scored_grains.score
ORDER BY 1;
