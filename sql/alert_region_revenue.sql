-- Databricks SQL alert: "Regional revenue shift"
-- Trigger condition: alerts > 0      Schedule: after the pipeline job
-- Replace the schema with yours (<catalog>.<project>_<env>_gold); notebook 06 prints it ready to paste.
SELECT count(*)                                                        AS alerts,
       concat_ws('; ', collect_list(concat(region_name, ': ', alert))) AS details
FROM workspace.tpch_dev_gold.region_revenue_alerts
WHERE is_latest_month
  AND alert IS NOT NULL;
