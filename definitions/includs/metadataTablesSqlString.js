function getLatestObject(source_dataset_ref, destination_dataset_ref, metadata_files_list) {
  let sql_loop_operations = []
  let sql_declare_operations = []
  let sql_block_operations = []
  let build_sql_query = ""
  let sql_query = ""

  for (i=0; i <= metadata_files_list.length - 1; i++) {
  
    sql_query = `
    -- HEADER
      DECLARE 
        source_dataset_ref,
        destination_dataset_ref,
        source_table_ref,
        destination_table_ref
      STRING;

      DECLARE
        metadata_files_list,
        delivery_files_list
      ARRAY<STRING>;

      DECLARE 
        latest_date
      DATE;


    -- CONFIGURATION
      SET source_dataset_ref = \`${source_dataset_ref}\`; 
      SET destination_dataset_ref = \`${destination_dataset_ref}\`; 
      SET metadata_files_list = ${metadata_files_list};

    -- FUNCTIONS
      CREATE TEMP FUNCTION get_latest_date_sql(source_dataset_ref STRING, source_table_name STRING) AS (
        FORMAT("""
        SELECT 
        MAX(source_file_date) AS max_source_file_date
        FROM \`%s.%s\`
        """,
        source_dataset_ref,
        source_table_name
        )); 

      CREATE TEMP FUNCTION create_table_from_latest_partition_sql(destination_ref STRING, source_ref STRING, partition_date DATE) AS (
        FORMAT("""
        CREATE OR REPLACE TABLE \`%s\`
        AS
        SELECT *
        FROM %s
        WHERE source_file_date = '%t'
        """,
        destination_ref,
        source_ref,
        partition_date
        ));

    -- EXECUTION (METADATA FILES)

    --FOR file IN (SELECT name FROM UNNEST(metadata_files_list) AS name)
    --DO

    SET source_table_ref = \`${source_dataset_ref}.${metadata_files_list[i]}\`;
    SET destination_table_ref = \`${destination_dataset_ref}.${metadata_files_list[i]}\`;

    EXECUTE IMMEDIATE(SELECT get_latest_date_sql(${source_dataset_ref}, ${metadata_files_list[i]})) INTO latest_date;
    EXECUTE IMMEDIATE (create_table_from_latest_partition_sql(destination_table_ref, source_table_ref, latest_date));

  --END FOR;

  --TRUNCATE TABLE \`milespartnership-data-lake.Centro_prod.creative_daily_delivery\`;
  --TRUNCATE TABLE \`milespartnership-data-lake.Centro_prod.line_item_daily_delivery\`;


  SELECT * 
  FROM \`milespartnership-data-lake.Centro_staging.get_creative_daily_delivery\`('2023-01-09')
  ORDER BY line_item_lineage, delivery_date DESC;
  `
}

  sql_loop_operations.push(sql_query)
  return sql_loop_operations.join("\n")
  }

module.exports = { getLatestObject };

