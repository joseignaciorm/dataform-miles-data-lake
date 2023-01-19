function renderMultipleExternalTables(dataset_name_source, table_preffix_source, dataset_name_destination, date_range) {
    sql_operations = []
    for (i=1; i <= date_range.length - 1; i++) {
        sql_operations.push(
            `
            CREATE OR REPLACE TABLE \`milespartnership-data-lake.${dataset_name_destination}.delivery_source_metadata_${date_range[i]}\` AS (
            SELECT
                *
            FROM \`milespartnership-data-lake.${dataset_name_source}.${table_preffix_source}${date_range[i]}\`
            )`
        )
    }
    return sql_operations.join(";\n")
};

module.exports = { renderMultipleExternalTables };