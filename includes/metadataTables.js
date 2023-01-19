function renderMetadataTables(source_dataset_ref, destination_dataset_ref, source_table) {
        let sql_declare_operations = []
        let sql_block_operations = []
        let build_sql_query = ""

        for (i=0; i <= source_table.length - 1; i++) {
                sql_declare_operations.push(
                        `
                        DECLARE latest_source_${source_table[i]}_file_date DATE 
                        DEFAULT (SELECT MAX(source_file_date) as max_source_file_date 
                                FROM \`${source_dataset_ref}.${source_table[i]}\`);
                        `
                )

                sql_block_operations.push(
                        `
                        CREATE OR REPLACE TABLE \`${destination_dataset_ref}.${source_table[i]}\` AS (
                        SELECT *
                        FROM \`${source_dataset_ref}.${source_table[i]}\` 
                        WHERE source_file_date = latest_source_${source_table[i]}_file_date
                        );
                        `
                )
        }

        build_sql_query = `
                ${sql_declare_operations.join("\n")}
                ${sql_block_operations.join("\n")} 
        `
        return build_sql_query 
}

module.exports = {  renderMetadataTables };



