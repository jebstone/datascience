CREATE PROCEDURAL LANGUAGE 'plpythonu' HANDLER plpython_call_handler; 

CREATE OR REPLACE FUNCTION count_missing (table_name text)
RETURNS TABLE (column_name text, column_type text, record_cnt integer, missing_cnt integer, missing_pct float)
AS $$

null_or_blank = ['character', 'character varying', 'json', 'text']

sql = "SELECT attname AS col, atttypid::regtype AS datatype FROM pg_attribute WHERE attrelid = 'public.{}'::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum;".format(table_name)
column_info = plpy.execute(sql)
column_cnt = column_info.nrows()

results_table = []

for i in range(0, column_cnt):
  sql = ""
  column_name = column_info[i]["col"]
  column_type = column_info[i]["datatype"]

  if ( column_type in null_or_blank ):
    sql = "SELECT count(*) AS record_cnt, sum(case when {1} is null or trim(both ' ' from {1})  = '' then 1 else 0 end) AS missing_cnt, sum(case when {1} is null or trim(both ' ' from {1}) = '' then 1 else 0 end) * 1.0 / count(*) AS missing_pct from {0};".format(table_name, column_name)

  else:
    sql = "SELECT count(*) AS record_cnt, sum(case when {1} is null then 1 else 0 end) AS missing_cnt, sum(case when {1} is null then 1 else 0 end) * 1.0 / count(*) AS missing_pct from {0};".format(table_name, column_name)

  column_stats = plpy.execute(sql)
  record_cnt = column_stats[0]["record_cnt"]
  missing_cnt = column_stats[0]["missing_cnt"]
  missing_pct = round(column_stats[0]["missing_pct"], 3)

  results_table.append([column_name, column_type, record_cnt, missing_cnt, missing_pct])
  
return results_table
$$ LANGUAGE plpythonu;

select * from count_missing('my_tablename')