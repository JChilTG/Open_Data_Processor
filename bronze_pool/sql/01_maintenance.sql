/* ============================================================================
   Bronze maintenance for the edw dedicated SQL pool
   ----------------------------------------------------------------------------
   WHY: This loader is append-only and writes many SMALL batches into CLUSTERED
   COLUMNSTORE tables. Small/trickle inserts create lots of small, open
   row-groups (< ~1M rows), which hurts compression and scan performance and
   bloats the deltastore. Run this periodically (e.g. nightly, after loads, or
   weekly) to compact row-groups and refresh stats.

   Safe to re-run. Adjust the schema name if you changed `target_schema`.
   ============================================================================ */

DECLARE @schema SYSNAME = 'brz';
DECLARE @sql NVARCHAR(MAX) = N'';

-- 1. Rebuild columnstore on bronze tables to compact row-groups.
--    (Skip the transient *__stage tables.)
SELECT @sql = @sql +
       'ALTER INDEX ALL ON ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) +
       ' REBUILD;' + CHAR(10)
FROM   sys.tables  t
JOIN   sys.schemas s ON s.schema_id = t.schema_id
WHERE  s.name = @schema
AND    t.name NOT LIKE '%\_\_stage' ESCAPE '\';

EXEC sp_executesql @sql;

-- 2. Refresh statistics so the optimizer (and dbt models) get good plans.
SET @sql = N'';
SELECT @sql = @sql +
       'UPDATE STATISTICS ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) + ';' + CHAR(10)
FROM   sys.tables  t
JOIN   sys.schemas s ON s.schema_id = t.schema_id
WHERE  s.name = @schema
AND    t.name NOT LIKE '%\_\_stage' ESCAPE '\';

EXEC sp_executesql @sql;

/* Inspect row-group health (run ad hoc):

   SELECT OBJECT_NAME(object_id) AS table_name,
          state_desc, COUNT(*) AS rowgroups, SUM(total_rows) AS rows
   FROM   sys.dm_pdw_nodes_db_column_store_row_group_physical_stats
   GROUP  BY OBJECT_NAME(object_id), state_desc
   ORDER  BY table_name, state_desc;
*/
