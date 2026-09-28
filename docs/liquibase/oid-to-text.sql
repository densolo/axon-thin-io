--liquibase formatted sql
--
-- Axon event tables: payload / meta_data from oid (large objects) to text (readable JSON; Axon 4 cannot map it).
-- Tables: ${axon_prefix}domain_event_entry and ${axon_prefix}snapshot_event_entry
-- (set the axon_prefix changelog parameter, e.g. axon_, or empty for Axon's default names).
--
-- Stop every application writing these tables first: axon-thin detects the column type on first use per process,
-- so the restart afterwards picks up the new type. An Axon 4 process needs its own mapping change (see docs/liquibase/README.md).
-- ALTER ... TYPE rewrites and locks each table: plan a maintenance window for big tables; test on a copy first.

--changeset axon-thin:axon-payload-oid-to-text dbms:postgresql
create table axon_thin_lo_unlink as
    select payload as lo from ${axon_prefix}domain_event_entry
    union select meta_data from ${axon_prefix}domain_event_entry where meta_data is not null
    union select payload from ${axon_prefix}snapshot_event_entry
    union select meta_data from ${axon_prefix}snapshot_event_entry where meta_data is not null;
alter table ${axon_prefix}domain_event_entry
    alter column payload type text using convert_from(lo_get(payload), 'UTF8'),
    alter column meta_data type text using convert_from(lo_get(meta_data), 'UTF8');
alter table ${axon_prefix}snapshot_event_entry
    alter column payload type text using convert_from(lo_get(payload), 'UTF8'),
    alter column meta_data type text using convert_from(lo_get(meta_data), 'UTF8');
-- the contents now live in the columns: free the large objects (they are not removed with the rows)
select lo_unlink(lo) from axon_thin_lo_unlink;
drop table axon_thin_lo_unlink;
--rollback alter table ${axon_prefix}domain_event_entry alter column payload type oid using lo_from_bytea(0, convert_to(payload, 'UTF8')), alter column meta_data type oid using lo_from_bytea(0, convert_to(meta_data, 'UTF8'));
--rollback alter table ${axon_prefix}snapshot_event_entry alter column payload type oid using lo_from_bytea(0, convert_to(payload, 'UTF8')), alter column meta_data type oid using lo_from_bytea(0, convert_to(meta_data, 'UTF8'));
