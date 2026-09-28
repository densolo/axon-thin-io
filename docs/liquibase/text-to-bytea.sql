--liquibase formatted sql
--
-- Axon event tables: payload / meta_data from text to bytea (back to Axon-compatible binary).
-- Tables: ${axon_prefix}domain_event_entry and ${axon_prefix}snapshot_event_entry
-- (set the axon_prefix changelog parameter, e.g. axon_, or empty for Axon's default names).
--
-- Stop every application writing these tables first: axon-thin detects the column type on first use per process,
-- so the restart afterwards picks up the new type. An Axon 4 process needs its own mapping change (see docs/liquibase/README.md).
-- ALTER ... TYPE rewrites and locks each table: plan a maintenance window for big tables; test on a copy first.

--changeset axon-thin:axon-payload-text-to-bytea dbms:postgresql
alter table ${axon_prefix}domain_event_entry
    alter column payload type bytea using convert_to(payload, 'UTF8'),
    alter column meta_data type bytea using convert_to(meta_data, 'UTF8');
alter table ${axon_prefix}snapshot_event_entry
    alter column payload type bytea using convert_to(payload, 'UTF8'),
    alter column meta_data type bytea using convert_to(meta_data, 'UTF8');
--rollback alter table ${axon_prefix}domain_event_entry alter column payload type text using convert_from(payload, 'UTF8'), alter column meta_data type text using convert_from(meta_data, 'UTF8');
--rollback alter table ${axon_prefix}snapshot_event_entry alter column payload type text using convert_from(payload, 'UTF8'), alter column meta_data type text using convert_from(meta_data, 'UTF8');
