-- Test schema: Axon event tables with payload/meta_data as text.
create sequence if not exists axon_domain_event_entry_seq start with 1 increment by 50;

create table if not exists axon_domain_event_entry (
    global_index         bigint       not null primary key,
    sequence_number      bigint       not null,
    aggregate_identifier varchar(255) not null,
    event_identifier     varchar(255) not null unique,
    payload_revision     varchar(255),
    payload_type         varchar(255) not null,
    time_stamp           varchar(255) not null,
    type                 varchar(255),
    meta_data            text,
    payload              text        not null,
    constraint axon_domain_event_entry_aggregate_seq unique (aggregate_identifier, sequence_number)
);

create table if not exists axon_snapshot_event_entry (
    sequence_number      bigint       not null,
    aggregate_identifier varchar(255) not null,
    event_identifier     varchar(255) not null unique,
    payload_revision     varchar(255),
    payload_type         varchar(255) not null,
    time_stamp           varchar(255) not null,
    type                 varchar(255) not null,
    meta_data            text,
    payload              text        not null,
    primary key (sequence_number, aggregate_identifier, type)
);
