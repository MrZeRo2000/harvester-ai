DROP TABLE IF EXISTS public.snapshot_changes_ai;

create table IF NOT EXISTS public.snapshot_changes_ai
(
    id                bigserial  primary key,
    log_id            bigint,
    activity_duration text,
    customer_id       integer,
    project           varchar(255),
    subproject        varchar(255),
    description       text,
    control_sum       varchar(64),
    update_date       timestamp with time zone
);

create unique index snapshot_changes_ai_log_id_uindex
    on public.snapshot_changes_ai (log_id);

