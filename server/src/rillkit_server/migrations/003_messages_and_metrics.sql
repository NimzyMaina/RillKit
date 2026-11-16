-- What each node lets the dashboard see (HELLO capabilities).
alter table nodes add column capabilities jsonb not null default '{}';

-- Streams created from the dashboard, listed even before any worker consumes them.
create table streams (
    tenant_id   text not null references tenants(id) on delete cascade,
    stream      text not null,
    "group"     text not null,
    created_at  timestamptz not null default now(),
    primary key (tenant_id, stream, "group")
);

-- Throughput per autoscaler tick: deltas of the workers' counters, summed over nodes.
alter table samples
    add column succeeded bigint not null default 0,
    add column failed bigint not null default 0,
    add column dead_lettered bigint not null default 0,
    add column bytes bigint not null default 0,
    add column dlq_length bigint;
create index samples_tenant_ts on samples(tenant_id, ts);

-- Answers relayed between replicas are no longer only logs.
alter table log_responses rename to relay_responses;
