-- rillkit-server schema, version 1.

create table tenants (
    id            text primary key,
    name          text not null,
    enforce_keys  boolean not null default false,  -- Cloud mode: workers must use HashTagKeys("rk", id)
    created_at    timestamptz not null default now()
);

create table api_keys (
    id          text primary key,
    tenant_id   text not null references tenants(id) on delete cascade,
    label       text,
    prefix      text not null,                 -- shown in the dashboard; the key itself is never stored
    hash        text not null unique,          -- sha256 of the key
    created_at  timestamptz not null default now(),
    revoked_at  timestamptz
);
create index api_keys_tenant on api_keys(tenant_id);

-- The latest view of every worker node, written by whichever replica holds its Relay connection.
create table nodes (
    tenant_id     text not null references tenants(id) on delete cascade,
    node_id       text not null,
    replica_id    text,
    key_id        text,
    connected     boolean not null default false,
    keys          jsonb,
    sdk           jsonb,
    telemetry     jsonb,
    connected_at  timestamptz,
    last_seen     timestamptz not null default now(),
    primary key (tenant_id, node_id)
);

create table directives (
    id            text primary key,
    tenant_id     text not null references tenants(id) on delete cascade,
    cmd           text not null,
    scope         text not null,
    target        jsonb not null default '{}',
    params        jsonb not null default '{}',
    reason        text,
    source        text not null default 'user',   -- user | autoscaler
    status        text not null default 'PENDING',
    sent_to       text,                           -- node id it was sent to
    delivered_by  text,                           -- replica that sent it
    ack           jsonb,
    created_at    timestamptz not null default now(),
    updated_at    timestamptz not null default now()
);
create index directives_tenant_created on directives(tenant_id, created_at desc);
create index directives_pending on directives(status) where status in ('PENDING', 'SENT');

create table policies (
    tenant_id               text not null references tenants(id) on delete cascade,
    stream                  text not null,
    "group"                 text not null,
    enabled                 boolean not null default true,
    min_workers             integer not null,
    max_workers             integer not null,
    target_lag_per_worker   integer not null,
    upscale_cooldown_sec    integer not null default 30,
    downscale_cooldown_sec  integer not null default 120,
    last_scaled_at          timestamptz,
    last_decision           text,
    primary key (tenant_id, stream, "group"),
    check (min_workers >= 0 and max_workers >= min_workers and target_lag_per_worker > 0)
);

-- One row per stream and group every autoscaler tick, kept for a day (dashboard charts).
create table samples (
    tenant_id     text not null references tenants(id) on delete cascade,
    stream        text not null,
    "group"       text not null,
    ts            timestamptz not null default now(),
    lag           bigint,
    pending       bigint,
    concurrency   integer not null,
    ack_rate_sec  double precision not null
);
create index samples_lookup on samples(tenant_id, stream, "group", ts);

-- Answers to LOGS_REQUEST relayed between replicas (too large for a NOTIFY payload).
create table log_responses (
    request_id  text primary key,
    payload     jsonb not null,
    created_at  timestamptz not null default now()
);
