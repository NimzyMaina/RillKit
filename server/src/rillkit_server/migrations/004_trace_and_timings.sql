-- Attempt timings per stream and group, from the workers' cumulative counters (spec 5.1,
-- data-plane 9.1): attempts, summed wait from publish to handler start, and summed handler time.
-- The API turns them into per-attempt averages.
alter table samples
    add column attempts bigint not null default 0,
    add column wait_ms double precision not null default 0,
    add column work_ms double precision not null default 0;
