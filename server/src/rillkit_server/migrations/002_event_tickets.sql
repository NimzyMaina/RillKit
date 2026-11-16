-- Short-lived, single-use tickets for the live events stream (EventSource cannot send an
-- Authorization header, and the admin token must never appear in a url or an access log).
create table event_tickets (
    hash        text primary key,          -- sha256 of the ticket; the ticket itself is not stored
    tenant_id   text not null references tenants(id) on delete cascade,
    expires_at  timestamptz not null
);
