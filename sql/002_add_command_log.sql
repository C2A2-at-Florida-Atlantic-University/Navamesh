-- Additive migration for the downlink command bus. Run manually against navamesh
-- before deploying a Pi that has command support. Idempotent, like 001.
--
-- This table IS the audit trail. Commands change deployed hardware in the field, so
-- every request is recorded with who asked, what they asked for, and what the node
-- actually did about it. Rows are never purged.
BEGIN;

CREATE TABLE IF NOT EXISTS public.command_log (
    cmd_id TEXT PRIMARY KEY,
    verb TEXT NOT NULL,
    -- "^all" for a broadcast, otherwise a Meshtastic "!hexid".
    target TEXT NOT NULL,
    params JSONB,
    -- The RNS/LXMF identity hash that issued the command.
    requested_by TEXT NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- pending -> sent -> acked | nak | timeout | error
    state TEXT NOT NULL DEFAULT 'pending',
    detail JSONB,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Cleared once the operator has been told the outcome over LXMF, so the poller
    -- does not notify the same command twice.
    notified BOOLEAN NOT NULL DEFAULT false
);

-- The ack poller scans for unnotified terminal rows and for stale pending ones, so
-- both predicates want an index.
CREATE INDEX IF NOT EXISTS idx_command_log_pending
    ON public.command_log (notified, state, requested_at);
CREATE INDEX IF NOT EXISTS idx_command_log_target
    ON public.command_log (target, requested_at DESC);

COMMIT;
