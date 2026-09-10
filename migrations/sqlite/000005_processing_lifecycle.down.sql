-- Restore a verified backup; ownership cannot be removed safely in place.
SELECT lifecycle_destructive_rollback_is_not_supported();
