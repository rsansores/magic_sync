CREATE TRIGGER sync_note_row AFTER INSERT OR UPDATE OR DELETE ON "shop"."products" FOR EACH ROW EXECUTE FUNCTION sync.note_row('products', '', '1', 'id', '0');
CREATE TRIGGER sync_refuse_truncate BEFORE TRUNCATE ON "shop"."products" FOR EACH STATEMENT EXECUTE FUNCTION sync.refuse_truncate();
CREATE TRIGGER sync_note_row AFTER INSERT OR UPDATE OR DELETE ON "sync"."sync_wishes" FOR EACH ROW EXECUTE FUNCTION sync.note_row('sync_wishes', '', '1', 'id', '0', 'answered', 'outcome', 'reason', 'answered_at');
CREATE TRIGGER sync_refuse_truncate BEFORE TRUNCATE ON "sync"."sync_wishes" FOR EACH STATEMENT EXECUTE FUNCTION sync.refuse_truncate();
