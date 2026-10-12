CREATE TRIGGER sync_note_row AFTER INSERT OR UPDATE OR DELETE ON "shop"."products" FOR EACH ROW EXECUTE FUNCTION sync.note_row('products', '', '1', 'id', '0');
CREATE TRIGGER sync_refuse_truncate BEFORE TRUNCATE ON "shop"."products" FOR EACH STATEMENT EXECUTE FUNCTION sync.refuse_truncate();
