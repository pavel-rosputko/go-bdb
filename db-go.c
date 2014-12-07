#include <db.h>

int db_env_open(DB_ENV *env, char *db_home, u_int32_t flags, int mode) {
	return env->open(env, db_home, flags, mode);
}

int db_env_close(DB_ENV *env, u_int32_t flags) {
	return env->close(env, flags);
}

int db_env_txn_begin(DB_ENV *env, DB_TXN *parent, DB_TXN **tid, u_int32_t flags) {
	return env->txn_begin(env, parent, tid, flags);
}

int db_open(DB *db, DB_TXN *txnid, char *file, char *database, DBTYPE type, u_int32_t flags, int mode) {
	return db->open(db, txnid, file, database, type, flags, mode);
}

int db_close(DB *db, u_int32_t flags) {
	return db->close(db, flags);
}

int db_set_flags(DB *db, u_int32_t flags) {
	return db->set_flags(db, flags);
}

int db_put(DB *db, DB_TXN *txnid, DBT *key, DBT *value, u_int32_t flags) {
	return db->put(db, txnid, key, value, flags);
}

int db_get(DB *db, DB_TXN *txnid, DBT *key, DBT *value, u_int32_t flags) {
	return db->get(db, txnid, key, value, flags);
}

int db_exists(DB *db, DB_TXN *txnid, DBT *key, u_int32_t flags) {
	return db->exists(db, txnid, key, flags);
}

int db_get_pagesize(DB *db, u_int32_t *pagesizep) {
	return db->get_pagesize(db, pagesizep);
}

int txn_commit(DB_TXN *tid, u_int32_t flags) {
	return tid->commit(tid, flags);
}

int txn_abort(DB_TXN *tid) {
	return tid->abort(tid);
}
