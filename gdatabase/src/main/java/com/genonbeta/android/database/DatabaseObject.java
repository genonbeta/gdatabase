package com.genonbeta.android.database;

import android.database.sqlite.SQLiteDatabase;

/**
 * created by: Veli
 * date: 2.11.2017 21:31
 */

public interface DatabaseObject<T> extends BaseDatabaseObject
{
	void onCreateObject(SQLiteDatabase db, KuickDb kuick, T parent);

	void onUpdateObject(SQLiteDatabase db, KuickDb kuick, T parent);

	void onRemoveObject(SQLiteDatabase db, KuickDb kuick, T parent);
}
