package com.genonbeta.android.database;

import android.content.ContentValues;

/**
 * created by: Veli
 * date: 2.11.2017 21:31
 */

public interface DatabaseObject<T>
{
	SQLQuery.Select getWhere();

	ContentValues getValues();

	void reconstruct(ContentValues item);

	void onCreateObject(android.database.sqlite.SQLiteDatabase db, SQLiteDatabase database, T parent);

	void onUpdateObject(android.database.sqlite.SQLiteDatabase db, SQLiteDatabase database, T parent);

	void onRemoveObject(android.database.sqlite.SQLiteDatabase db, SQLiteDatabase database, T parent);
}
