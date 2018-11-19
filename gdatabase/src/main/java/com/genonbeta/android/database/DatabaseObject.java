package com.genonbeta.android.database;

import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;

import com.genonbeta.android.database.CursorItem;
import com.genonbeta.android.database.SQLQuery;

/**
 * created by: Veli
 * date: 2.11.2017 21:31
 */

public interface DatabaseObject<T extends Object>
{
	SQLQuery.Select getWhere();

	ContentValues getValues();

	void reconstruct(CursorItem item);

	void onCreateObject(android.database.sqlite.SQLiteDatabase db, SQLiteDatabase database, T parent);

	void onUpdateObject(android.database.sqlite.SQLiteDatabase db, SQLiteDatabase database, T parent);

	void onRemoveObject(android.database.sqlite.SQLiteDatabase db, SQLiteDatabase database, T parent);
}
