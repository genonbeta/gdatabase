package com.genonbeta.android.database;

import android.content.ContentValues;

public interface BaseDatabaseObject
{
    ContentValues getValues();

    SQLQuery.Select getWhere();

    void reconstruct(ContentValues item);
}
