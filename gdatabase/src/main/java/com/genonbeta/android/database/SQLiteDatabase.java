package com.genonbeta.android.database;

import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.database.Cursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteStatement;
import android.util.Log;
import com.genonbeta.android.database.exception.ReconstructionFailedException;

import java.io.Serializable;
import java.lang.annotation.Inherited;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by: veli
 * Date: 1/31/17 4:51 PM
 */

abstract public class SQLiteDatabase extends SQLiteOpenHelper
{

    public static final String TAG = SQLiteDatabase.class.getSimpleName(),
            ACTION_DATABASE_CHANGE = "com.genonbeta.database.intent.action.DATABASE_CHANGE",
            EXTRA_BROADCAST_DATA = "extraBroadcastData",
            TYPE_REMOVE = "typeRemove",
            TYPE_INSERT = "typeInsert",
            TYPE_UPDATE = "typeUpdate";

    private final List<BroadcastData> mBroadcastOverhead = new ArrayList<>();
    private Context mContext;

    public SQLiteDatabase(Context context, String name, android.database.sqlite.SQLiteDatabase.CursorFactory factory,
                          int version)
    {
        super(context, name, factory, version);
        mContext = context;
    }

    public void bindContentValue(SQLiteStatement statement, int iteratorPosition, Object bindingObject)
    {
        if (bindingObject == null)
            statement.bindNull(iteratorPosition);
        else if (bindingObject instanceof Long)
            statement.bindLong(iteratorPosition, (Long) bindingObject);
        else if (bindingObject instanceof Integer)
            statement.bindLong(iteratorPosition, (Integer) bindingObject);
        else if (bindingObject instanceof Double)
            statement.bindDouble(iteratorPosition, (Double) bindingObject);
        else if (bindingObject instanceof byte[])
            statement.bindBlob(iteratorPosition, (byte[]) bindingObject);
        else
            statement.bindString(iteratorPosition, bindingObject instanceof String
                    ? (String) bindingObject : String.valueOf(bindingObject));
    }

    public <T extends DatabaseObject> List<T> castQuery(SQLQuery.Select select, final Class<T> clazz)
    {
        return castQuery(select, clazz, null);
    }

    public <T extends DatabaseObject> List<T> castQuery(SQLQuery.Select select, final Class<T> clazz,
                                                        CastQueryListener<T> listener)
    {
        return castQuery(getReadableDatabase(), select, clazz, listener);
    }

    public <T extends DatabaseObject> List<T> castQuery(android.database.sqlite.SQLiteDatabase db,
                                                        SQLQuery.Select select, final Class<T> clazz,
                                                        CastQueryListener<T> listener)
    {
        List<T> returnedList = new ArrayList<>();
        List<ContentValues> itemList = getTable(db, select);

        try {
            for (ContentValues item : itemList) {
                T newClazz = clazz.newInstance();

                newClazz.reconstruct(item);

                if (listener != null)
                    listener.onObjectReconstructed(this, item, newClazz);

                returnedList.add(newClazz);
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }

        return returnedList;
    }

    public synchronized void append(android.database.sqlite.SQLiteDatabase dbInstance, String tableName,
                                    String changeType)
    {
        append(dbInstance, tableName, changeType, getAffectedRowCount(dbInstance));
    }

    public synchronized void append(android.database.sqlite.SQLiteDatabase dbInstance, String tableName,
                                    String changeType, long affectedRows)
    {
        // If no row were affected, we shouldn't add changelog.
        if (affectedRows <= 0) {
            Log.e(TAG, "Changelog is not added because there is no change. table: " + tableName + "; change: "
                    + changeType + "; affected rows: " + affectedRows);
            return;
        }

        BroadcastData data = null;

        synchronized (mBroadcastOverhead) {
            for (BroadcastData testedData : mBroadcastOverhead) {
                if (tableName.equals(testedData.tableName)) {
                    data = testedData;
                    break;
                }
            }

            if (data == null) {
                data = new BroadcastData(tableName);
                mBroadcastOverhead.add(data);
            }
        }

        switch (changeType) {
            case TYPE_INSERT:
                data.inserted = true;
                break;
            case TYPE_REMOVE:
                data.removed = true;
                break;
            case TYPE_UPDATE:
                data.updated = true;
        }

        data.affectedRowCount += affectedRows;
    }

    public synchronized void broadcast()
    {
        synchronized (mBroadcastOverhead) {
            for (BroadcastData data : mBroadcastOverhead)
                getContext().sendBroadcast(new Intent(ACTION_DATABASE_CHANGE).putExtra(EXTRA_BROADCAST_DATA, data));

            mBroadcastOverhead.clear();
        }
    }

    public long getAffectedRowCount(android.database.sqlite.SQLiteDatabase database)
    {
        Cursor cursor = null;
        long returnCount = 0;

        try {
            cursor = database.rawQuery("SELECT changes() AS affected_row_count", null);

            if (cursor != null && cursor.getCount() > 0 && cursor.moveToFirst())
                returnCount = cursor.getLong(cursor.getColumnIndex("affected_row_count"));
        } catch (SQLException ignored) {
        } finally {
            if (cursor != null)
                cursor.close();
        }

        return returnCount;
    }

    public <T, V extends DatabaseObject<T>> Map<String, List<V>> explodePerTable(List<V> objects)
    {
        Map<String, List<V>> tables = new HashMap<>();

        for (V object : objects) {
            String tableName = object.getWhere().tableName;
            List<V> availTable = tables.get(tableName);

            if (availTable == null) {
                availTable = new ArrayList<>();
                tables.put(tableName, availTable);
            }

            availTable.add(object);
        }

        return tables;
    }

    public Context getContext()
    {
        return mContext;
    }

    public ContentValues getFirstFromTable(SQLQuery.Select select)
    {
        return getFirstFromTable(getReadableDatabase(), select);
    }

    public ContentValues getFirstFromTable(android.database.sqlite.SQLiteDatabase db, SQLQuery.Select select)
    {
        List<ContentValues> list = getTable(db, select.setLimit(1));
        return list.size() > 0 ? list.get(0) : null;
    }

    public List<ContentValues> getTable(SQLQuery.Select select)
    {
        return getTable(getReadableDatabase(), select);
    }

    public List<ContentValues> getTable(android.database.sqlite.SQLiteDatabase db, SQLQuery.Select select)
    {
        List<ContentValues> list = new ArrayList<>();
        Cursor cursor = db.query(select.tableName, select.columns, select.where, select.whereArgs, select.groupBy,
                select.having, select.orderBy, select.limit);

        if (cursor.moveToFirst()) {
            if (select.loadListener != null)
                select.loadListener.onOpen(this, cursor);

            do {
                ContentValues item = new ContentValues();

                for (int i = 0; i < cursor.getColumnCount(); i++)
                    item.put(cursor.getColumnName(i), cursor.getString(i));

                if (select.loadListener != null)
                    select.loadListener.onLoad(this, cursor, item);

                list.add(item);
            } while (cursor.moveToNext());
        }

        cursor.close();

        return list;
    }

    public long insert(DatabaseObject object)
    {
        return insert(getWritableDatabase(), object, null);
    }

    public <T> long insert(android.database.sqlite.SQLiteDatabase db, DatabaseObject<T> object, T parent)
    {
        object.onCreateObject(db, this, parent);
        return insert(db, object.getWhere().tableName, null, object.getValues());
    }

    public long insert(android.database.sqlite.SQLiteDatabase db, String tableName, String nullColumnHack,
                       ContentValues contentValues)
    {
        long insertedId = db.insert(tableName, nullColumnHack, contentValues);
        append(db, tableName, TYPE_INSERT, insertedId > -1 ? 1 : 0);
        return insertedId;
    }

    public <T extends DatabaseObject> boolean insert(List<T> objects)
    {
        return insert(objects, null);
    }

    public <T extends DatabaseObject> boolean insert(List<T> objects, ProgressUpdater updater)
    {
        return insert(getWritableDatabase(), objects, updater, null);
    }

    public <T, V extends DatabaseObject<T>> boolean insert(android.database.sqlite.SQLiteDatabase openDatabase,
                                                           List<V> objects, ProgressUpdater updater, T parent)
    {
        int progress = 0;
        openDatabase.beginTransaction();

        try {
            for (V object : objects) {
                if (updater != null)
                    if (!updater.onProgressChange(objects.size(), progress++))
                        break;

                insert(openDatabase, object, parent);
            }

            openDatabase.setTransactionSuccessful();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            openDatabase.endTransaction();
        }

        return false;
    }

    public int publish(DatabaseObject object)
    {
        return publish(getWritableDatabase(), object, null);
    }

    public <T> int publish(android.database.sqlite.SQLiteDatabase database, DatabaseObject<T> object, T parent)
    {
        int rowsChanged = update(database, object, parent);

        if (rowsChanged <= 0)
            rowsChanged = insert(database, object, parent) >= -1 ? 1 : 0;

        return rowsChanged;
    }

    public <T extends DatabaseObject> boolean publish(List<T> objects)
    {
        return publish(objects, null);
    }

    public <T extends DatabaseObject> boolean publish(List<T> objects, ProgressUpdater updater)
    {
        return publish(getWritableDatabase(), objects, updater, null);
    }

    public <T, V extends DatabaseObject<T>> boolean publish(android.database.sqlite.SQLiteDatabase openDatabase,
                                                            List<V> objects, ProgressUpdater updater, T parent)
    {
        int progress = 0;
        openDatabase.beginTransaction();

        try {
            for (V object : objects) {
                if (updater != null)
                    if (!updater.onProgressChange(objects.size(), progress++))
                        break;

                publish(openDatabase, object, parent);
            }

            openDatabase.setTransactionSuccessful();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            openDatabase.endTransaction();
        }

        return false;
    }

    public void reconstruct(DatabaseObject object) throws ReconstructionFailedException
    {
        reconstruct(getReadableDatabase(), object);
    }

    public void reconstruct(android.database.sqlite.SQLiteDatabase db, DatabaseObject object)
            throws ReconstructionFailedException
    {
        ContentValues item = getFirstFromTable(db, object.getWhere());

        if (item == null) {
            SQLQuery.Select select = object.getWhere();
            StringBuilder whereArgs = new StringBuilder();

            for (String arg : select.whereArgs) {
                if (whereArgs.length() > 0)
                    whereArgs.append(", ");

                whereArgs.append("[] ");
                whereArgs.append(arg);
            }

            throw new ReconstructionFailedException("No data was returned from: query" + "; tableName: "
                    + select.tableName + "; where: " + select.where + "; whereArgs: " + whereArgs.toString());
        }

        object.reconstruct(item);
    }

    public void remove(DatabaseObject object)
    {
        remove(getWritableDatabase(), object, null);
    }

    public <T> void remove(android.database.sqlite.SQLiteDatabase db, DatabaseObject<T> object, T parent)
    {
        object.onRemoveObject(db, this, parent);
        remove(db, object.getWhere());
    }

    public int remove(SQLQuery.Select select)
    {
        return remove(getWritableDatabase(), select);
    }

    public int remove(android.database.sqlite.SQLiteDatabase db, SQLQuery.Select select)
    {
        int affectedRows = db.delete(select.tableName, select.where, select.whereArgs);
        append(db, select.tableName, TYPE_REMOVE, affectedRows);
        return affectedRows;
    }

    public <T extends DatabaseObject> boolean remove(List<T> objects)
    {
        return remove(objects, null);
    }

    public <T extends DatabaseObject> boolean remove(List<T> objects, ProgressUpdater updater)
    {
        return remove(getWritableDatabase(), objects, updater, null);
    }

    public <T, V extends DatabaseObject<T>> boolean remove(android.database.sqlite.SQLiteDatabase openDatabase,
                                                        List<V> objects, ProgressUpdater updater, T parent)
    {
        int progress = 0;
        openDatabase.beginTransaction();

        try {
            for (V object : objects) {
                if (updater != null)
                    if (!updater.onProgressChange(objects.size(), progress++))
                        break;

                remove(openDatabase, object, parent);
            }

            openDatabase.setTransactionSuccessful();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            openDatabase.endTransaction();
        }

        return false;
    }

    public <V, T extends DatabaseObject<V>> boolean removeAsObject(android.database.sqlite.SQLiteDatabase openDatabase,
                                                                SQLQuery.Select select, Class<T> objectType,
                                                                CastQueryListener<T> listener, ProgressUpdater updater,
                                                                V parent)
    {
        int progress = 0;
        openDatabase.beginTransaction();

        try {
            List<T> objects = castQuery(openDatabase, select, objectType, listener);

            for (T object : objects) {
                if (updater != null)
                    if (!updater.onProgressChange(objects.size(), progress++))
                        break;

                remove(openDatabase, object, parent);
            }

            openDatabase.setTransactionSuccessful();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            openDatabase.endTransaction();
        }

        return false;
    }

    public int update(DatabaseObject object)
    {
        return update(getWritableDatabase(), object, null);
    }

    public <T> int update(android.database.sqlite.SQLiteDatabase db, DatabaseObject<T> object, T parent)
    {
        object.onUpdateObject(db, this, parent);
        return update(db, object.getWhere(), object.getValues());
    }

    public int update(SQLQuery.Select select, ContentValues values)
    {
        return update(getWritableDatabase(), select, values);
    }

    public int update(android.database.sqlite.SQLiteDatabase database, SQLQuery.Select select, ContentValues values)
    {
        int rowsAffected = database.update(select.tableName, values, select.where, select.whereArgs);
        append(database, select.tableName, TYPE_UPDATE, rowsAffected);
        return rowsAffected;
    }

    public <T extends DatabaseObject> boolean update(List<T> objects)
    {
        return update(objects, null);
    }

    public <T extends DatabaseObject> boolean update(List<T> objects, ProgressUpdater updater)
    {
        return update(getWritableDatabase(), objects, updater, null);
    }

    public <T, V extends DatabaseObject<T>> boolean update(android.database.sqlite.SQLiteDatabase openDatabase,
                                                        List<V> objects, ProgressUpdater updater, T parent)
    {
        int progress = 0;
        openDatabase.beginTransaction();

        try {
            for (V object : objects) {
                if (updater != null)
                    if (!updater.onProgressChange(objects.size(), progress++))
                        break;

                update(openDatabase, object, parent);
            }

            openDatabase.setTransactionSuccessful();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            openDatabase.endTransaction();
        }

        return false;
    }

    public interface CastQueryListener<T extends DatabaseObject>
    {
        void onObjectReconstructed(SQLiteDatabase db, ContentValues item, T object);
    }

    public interface ProgressUpdater
    {
        boolean onProgressChange(int total, int current);
    }

    public static BroadcastData toData(Intent intent)
    {
        return (BroadcastData) intent.getSerializableExtra(EXTRA_BROADCAST_DATA);
    }

    public static class BroadcastData implements Serializable
    {
        public int affectedRowCount = 0;
        public boolean inserted = false;
        public boolean removed = false;
        public boolean updated = false;
        public String tableName;

        BroadcastData(String tableName)
        {
            this.tableName = tableName;
        }
    }
}