package com.genonbeta.android.database;

import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.database.Cursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteStatement;
import android.util.Log;
import com.genonbeta.android.database.exception.ReconstructionFailedException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by: veli
 * Date: 1/31/17 4:51 PM
 */

public abstract class KuickDb extends SQLiteOpenHelper
{
    public static final String TAG = KuickDb.class.getSimpleName(),
            ACTION_DATABASE_CHANGE = "com.genonbeta.database.intent.action.DATABASE_CHANGE",
            EXTRA_BROADCAST_DATA = "extraBroadcastData",
            TYPE_REMOVE = "typeRemove",
            TYPE_INSERT = "typeInsert",
            TYPE_UPDATE = "typeUpdate";

    private final List<BroadcastData> mBroadcastOverhead = new ArrayList<>();
    private final Context mContext;

    public KuickDb(Context context, String name, SQLiteDatabase.CursorFactory factory, int version)
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

    public <T, V extends DatabaseObject<T>> List<V> castQuery(SQLQuery.Select select, final Class<V> clazz)
    {
        return castQuery(select, clazz, null);
    }

    public <T, V extends DatabaseObject<T>> List<V> castQuery(SQLQuery.Select select, final Class<V> clazz,
                                                              CastQueryListener<V> listener)
    {
        return castQuery(getReadableDatabase(), select, clazz, listener);
    }

    public <T, V extends DatabaseObject<T>> List<V> castQuery(SQLiteDatabase db, SQLQuery.Select select,
                                                              final Class<V> clazz, CastQueryListener<V> listener)
    {
        List<V> returnedList = new ArrayList<>();
        List<ContentValues> itemList = getTable(db, select);

        try {
            for (ContentValues item : itemList) {
                V newClazz = clazz.newInstance();
                newClazz.reconstruct(db, this, item);

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

    public synchronized void append(SQLiteDatabase db, String tableName, String changeType)
    {
        append(db, tableName, changeType, getAffectedRowCount(db));
    }

    public synchronized void append(SQLiteDatabase db, String tableName, String changeType, long affectedRows)
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

    public long getAffectedRowCount(SQLiteDatabase database)
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

    public Context getContext()
    {
        return mContext;
    }

    public ContentValues getFirstFromTable(SQLQuery.Select select)
    {
        return getFirstFromTable(getReadableDatabase(), select);
    }

    public ContentValues getFirstFromTable(SQLiteDatabase db, SQLQuery.Select select)
    {
        List<ContentValues> list = getTable(db, select.setLimit(1));
        return list.size() > 0 ? list.get(0) : null;
    }

    public List<ContentValues> getTable(SQLQuery.Select select)
    {
        return getTable(getReadableDatabase(), select);
    }

    public List<ContentValues> getTable(SQLiteDatabase db, SQLQuery.Select select)
    {
        List<ContentValues> list = new ArrayList<>();
        Cursor cursor = db.query(select.tableName, select.columns, select.where, select.whereArgs, select.groupBy,
                select.having, select.orderBy, select.limit);

        if (cursor.moveToFirst()) {
            if (select.loadListener != null)
                select.loadListener.onOpen(this, cursor);

            int columnCount = cursor.getColumnCount();
            String[] columns = new String[columnCount];
            int[] types = new int[columnCount];

            for (int i = 0; i < columnCount; i++) {
                columns[i] = cursor.getColumnName(i);
                types[i] = cursor.getType(i);
            }

            do {
                ContentValues item = new ContentValues();

                for (int i = 0; i < columnCount; i++) {
                    String columnName = columns[i];
                    switch (types[i]) {
                        case Cursor.FIELD_TYPE_INTEGER:
                            item.put(columnName, cursor.getLong(i));
                            break;
                        case Cursor.FIELD_TYPE_STRING:
                        case Cursor.FIELD_TYPE_NULL:
                            item.put(columnName, cursor.getString(i));
                            break;
                        case Cursor.FIELD_TYPE_FLOAT:
                            item.put(columnName, cursor.getFloat(i));
                            break;
                        case Cursor.FIELD_TYPE_BLOB:
                            item.put(columnName, cursor.getBlob(i));
                            break;
                    }
                }

                if (select.loadListener != null)
                    select.loadListener.onLoad(this, cursor, item);

                list.add(item);
            } while (cursor.moveToNext());
        }

        cursor.close();

        return list;
    }

    public <T, V extends DatabaseObject<T>> long insert(V object)
    {
        return insert(getWritableDatabase(), object, null, null);
    }

    public <T, V extends DatabaseObject<T>> long insert(SQLiteDatabase db, V object, T parent,
                                                        Progress.Listener listener)
    {
        object.onCreateObject(db, this, parent, listener);
        return insert(db, object.getWhere().tableName, null, object.getValues());
    }

    public long insert(SQLiteDatabase db, String tableName, String nullColumnHack, ContentValues contentValues)
    {
        long insertedId = db.insert(tableName, nullColumnHack, contentValues);
        append(db, tableName, TYPE_INSERT, insertedId > -1 ? 1 : 0);
        return insertedId;
    }

    public <T, V extends DatabaseObject<T>> boolean insert(List<V> objects)
    {
        return insert(getWritableDatabase(), objects, null, null);
    }

    public <T, V extends DatabaseObject<T>> boolean insert(SQLiteDatabase db, List<V> objects, T parent,
                                                           Progress.Listener listener)
    {
        db.beginTransaction();

        try {
            Progress.addToTotal(listener, objects.size());

            for (V object : objects) {
                if (!Progress.call(listener, 1))
                    break;

                insert(db, object, parent, listener);
            }

            db.setTransactionSuccessful();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.endTransaction();
        }

        return false;
    }

    public <T, V extends DatabaseObject<T>> int publish(V object)
    {
        return publish(getWritableDatabase(), object, null, null);
    }

    public <T, V extends DatabaseObject<T>> int publish(SQLiteDatabase database, V object, T parent,
                                                        Progress.Listener listener)
    {
        int rowsChanged = update(database, object, parent, listener);

        if (rowsChanged <= 0)
            rowsChanged = insert(database, object, parent, listener) >= -1 ? 1 : 0;

        return rowsChanged;
    }

    public <T, V extends DatabaseObject<T>> boolean publish(List<V> objects)
    {
        return publish(getWritableDatabase(), objects, null, null);
    }

    public <T, V extends DatabaseObject<T>> boolean publish(SQLiteDatabase db, List<V> objectList, T parent,
                                                            Progress.Listener listener)
    {
        db.beginTransaction();

        try {
            Progress.addToTotal(listener, objectList.size());

            for (V object : objectList) {
                if (!Progress.call(listener, 1))
                    break;

                publish(db, object, parent, listener);
            }

            db.setTransactionSuccessful();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.endTransaction();
        }

        return false;
    }

    public <T, V extends DatabaseObject<T>> void reconstruct(V object) throws ReconstructionFailedException
    {
        reconstruct(getReadableDatabase(), object);
    }

    public <T, V extends DatabaseObject<T>> void reconstruct(SQLiteDatabase db, V object)
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

        object.reconstruct(db, this, item);
    }

    public <T, V extends DatabaseObject<T>> void remove(V object)
    {
        remove(getWritableDatabase(), object, null, null);
    }

    public <T, V extends DatabaseObject<T>> void remove(SQLiteDatabase db, V object, T parent,
                                                        Progress.Listener listener)
    {
        object.onRemoveObject(db, this, parent, listener);
        remove(db, object.getWhere());
    }

    public int remove(SQLQuery.Select select)
    {
        return remove(getWritableDatabase(), select);
    }

    public int remove(SQLiteDatabase db, SQLQuery.Select select)
    {
        int affectedRows = db.delete(select.tableName, select.where, select.whereArgs);
        append(db, select.tableName, TYPE_REMOVE, affectedRows);
        return affectedRows;
    }

    public <T, V extends DatabaseObject<T>> boolean remove(List<V> objects)
    {
        return remove(getWritableDatabase(), objects, null, null);
    }

    public <T, V extends DatabaseObject<T>> boolean remove(SQLiteDatabase db, List<V> objects, T parent,
                                                           Progress.Listener listener)
    {
        db.beginTransaction();

        try {
            Progress.addToTotal(listener, objects.size());

            for (V object : objects) {
                if (!Progress.call(listener, 1))
                    break;

                remove(db, object, parent, listener);
            }

            db.setTransactionSuccessful();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.endTransaction();
        }

        return false;
    }

    public <T, V extends DatabaseObject<T>> boolean removeAsObject(SQLiteDatabase db, SQLQuery.Select select,
                                                                   Class<V> objectType, T parent,
                                                                   Progress.Listener progressListener,
                                                                   CastQueryListener<V> queryListener)
    {
        db.beginTransaction();

        try {
            List<V> objects = castQuery(db, select, objectType, queryListener);
            Progress.addToTotal(progressListener, objects.size());

            for (V object : objects) {
                if (!Progress.call(progressListener, 1))
                    break;

                remove(db, object, parent, progressListener);
            }

            db.setTransactionSuccessful();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.endTransaction();
        }

        return false;
    }

    public <T, V extends DatabaseObject<T>> int update(V object)
    {
        return update(getWritableDatabase(), object, null, null);
    }

    public <T, V extends DatabaseObject<T>> int update(SQLiteDatabase db, V object, T parent,
                                                       Progress.Listener listener)
    {
        object.onUpdateObject(db, this, parent, listener);
        return update(db, object.getWhere(), object.getValues());
    }

    public int update(SQLQuery.Select select, ContentValues values)
    {
        return update(getWritableDatabase(), select, values);
    }

    public int update(SQLiteDatabase database, SQLQuery.Select select, ContentValues values)
    {
        int rowsAffected = database.update(select.tableName, values, select.where, select.whereArgs);
        append(database, select.tableName, TYPE_UPDATE, rowsAffected);
        return rowsAffected;
    }

    public <T, V extends DatabaseObject<T>> boolean update(List<V> objects)
    {
        return update(getWritableDatabase(), objects, null, null);
    }

    public <T, V extends DatabaseObject<T>> boolean update(SQLiteDatabase db, List<V> objects, T parent,
                                                           Progress.Listener listener)
    {
        db.beginTransaction();

        try {
            Progress.addToTotal(listener, objects.size());

            for (V object : objects) {
                if (!Progress.call(listener, 1))
                    break;

                update(db, object, parent, listener);
            }

            db.setTransactionSuccessful();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.endTransaction();
        }

        return false;
    }

    public interface CastQueryListener<T extends DatabaseObject<?>>
    {
        void onObjectReconstructed(KuickDb manager, ContentValues item, T object);
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