package com.genonbeta.android.database;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteStatement;

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
    private Context mContext;
    private int mMaxHeapSize = 250;

    public SQLiteDatabase(Context context, String name, android.database.sqlite.SQLiteDatabase.CursorFactory factory, int version)
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
                    ? (String) bindingObject
                    : String.valueOf(bindingObject));
    }

    public <T extends DatabaseObject> ArrayList<T> castQuery(SQLQuery.Select select, final Class<T> clazz)
    {
        return castQuery(select, clazz, null);
    }

    public <T extends DatabaseObject> ArrayList<T> castQuery(SQLQuery.Select select, final Class<T> clazz, CastQueryListener<T> listener)
    {
        return castQuery(getReadableDatabase(), select, clazz, listener);
    }

    public <T extends DatabaseObject> ArrayList<T> castQuery(android.database.sqlite.SQLiteDatabase db, SQLQuery.Select select, final Class<T> clazz, CastQueryListener<T> listener)
    {
        ArrayList<T> returnedList = new ArrayList<>();
        ArrayList<CursorItem> itemList = getTable(db, select);

        try {
            for (CursorItem item : itemList) {
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

    public Map<String, List<DatabaseObject>> explodePerTable(List<? extends DatabaseObject> objects)
    {
        Map<String, List<DatabaseObject>> tables = new HashMap<>();

        for (DatabaseObject object : objects) {
            String tableName = object.getWhere().tableName;
            List<DatabaseObject> availTable = tables.get(tableName);

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

    public CursorItem getFirstFromTable(SQLQuery.Select select)
    {
        return getFirstFromTable(getReadableDatabase(), select);
    }

    public CursorItem getFirstFromTable(android.database.sqlite.SQLiteDatabase db, SQLQuery.Select select)
    {
        ArrayList<CursorItem> list = getTable(db, select.setLimit(1));
        return list.size() > 0 ? list.get(0) : null;
    }

    public ArrayList<CursorItem> getTable(SQLQuery.Select select)
    {
        return getTable(getReadableDatabase(), select);
    }

    public ArrayList<CursorItem> getTable(android.database.sqlite.SQLiteDatabase db, SQLQuery.Select select)
    {
        ArrayList<CursorItem> list = new ArrayList<>();

        Cursor cursor = db.query(select.tableName,
                select.columns,
                select.where,
                select.whereArgs,
                select.groupBy,
                select.having,
                select.orderBy,
                select.limit);

        if (cursor.moveToFirst()) {
            if (select.loadListener != null)
                select.loadListener.onOpen(this, cursor);

            do {
                CursorItem item = new CursorItem();

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
        return insert(getWritableDatabase(), object);
    }

    public long insert(android.database.sqlite.SQLiteDatabase db, DatabaseObject object)
    {
        object.onCreateObject(this);
        return insert(db, object.getWhere().tableName, null, object.getValues());
    }

    public long insert(android.database.sqlite.SQLiteDatabase db, String tableName, String nullColumnHack, ContentValues contentValues)
    {
        return db.insert(tableName, nullColumnHack, contentValues);
    }

    public void insert(List<? extends DatabaseObject> objects)
    {
        insert(objects, null);
    }

    public void insert(List<? extends DatabaseObject> objects, ProgressUpdater updater)
    {
        insert(getWritableDatabase(), objects, updater);
    }

    public void insert(android.database.sqlite.SQLiteDatabase openDatabase, List<? extends DatabaseObject> objects, ProgressUpdater updater)
    {
        Map<String, List<DatabaseObject>> tables = explodePerTable(objects);

        openDatabase.beginTransaction();

        try {
            if (tables.size() > 0) {
                for (String tableName : tables.keySet()) {
                    List<String> baseKeys = new ArrayList<>();
                    List<DatabaseObject> databaseObjects = tables.get(tableName);
                    StringBuilder valueTemplate = new StringBuilder();
                    int indexPosition = 0;

                    if (databaseObjects != null) {
                        for (DatabaseObject thisObject : databaseObjects) {
                            ContentValues contentValues = thisObject.getValues();

                            {
                                // these are not related to individual processes
                                if (baseKeys.size() == 0)
                                    baseKeys.addAll(contentValues.keySet());

                                if (valueTemplate.length() == 0) {
                                    valueTemplate.append("(");

                                    for (int columnIterator = 0; columnIterator < baseKeys.size(); columnIterator++) {
                                        if (columnIterator > 0)
                                            valueTemplate.append(",");

                                        valueTemplate.append("?");
                                    }

                                    valueTemplate.append(")");
                                }
                            }

                            StringBuilder sqlQuery = new StringBuilder();
                            StringBuilder columnIndex = new StringBuilder();

                            sqlQuery.append(String.format("INSERT INTO `%s` (", tableName));

                            for (String columnName : baseKeys) {
                                if (columnIndex.length() > 0)
                                    columnIndex.append(",");

                                columnIndex.append(String.format("`%s`", columnName));
                            }

                            sqlQuery.append(columnIndex);
                            sqlQuery.append(") VALUES ");
                            sqlQuery.append(valueTemplate);
                            sqlQuery.append(";");

                            if (updater == null || updater.onProgressState()) {
                                SQLiteStatement statement = openDatabase.compileStatement(sqlQuery.toString());

                                int iterator = 0;
                                for (String baseKey : baseKeys) {
                                    bindContentValue(statement, ++iterator, contentValues.get(baseKey));
                                }

                                statement.execute();
                                statement.close();
                            } else
                                break;

                            if (updater != null)
                                updater.onProgressChange(objects.size(), indexPosition++);

                            thisObject.onCreateObject(this);
                        }
                    }
                }
            }

            openDatabase.setTransactionSuccessful();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            openDatabase.endTransaction();
        }
    }

    public void publish(DatabaseObject object) {
        publish(getWritableDatabase(), object);
    }

    public void publish(android.database.sqlite.SQLiteDatabase database, DatabaseObject object)
    {
        if (getFirstFromTable(object.getWhere()) != null)
            update(database, object);
        else
            insert(database, object);
    }

    public <T extends DatabaseObject> boolean publish(Class<T> clazz, List<T> objects)
    {
        return publish(clazz, objects, null);
    }

    public <T extends DatabaseObject> boolean publish(Class<T> clazz, List<T> objects, ProgressUpdater updater)
    {
        return publish(getWritableDatabase(), clazz, objects, updater);
    }

    public <T extends DatabaseObject> boolean publish(android.database.sqlite.SQLiteDatabase openDatabase, Class<T> clazz, List<T> objects, ProgressUpdater updater)
    {
        Map<String, List<DatabaseObject>> tables = explodePerTable(objects);

        if (tables.size() > 0) {
            try {
                T exampleClazz = clazz.newInstance();

                for (String tableName : tables.keySet()) {
                    List<DatabaseObject> objectList = tables.get(tableName);

                    if (objectList != null) {
                        List<DatabaseObject> updatingObjects = new ArrayList<>();
                        List<DatabaseObject> insertingObjects = new ArrayList<>();
                        int existenceIterator = 0;

                        for (DatabaseObject currentObject : objectList) {
                            if (updater != null && !updater.onProgressState())
                                return false;

                            if (getFirstFromTable(currentObject.getWhere()) == null)
                                insertingObjects.add(currentObject);
                            else
                                updatingObjects.add(currentObject);

                            if (updater != null)
                                updater.onProgressChange(objectList.size(), existenceIterator++);
                        }

                        insert(openDatabase, insertingObjects, updater);
                        update(openDatabase, updatingObjects, updater);
                    }
                }

                return true;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return false;
    }

    public void reconstruct(DatabaseObject object) throws Exception
    {
        reconstruct(getReadableDatabase(), object);
    }

    public void reconstruct(android.database.sqlite.SQLiteDatabase db, DatabaseObject object) throws Exception
    {
        CursorItem item = getFirstFromTable(db, object.getWhere());

        if (item == null) {
            SQLQuery.Select select = object.getWhere();

            StringBuilder whereArgs = new StringBuilder();

            for (String arg : select.whereArgs) {
                if (whereArgs.length() > 0)
                    whereArgs.append(", ");

                whereArgs.append("[] ");
                whereArgs.append(arg);
            }

            throw new Exception("No data was returned from: query"
                    + "; tableName: " + select.tableName
                    + "; where: " + select.where
                    + "; whereArgs: " + whereArgs.toString());
        }

        object.reconstruct(item);
    }

    public void remove(DatabaseObject object)
    {
        object.onRemoveObject(this);
        remove(getWritableDatabase(), object.getWhere());
    }

    public void remove(android.database.sqlite.SQLiteDatabase db, DatabaseObject object)
    {
        object.onRemoveObject(this);
        remove(db, object.getWhere());
    }

    public int remove(SQLQuery.Select select)
    {
        return remove(getWritableDatabase(), select);
    }

    public int remove(android.database.sqlite.SQLiteDatabase db, SQLQuery.Select select)
    {
        return db.delete(select.tableName, select.where, select.whereArgs);
    }

    public void remove(List<? extends DatabaseObject> objects)
    {
        remove(objects, null);
    }

    public void remove(List<? extends DatabaseObject> objects, ProgressUpdater updater)
    {
        remove(getWritableDatabase(), objects, updater);
    }

    public void remove(android.database.sqlite.SQLiteDatabase openDatabase, List<? extends DatabaseObject> objects, ProgressUpdater updater)
    {
        int progress = 0;

        openDatabase.beginTransaction();

        try {
            for (DatabaseObject object : objects) {
                if (updater != null && !updater.onProgressState())
                    break;

                SQLQuery.Select select = object.getWhere();
                openDatabase.delete(select.tableName, select.where, select.whereArgs);

                if (updater != null)
                    updater.onProgressChange(objects.size(), progress++);
            }

            openDatabase.setTransactionSuccessful();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            openDatabase.endTransaction();
        }
    }

    public int update(DatabaseObject object)
    {
        return update(getWritableDatabase(), object);
    }

    public int update(android.database.sqlite.SQLiteDatabase db, DatabaseObject object)
    {
        object.onUpdateObject(this);
        return update(db, object.getWhere(), object.getValues());
    }

    public int update(SQLQuery.Select select, ContentValues values)
    {
        return update(getWritableDatabase(), select, values);
    }

    public int update(android.database.sqlite.SQLiteDatabase database, SQLQuery.Select select, ContentValues values)
    {
        return database.update(select.tableName, values, select.where, select.whereArgs);
    }

    public void update(List<? extends DatabaseObject> objects)
    {
        update(objects, null);
    }

    public void update(List<? extends DatabaseObject> objects, ProgressUpdater updater)
    {
        update(getWritableDatabase(), objects, updater);
    }

    public void update(android.database.sqlite.SQLiteDatabase openDatabase, List<? extends DatabaseObject> objects, ProgressUpdater updater)
    {
        int progress = 0;

        openDatabase.beginTransaction();

        try {
            for (DatabaseObject object : objects) {
                if (updater != null && !updater.onProgressState())
                    break;

                SQLQuery.Select select = object.getWhere();
                openDatabase.update(select.tableName, object.getValues(), select.where, select.whereArgs);

                if (updater != null)
                    updater.onProgressChange(objects.size(), progress++);
            }

            openDatabase.setTransactionSuccessful();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            openDatabase.endTransaction();
        }
    }

    public interface CastQueryListener<T extends DatabaseObject>
    {
        void onObjectReconstructed(SQLiteDatabase db, CursorItem item, T object);
    }

    public interface ProgressUpdater
    {
        void onProgressChange(int total, int current);

        boolean onProgressState(); // true to continue
    }
}