package com.genonbeta.android.database;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteStatement;

import com.genonbeta.android.database.exception.ReconstructionFailedException;

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
        return insert(getWritableDatabase(), object, null);
    }

    public <T> long insert(android.database.sqlite.SQLiteDatabase db, DatabaseObject<T> object, T parent)
    {
        object.onCreateObject(db, this, parent);
        return insert(db, object.getWhere().tableName, null, object.getValues());
    }

    public long insert(android.database.sqlite.SQLiteDatabase db, String tableName, String nullColumnHack, ContentValues contentValues)
    {
        return db.insert(tableName, nullColumnHack, contentValues);
    }

    public <T extends DatabaseObject> void insert(List<T> objects)
    {
        insert(objects, null);
    }

    public <T extends DatabaseObject> void insert(List<T> objects, ProgressUpdater updater)
    {
        insert(getWritableDatabase(), objects, updater, null);
    }

    public <T, V extends DatabaseObject<T>> void insert(android.database.sqlite.SQLiteDatabase openDatabase, List<V> objects, ProgressUpdater updater, T parent)
    {
        Map<String, List<V>> tables = explodePerTable(objects);
        boolean successful = false;

        openDatabase.beginTransaction();

        try {
            if (tables.size() > 0) {
                for (String tableName : tables.keySet()) {
                    List<String> baseKeys = new ArrayList<>();
                    List<V> databaseObjects = tables.get(tableName);
                    StringBuilder valueTemplate = new StringBuilder();
                    int indexPosition = 0;

                    if (databaseObjects != null) {
                        for (V thisObject : databaseObjects) {
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
                        }
                    }
                }
            }

            openDatabase.setTransactionSuccessful();
            successful = true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            openDatabase.endTransaction();

            if (successful)
                for (V object : objects)
                    object.onCreateObject(openDatabase, this, parent);
        }
    }

    public void publish(DatabaseObject object)
    {
        publish(getWritableDatabase(), object, null);
    }

    public <T> void publish(android.database.sqlite.SQLiteDatabase database, DatabaseObject<T> object, T parent)
    {
        if (getFirstFromTable(database, object.getWhere()) != null)
            update(database, object, parent);
        else
            insert(database, object, parent);
    }

    public <T extends DatabaseObject> boolean publish(List<T> objects)
    {
        return publish(objects, null);
    }

    public <T extends DatabaseObject> boolean publish(List<T> objects, ProgressUpdater updater)
    {
        return publish(getWritableDatabase(), objects, updater, null);
    }

    public <T, V extends DatabaseObject<T>> boolean publish(android.database.sqlite.SQLiteDatabase openDatabase, List<V> objects, ProgressUpdater updater, T parent)
    {
        Map<String, List<V>> tables = explodePerTable(objects);

        if (tables.size() > 0) {
            try {
                for (String tableName : tables.keySet()) {
                    List<V> objectList = tables.get(tableName);

                    if (objectList != null) {
                        List<DatabaseObject<T>> updatingObjects = new ArrayList<>();
                        List<DatabaseObject<T>> insertingObjects = new ArrayList<>();
                        int existenceIterator = 0;

                        for (V currentObject : objectList) {
                            if (updater != null && !updater.onProgressState())
                                return false;

                            if (getFirstFromTable(currentObject.getWhere()) == null)
                                insertingObjects.add(currentObject);
                            else
                                updatingObjects.add(currentObject);

                            if (updater != null)
                                updater.onProgressChange(objectList.size(), existenceIterator++);
                        }

                        insert(openDatabase, insertingObjects, updater, parent);
                        update(openDatabase, updatingObjects, updater, parent);
                    }
                }

                return true;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return false;
    }

    public void reconstruct(DatabaseObject object) throws ReconstructionFailedException
    {
        reconstruct(getReadableDatabase(), object);
    }

    public void reconstruct(android.database.sqlite.SQLiteDatabase db, DatabaseObject object) throws ReconstructionFailedException
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

            throw new ReconstructionFailedException("No data was returned from: query"
                    + "; tableName: " + select.tableName
                    + "; where: " + select.where
                    + "; whereArgs: " + whereArgs.toString());
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
        return db.delete(select.tableName, select.where, select.whereArgs);
    }

    public <T extends DatabaseObject> void remove(List<T> objects)
    {
        remove(objects, null);
    }

    public <T extends DatabaseObject> void remove(List<T> objects, ProgressUpdater updater)
    {
        remove(getWritableDatabase(), objects, updater, null);
    }

    public <T, V extends DatabaseObject<T>> void remove(android.database.sqlite.SQLiteDatabase openDatabase, List<V> objects, ProgressUpdater updater, T parent)
    {
        int progress = 0;
        boolean successful = false;

        openDatabase.beginTransaction();

        try {
            for (V object : objects) {
                if (updater != null && !updater.onProgressState())
                    break;

                // remove(DatabaseObject) might be overloaded, manually complete
                SQLQuery.Select select = object.getWhere();
                openDatabase.delete(select.tableName, select.where, select.whereArgs);

                if (updater != null)
                    updater.onProgressChange(objects.size(), progress++);
            }

            openDatabase.setTransactionSuccessful();
            successful = true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            openDatabase.endTransaction();

            if (successful)
                for (V object : objects)
                    object.onRemoveObject(openDatabase, this, parent);
        }
    }

    public <V, T extends DatabaseObject<V>> void removeAsObject(android.database.sqlite.SQLiteDatabase database,
                                                                SQLQuery.Select select,
                                                                Class<T> objectType,
                                                                CastQueryListener<T> listener,
                                                                V parent)
    {
        ArrayList<T> transferList = castQuery(database, select, objectType, listener);

        remove(database, select);

        for (T object : transferList)
            object.onRemoveObject(database, this, parent);
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
        return database.update(select.tableName, values, select.where, select.whereArgs);
    }

    public <T extends DatabaseObject> void update(List<T> objects)
    {
        update(objects, null);
    }

    public <T extends DatabaseObject> void update(List<T> objects, ProgressUpdater updater)
    {
        update(getWritableDatabase(), objects, updater, null);
    }

    public <T, V extends DatabaseObject<T>> void update(android.database.sqlite.SQLiteDatabase openDatabase, List<V> objects, ProgressUpdater updater, T parent)
    {
        int progress = 0;
        boolean successful = false;

        openDatabase.beginTransaction();

        try {
            for (V object : objects) {
                if (updater != null && !updater.onProgressState())
                    break;

                // update(DatabaseObject) might be overloaded, manually complete
                SQLQuery.Select select = object.getWhere();
                openDatabase.update(select.tableName, object.getValues(), select.where, select.whereArgs);

                if (updater != null)
                    updater.onProgressChange(objects.size(), progress++);
            }

            openDatabase.setTransactionSuccessful();
            successful = true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            openDatabase.endTransaction();

            if (successful)
                for (V object : objects)
                    object.onUpdateObject(openDatabase, this, parent);
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