package bsis.dev;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UUIDChangesetGenerator {

  private static final String NEWCOLUMNNAME = "NEWCOLUMNNAME";
  private static final String OLDCOLUMNNAME = "OLDCOLUMNNAME";
  private static final String CONSTRAINTNAME = "CONSTRAINTNAME";
  private static final String TABLENAME = "TABLENAME";
  private static final String AFTERCOLUMNNAME = "AFTERCOLUMNNAME";
  private static final String UUIDCOLUMNNAME = "UUIDCOLUMNNAME";
  private static final String JOINEDTABLENAME = "JOINEDTABLENAME";
  private static final String JOINEDTABLENAMEFIELDNAME = "JOINEDTABLENAMEFIELDNAME";
  private static final String UPDATETABLENAMEFIELDNAME = "UPDATETABLENAMEFIELDNAME";
  private static final String JOINEDTABLENAMEFIELDNAME_TEMP = "JOINEDTABLENAMEFIELDNAME_TEMP";
  private static final String UPDATETABLENAMEFIELDNAME_TEMP = "UPDATETABLENAMEFIELDNAME_TEMP";
  private static final String PRIMARYKEYNAME = "PRIMARYKEYNAME";
  private static final String BASETABLENAME = "BASETABLENAME";
  private static final String BASECOLUMNTABLENAME = "BASECOLUMNTABLENAME";
  private static final String PRIMARYKEYNAME_TEMP = "PRIMARYKEYNAME_TEMP";
  private static final String TABLENAME_AUD = "TABLENAME_AUD";

  public static class TEMPLATE {
    public static final String DROP_FKR 
      = "    <dropForeignKeyConstraint baseTableName=\"["+TABLENAME+"]\" constraintName=\"["+CONSTRAINTNAME+"]\"/>";
    
    public static final String RENAME_COLUMN 
      = "    <renameColumn columnDataType=\"BIGINT\" newColumnName=\"["+NEWCOLUMNNAME+"]\" oldColumnName=\"["+OLDCOLUMNNAME+"]\" tableName=\"[TABLENAME]\"/>";
    
    public static final String ADD_NEW_COLUMN_OPEN 
      = "    <addColumn tableName=\"["+TABLENAME+"]\">"; 
    public static final String ADD_NEW_COLUMN_LINE 
      = "      <column name=\"["+NEWCOLUMNNAME+"]\" type=\"BINARY(16)\" afterColumn=\"["+AFTERCOLUMNNAME+"]\"/>"; 
    public static final String ADD_NEW_COLUMN_CLOSE    
      = "    </addColumn>";


    public static final String ADD_VIRTUAL_COLUMN
      = "    <sql dbms=\"mysql\">\n" + 
        "      ALTER TABLE ["+TABLENAME+"] ADD ["+NEWCOLUMNNAME+"] varchar(36) GENERATED ALWAYS AS (LCASE(CONCAT_WS('-', \n" + 
        "        HEX(SUBSTR(["+UUIDCOLUMNNAME+"],  1, 4)),\n" + 
        "        HEX(SUBSTR(["+UUIDCOLUMNNAME+"],  5, 2)),\n" + 
        "        HEX(SUBSTR(["+UUIDCOLUMNNAME+"],  7, 2)),\n" + 
        "        HEX(SUBSTR(["+UUIDCOLUMNNAME+"],  9, 2)),\n" + 
        "        HEX(SUBSTR(["+UUIDCOLUMNNAME+"], 11)) )))\n" + 
        "      VIRTUAL AFTER ["+UUIDCOLUMNNAME+"]\n" + 
        "    </sql>";

    public static final String SET_UUID_VALUES 
      = "    <sql>\n" + 
        "      UPDATE ["+TABLENAME+"] \n" + 
        "      SET id = GENERATEBINARYUUID();\n" + 
        "    </sql>";

    public static final String UPDATE_AUD_TABLE_WITH_NEW_UUID
      =   "    <sql>\n" + 
          "      UPDATE ["+TABLENAME_AUD+"] AS updateTable\n" + 
          "        LEFT JOIN ["+TABLENAME+"] AS joinTable ON (updateTable.["+PRIMARYKEYNAME_TEMP+"] = joinTable.["+PRIMARYKEYNAME_TEMP+"])\n" + 
          "      SET \n" + 
          "        updateTable.["+PRIMARYKEYNAME+"] = joinTable.["+PRIMARYKEYNAME+"]\n" + 
          "    </sql>";

    public static final String SET_FOREIGN_KEY_REF_VALUE 
      = "    <sql>\n" + 
        "      UPDATE ["+TABLENAME+"] AS updateTable\n" + 
        "        LEFT JOIN ["+JOINEDTABLENAME+"] AS joinedTable ON (updateTable.["+UPDATETABLENAMEFIELDNAME_TEMP+"] = joinedTable.["+JOINEDTABLENAMEFIELDNAME_TEMP+"])\n" + 
        "      SET \n" + 
        "         updateTable.["+UPDATETABLENAMEFIELDNAME+"] = joinedTable.["+JOINEDTABLENAMEFIELDNAME+"]\n" + 
        "    </sql>";

    public static final String REMOVE_AUTO_INCREMENT
      =   "    <!-- Remove auto increment from the existing id column -->\n" + 
          "    <modifyDataType\n" + 
          "            columnName=\"["+PRIMARYKEYNAME+"]\"\n" + 
          "            newDataType=\"BIGINT(20)\"\n" + 
          "            schemaName=\"bsis\"\n" + 
          "            tableName=\"["+TABLENAME+"]\"/>";
    
    public static final String DROP_AND_ADD_NEW_PRIMARY_KEY
      = "    <dropPrimaryKey \n" + 
          "            constraintName=\"PRIMARY\"\n" + 
          "            schemaName=\"bsis\"\n" + 
          "            tableName=\"["+TABLENAME+"]\"/>" + 
          "\n" + 
          "    <addPrimaryKey\n" + 
          "            columnNames=\"["+PRIMARYKEYNAME+"]\"\n" + 
          "            constraintName=\"PRIMARY\"\n" + 
          "            schemaName=\"bsis\"\n" + 
          "            tableName=\"["+TABLENAME+"]\"/>";

    public static final String DROP_COLUMN 
     = "    <dropColumn columnName=\"["+OLDCOLUMNNAME+"]\"\n" + 
         "            schemaName=\"bsis\"\n" + 
         "            tableName=\"["+TABLENAME+"]\"/>";

    public static final String ADD_FOREIGN_KEY_CONSTRAINT 
     = "    <addForeignKeyConstraint baseColumnNames=\"["+BASECOLUMNTABLENAME+"]\"\n" + 
         "            baseTableName=\"["+BASETABLENAME+"]\"\n" + 
         "            constraintName=\"["+CONSTRAINTNAME+"]\"\n" + 
         "            referencedColumnNames=\"["+PRIMARYKEYNAME+"]\"\n" + 
         "            referencedTableName=\"["+TABLENAME+"]\"/>";
  }
  private static String userName = "root";
  private static String password = "root";
  
  final static String regex = "\\[[^\\]]*\\]";
  
  final static Pattern pattern = Pattern.compile(regex);
  
  public static void generateLiquibaseChangeSet(String tableName) throws Exception {

    String pkName = getPrimaryKeyName(tableName);
    if(pkName != null) {
      List<ForeignKeyReference> refs = getForeignKeyRefs(tableName, pkName);

      //drop all foreign key constraints
      Map<String, String> map;
      for(ForeignKeyReference ref : refs) {
        map = convertFKRToMap(ref);
        System.out.println(matchAndReplace(TEMPLATE.DROP_FKR, map));
      }
      
      System.out.println();
      //rename primary key column
      System.out.println(matchAndReplace(TEMPLATE.RENAME_COLUMN, createMapForFieldRename(tableName, pkName)));
      
      System.out.println(matchAndReplace(TEMPLATE.RENAME_COLUMN, createMapForFieldRename(tableName, pkName, true)));
      
      System.out.println();
      
      //rename all FK columns
      for(ForeignKeyReference ref : refs) {
        System.out.println(matchAndReplace(TEMPLATE.RENAME_COLUMN, createMapForFieldRename(ref.tableName, ref.columnName)));
        System.out.println(matchAndReplace(TEMPLATE.RENAME_COLUMN, createMapForFieldRename(ref.tableName, ref.columnName, true)));
        
        System.out.println();
      }
      
      map = createMapForFieldAdd(tableName, pkName);
      //add new uuid primary key
      addAddColumnBasedOnMap(map);

      //add virtual column
      System.out.println(matchAndReplace(TEMPLATE.ADD_VIRTUAL_COLUMN, createMapForAddingVirtualColumn(tableName, pkName)));
      System.out.println();

      map = createMapForFieldAdd(tableName, pkName, true);
      //add new uuid primary key to AUD table
      addAddColumnBasedOnMap(map);
      
      //add virtual column
      System.out.println(matchAndReplace(TEMPLATE.ADD_VIRTUAL_COLUMN, createMapForAddingVirtualColumn(tableName, pkName, true)));
      System.out.println();
      
      //add new uuid fields for foreign keys
      String oldTableName = null;
      for(ForeignKeyReference ref : refs) {
        map = createMapForFieldAdd(ref.tableName, ref.columnName, false);
        if(ref.tableName != null && !ref.tableName.equals(oldTableName)) {
          if(oldTableName != null) {
            System.out.println(TEMPLATE.ADD_NEW_COLUMN_CLOSE);
            System.out.println();
          }
          System.out.println(matchAndReplace(TEMPLATE.ADD_NEW_COLUMN_OPEN, map));
        }
        System.out.println(matchAndReplace(TEMPLATE.ADD_NEW_COLUMN_LINE, map));
        oldTableName = ref.tableName;
      }
      if(oldTableName != null) {
        System.out.println(TEMPLATE.ADD_NEW_COLUMN_CLOSE);
        System.out.println();
      }

      for(ForeignKeyReference ref : refs) {
        System.out.println(matchAndReplace(TEMPLATE.ADD_VIRTUAL_COLUMN, createMapForAddingVirtualColumn(ref.tableName, ref.columnName)));
        System.out.println();
      }
      
      oldTableName = null;
      for(ForeignKeyReference ref : refs) {
        map = createMapForFieldAdd(ref.tableName, ref.columnName, true);
        if(ref.tableName != null && !ref.tableName.equals(oldTableName)) {
          if(oldTableName != null) {
            System.out.println(TEMPLATE.ADD_NEW_COLUMN_CLOSE);
            System.out.println();
          }
          System.out.println(matchAndReplace(TEMPLATE.ADD_NEW_COLUMN_OPEN, map));
        }
        System.out.println(matchAndReplace(TEMPLATE.ADD_NEW_COLUMN_LINE, map));
        oldTableName = ref.tableName;
      }
      if(oldTableName != null) {
        System.out.println(TEMPLATE.ADD_NEW_COLUMN_CLOSE);
        System.out.println();
      }

      for(ForeignKeyReference ref : refs) {
        System.out.println(matchAndReplace(TEMPLATE.ADD_VIRTUAL_COLUMN, createMapForAddingVirtualColumn(ref.tableName, ref.columnName, true)));
        System.out.println();
      }
      
      map = new HashMap<String, String>();
      putTableNameIntoMap(tableName, false, map);
      System.out.println(matchAndReplace(TEMPLATE.SET_UUID_VALUES, map));
      System.out.println();

      System.out.println(matchAndReplace(TEMPLATE.UPDATE_AUD_TABLE_WITH_NEW_UUID, createMapForUpdateAudTableWithNewUUIDIDValue(tableName, pkName)));
      
      System.out.println();
      for(ForeignKeyReference ref : refs) {
        System.out.println(matchAndReplace(TEMPLATE.SET_FOREIGN_KEY_REF_VALUE, createMapForeignKeyRefUpdate(tableName, ref, pkName)));
        System.out.println();
      }
      
      for(ForeignKeyReference ref : refs) {
        System.out.println(matchAndReplace(TEMPLATE.SET_FOREIGN_KEY_REF_VALUE, createMapForeignKeyRefUpdate(tableName, ref, pkName, true)));
        System.out.println();
      }

      //Remove auto increment on old primary key
      System.out.println(matchAndReplace(TEMPLATE.REMOVE_AUTO_INCREMENT, createMapForAutoIncrementAndPrimaryKeyUpdates(tableName, pkName+"_temp")));
      System.out.println();
      
      System.out.println(matchAndReplace(TEMPLATE.DROP_AND_ADD_NEW_PRIMARY_KEY, createMapForAutoIncrementAndPrimaryKeyUpdates(tableName, pkName)));
      System.out.println();

      System.out.println(matchAndReplace(TEMPLATE.DROP_COLUMN, createMapForDroppingPK(tableName, false, pkName)));
      System.out.println(matchAndReplace(TEMPLATE.DROP_COLUMN, createMapForDroppingPK(tableName, true, pkName)));

      System.out.println();
      for(ForeignKeyReference ref : refs) {
        System.out.println(matchAndReplace(TEMPLATE.DROP_COLUMN, createMapDroppingColumn(ref)));
        System.out.println();
      }

      for(ForeignKeyReference ref : refs) {
        System.out.println(matchAndReplace(TEMPLATE.DROP_COLUMN, createMapDroppingColumn(ref, true)));
        System.out.println();
      }
      
      for(ForeignKeyReference ref : refs) {
        System.out.println(matchAndReplace(TEMPLATE.ADD_FOREIGN_KEY_CONSTRAINT, createMapForAddForeignConstraint(tableName, pkName, ref)));
        System.out.println();
      }
    }
  }

  private static Map<String,String> createMapForAddForeignConstraint(
      String tableName, 
      String pkName,
      ForeignKeyReference fkr) {
    Map<String,String> map = new HashMap<String, String>();
    putTableNameIntoMap(fkr.tableName, false, map);
    map.put(BASETABLENAME, fkr.tableName);
    map.put(BASECOLUMNTABLENAME, fkr.columnName);
    map.put(CONSTRAINTNAME, fkr.constraintName);
    map.put(PRIMARYKEYNAME, pkName);
    map.put(TABLENAME, tableName);
    return map;
  }

  private static Map<String,String> createMapForUpdateAudTableWithNewUUIDIDValue(
      String tableName, 
      String pkName) {
    Map<String,String> map = new HashMap<String, String>();
    map.put(TABLENAME, tableName);
    map.put(TABLENAME_AUD, tableName +"_AUD");
    map.put(PRIMARYKEYNAME, pkName);
    map.put(PRIMARYKEYNAME_TEMP, pkName +"_temp");
    return map;
  }

  private static Map<String, String> createMapForDroppingPK(String tableName, boolean audTable, String pkName) {
    Map<String, String> map;
    map = new HashMap<String, String>();
    putTableNameIntoMap(tableName, audTable, map);
    map.put(OLDCOLUMNNAME, pkName + "_temp");
    return map;
  }

  private static Map<String,String> createMapDroppingColumn(ForeignKeyReference ref, boolean audTable) {
    Map<String,String> map = new HashMap<String, String>();
    putTableNameIntoMap(ref.tableName, audTable, map);
    map.put(OLDCOLUMNNAME, ref.columnName + "_temp");
    return map;
  }
  
  private static Map<String,String> createMapDroppingColumn(ForeignKeyReference ref) {
    return createMapDroppingColumn(ref, false);
  }
  
  private static Map<String,String> createMapForAutoIncrementAndPrimaryKeyUpdates(String tableName, String primaryKeyName) {
    Map<String,String> map = new HashMap<String, String>();
    putTableNameIntoMap(tableName, false, map);
    map.put(PRIMARYKEYNAME, primaryKeyName);
    return map;
  }

  private static Map<String,String> createMapForeignKeyRefUpdate(String tableName, ForeignKeyReference fkr, String pkName) {
    return createMapForeignKeyRefUpdate(tableName, fkr, pkName, false);
  }

  private static Map<String,String> createMapForeignKeyRefUpdate(String tableName, ForeignKeyReference fkr, String pkName, boolean audTable) {
    Map<String,String> map = new HashMap<String, String>();
    putTableNameIntoMap(fkr.tableName, audTable, map);
    map.put(JOINEDTABLENAME, tableName);
    map.put(UPDATETABLENAMEFIELDNAME_TEMP, fkr.columnName +"_temp");
    map.put(JOINEDTABLENAMEFIELDNAME_TEMP, pkName +"_temp");
    map.put(UPDATETABLENAMEFIELDNAME, fkr.columnName);
    map.put(JOINEDTABLENAMEFIELDNAME, pkName);
    return map;
  }

  private static void addAddColumnBasedOnMap(Map<String, String> map) {
    System.out.println(matchAndReplace(TEMPLATE.ADD_NEW_COLUMN_OPEN, map));
    System.out.println(matchAndReplace(TEMPLATE.ADD_NEW_COLUMN_LINE, map));
    System.out.println(TEMPLATE.ADD_NEW_COLUMN_CLOSE);
    System.out.println();
  }

  private static Map<String,String> createMapForAddingVirtualColumn(String tableName, String fieldName, boolean audTable) {
    Map<String,String> map = new HashMap<String, String>();
    putTableNameIntoMap(tableName, audTable, map);
    map.put(NEWCOLUMNNAME, fieldName +"_text");
    map.put(UUIDCOLUMNNAME, fieldName);
    return map;
  }
  
  private static Map<String,String> createMapForAddingVirtualColumn(String tableName, String fieldName) {
    return createMapForAddingVirtualColumn(tableName, fieldName, false);
  }
  
  private static Map<String,String> createMapForFieldAdd(String tableName, String fieldName, boolean audTable) {
    Map<String,String> map = new HashMap<String, String>();
    putTableNameIntoMap(tableName, audTable, map);
    map.put(NEWCOLUMNNAME, fieldName);
    map.put(AFTERCOLUMNNAME, fieldName+"_temp");
    return map;
  }
  
  private static Map<String,String> createMapForFieldAdd(String tableName, String fieldName) {
    return createMapForFieldAdd(tableName, fieldName, false);
  }

  private static Map<String,String> createMapForFieldRename(String tableName, String fieldName) {
    return createMapForFieldRename(tableName, fieldName, false);
  }

  private static Map<String,String> createMapForFieldRename(String tableName, String fieldName, boolean audTable) {
    Map<String,String> map = new HashMap<String, String>();
    putTableNameIntoMap(tableName, audTable, map);
    map.put(OLDCOLUMNNAME, fieldName);
    map.put(NEWCOLUMNNAME, fieldName+"_temp");
    return map;
  }

  private static Map<String, String> convertFKRToMap(ForeignKeyReference fkr) {
    return convertFKRToMap(fkr, false);
  }

  private static Map<String, String> convertFKRToMap(ForeignKeyReference fkr, boolean audTable) {
    Map<String,String> map = new HashMap<String, String>();
    putTableNameIntoMap(fkr.tableName, audTable, map);
    putGeneralFields(fkr, map);
    return map;
  }

  private static void putGeneralFields(ForeignKeyReference fkr, Map<String, String> map) {
    map.put(OLDCOLUMNNAME, fkr.tableName);
    map.put(NEWCOLUMNNAME, fkr.tableName+"_temp");
    map.put(CONSTRAINTNAME, fkr.constraintName);
  }
  
  private static void putTableNameIntoMap(String tableName, boolean audTable, Map<String, String> map) {
    if(audTable) {
      map.put(TABLENAME, tableName + "_AUD");
    } else {
      map.put(TABLENAME, tableName);
    }
  }

  private static String matchAndReplace(String template, Map<String, String> keyVals) {
    Set<String> keys = keyVals.keySet();
    for(String key : keys) {
      template = template.replace("[" + key + "]", keyVals.get(key));
    }
    final Matcher matcher = pattern.matcher(template);
    
    if(matcher.find()){
      System.err.println("WARNING NOT ALL VARS HAVE BEEN REPLACED IN following template \n" + template);
    }
    return template;
  }
  
  public static void main(String[] args) throws Exception {
    if (args.length < 1 || args.length > 3) {
      throw new Exception("Valid parameters are tableName [dbUserName] [dbPassword]");
    }
    
    if(args.length == 2) {
      throw new Exception("Valid parameters are tableName [dbUserName] [dbPassword]. If dbUserName is set then password has to be provided");
    } else if(args.length == 3) {
      userName = args[1];
      password = args[2];
    }
    generateLiquibaseChangeSet(args[0]);
  }

  public static String getPrimaryKeyName(String tableName) throws Exception {
    final String sql = "SHOW KEYS FROM " + tableName + " WHERE Key_name = 'PRIMARY'";
    try {
      Connection conn = getConnectionToBSISDatabase();

      PreparedStatement ps = conn.prepareStatement(sql);
      String result = null;
      try {
        ResultSet rs = ps.executeQuery();
        try {
          if (!rs.next()) {
            throw new Exception("The database returned no primary key for provided table: " + tableName);
          }
          result = (rs.getString("Column_name"));
        } finally {
          rs.close();
        }
        return result;
      } finally {
        ps.close();conn.close();
      }
    }
    catch (SQLException sqle) {
      Exception e = new Exception("Could not retrieve Primary Key of table: " + tableName);
      e.initCause(sqle);
      throw e;
    }
  }
  

  public static List<ForeignKeyReference> getForeignKeyRefs(String tableName, String primaryKeyName) throws Exception {
    final String sql = "SELECT CONSTRAINT_NAME, TABLE_NAME, COLUMN_NAME " +
          "FROM information_schema.KEY_COLUMN_USAGE " +
          "WHERE " +
            "REFERENCED_TABLE_NAME = '" + tableName + "'" +
            "AND REFERENCED_COLUMN_NAME = '"+ primaryKeyName +"' " +
            "AND TABLE_SCHEMA = 'bsis' " +
            " ORDER BY TABLE_NAME, COLUMN_NAME";
        
    try {
      Connection conn = getConnectionToBSISDatabase();

      PreparedStatement ps = conn.prepareStatement(sql);
      List<ForeignKeyReference> refs = new ArrayList<ForeignKeyReference>();
      try {
        ResultSet rs = ps.executeQuery();
        try {
          while(rs.next()) {
            ForeignKeyReference fkr = new ForeignKeyReference(
                rs.getString("CONSTRAINT_NAME"), rs.getString("TABLE_NAME"), rs.getString("COLUMN_NAME"));
            refs.add(fkr);
          }
        } finally {
          rs.close();
        }
        return refs;
      } finally {
        ps.close();conn.close();
      }
    }
    catch (SQLException sqle) {
      Exception e = new Exception("Could not retrieve Primary Key of table: " + tableName);
      e.initCause(sqle);
      throw e;
    }
  }

  private static Connection getConnectionToBSISDatabase() throws SQLException {
    String url = "jdbc:mysql://localhost:3306/bsis?useSSL=false";
    Connection conn = DriverManager.getConnection(url, userName, password);
    return conn;
  }
  
  static class ForeignKeyReference {
    public String constraintName;
    public String tableName;
    public String columnName;
    
    public ForeignKeyReference(String constraintName, String tableName, String columnName) {
      this.constraintName = constraintName;
      this.tableName = tableName;
      this.columnName = columnName;
    }
    
    public String toString() {
      return "FKR: constraintName = " +constraintName + "; tableName=" + tableName+ "; columnName="+columnName;
    }
  }
}