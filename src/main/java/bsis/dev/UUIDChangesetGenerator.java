package bsis.dev;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UUIDChangesetGenerator {

  private static final String COLUMNNAME = "COLUMNNAME";
  private static final String NEWCOLUMNNAME = "NEWCOLUMNNAME";
  private static final String OLDCOLUMNNAME = "OLDCOLUMNNAME";
  private static final String CONSTRAINTNAME = "CONSTRAINTNAME";
  private static final String TABLENAME = "TABLENAME";
  private static final String TABLENAME_LOWERCASE = "TABLENAME_LOWERCASE";
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
  private static final String FIELDTOUPDATE = "FIELDTOUPDATE";
  private static final String UPDATEFIELDTOTHISFIELD = "UPDATEFIELDTO";
  
  public static class TEMPLATE { 
    public static final String DROP_FKR 
      = "    <dropForeignKeyConstraint baseTableName=\"["+TABLENAME+"]\" constraintName=\"["+CONSTRAINTNAME+"]\"/>";
    
    public static final String RENAME_COLUMN 
      = "    <renameColumn columnDataType=\"BIGINT\" newColumnName=\"["+NEWCOLUMNNAME+"]\" oldColumnName=\"["+OLDCOLUMNNAME+"]\" tableName=\"[TABLENAME]\"/>";
    
    public static final String ADD_NEW_COLUMN_OPEN 
      = "    <addColumn tableName=\"["+TABLENAME+"]\">"; 
    public static final String ADD_NEW_COLUMN_LINE 
      = "      <column name=\"["+NEWCOLUMNNAME+"]\" type=\"BIGINT\" afterColumn=\"["+AFTERCOLUMNNAME+"]\"/>"; 
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
        "      VIRTUAL AFTER ["+UUIDCOLUMNNAME+"];\n" + 
        "    </sql>";

    public static final String SET_UUID_VALUES 
      = "    <sql>\n" + 
        "      UPDATE ["+TABLENAME+"] \n"+ 
        "      SET ["+FIELDTOUPDATE+"] = GENERATEBINARYUUID()\n" + 
        "      ORDER BY id_temp;\n" +
        "    </sql>";

    public static final String UPDATE_FIELD_WITH_ANOTHER_FIELD 
      = "    <sql>\n" + 
        "      UPDATE ["+TABLENAME+"] \n" +
        "      SET ["+FIELDTOUPDATE+"] = ["+UPDATEFIELDTOTHISFIELD+"];\n" + 
        "    </sql>";

    public static final String UPDATE_AUD_TABLE_WITH_NEW_UUID
      =   "    <sql>\n" + 
          "      UPDATE ["+TABLENAME_AUD+"] AS updateTable\n" + 
          "        LEFT JOIN ["+TABLENAME+"] AS joinTable ON (updateTable.["+PRIMARYKEYNAME_TEMP+"] = joinTable.["+PRIMARYKEYNAME_TEMP+"])\n" + 
          "      SET updateTable.["+PRIMARYKEYNAME+"] = joinTable.["+PRIMARYKEYNAME+"]\n" + 
          "      WHERE joinTable.["+PRIMARYKEYNAME+"] IS NOT null; \n" + 
          "    </sql>";

    public static final String SET_FOREIGN_KEY_REF_VALUE 
      = "    <sql>\n" + 
        "      UPDATE ["+TABLENAME+"] AS updateTable\n" + 
        "        LEFT JOIN ["+JOINEDTABLENAME+"] AS joinedTable ON (updateTable.["+UPDATETABLENAMEFIELDNAME_TEMP+"] = joinedTable.["+JOINEDTABLENAMEFIELDNAME_TEMP+"])\n" + 
        "      SET updateTable.["+UPDATETABLENAMEFIELDNAME+"] = joinedTable.["+JOINEDTABLENAMEFIELDNAME+"] \n" + 
        "      WHERE joinedTable.["+JOINEDTABLENAMEFIELDNAME+"] IS NOT null; \n" + 
        "    </sql>";

    public static final String REMOVE_AUTO_INCREMENT
      =   "    <!-- Remove auto increment from the existing id column -->\n" + 
          "    <modifyDataType columnName=\"["+PRIMARYKEYNAME+"]\" newDataType=\"BIGINT(20)\" tableName=\"["+TABLENAME+"]\"/>";
    
    public static final String DROP_AND_ADD_NEW_PRIMARY_KEY
      = "    <dropPrimaryKey constraintName=\"PRIMARY\" tableName=\"["+TABLENAME+"]\"/>" + 
          "\n" + 
          "    <addPrimaryKey columnNames=\"["+PRIMARYKEYNAME+"]\" constraintName=\"PRIMARY\" tableName=\"["+TABLENAME+"]\"/>";

    public static final String DROP_COLUMN 
     =   "    <dropColumn columnName=\"["+OLDCOLUMNNAME+"]\" tableName=\"["+TABLENAME+"]\"/>";

    public static final String ADD_FOREIGN_KEY_CONSTRAINT 
     =   "    <addForeignKeyConstraint baseColumnNames=\"["+BASECOLUMNTABLENAME+"]\" baseTableName=\"["+BASETABLENAME+"]\" constraintName=\"["+CONSTRAINTNAME+"]\"\n" + 
         "      referencedColumnNames=\"["+PRIMARYKEYNAME+"]\" referencedTableName=\"["+TABLENAME+"]\"/>";

    public static final String MODIFY_DATA_TYPE_TO_BINARY_16 
      = "    <modifyDataType columnName=\"["+COLUMNNAME+"]\" newDataType=\"BINARY(16)\" tableName=\"["+TABLENAME+"]\"/>";

    public static final String DELETE_ORPHANED_AUDIT_RECORDS 
      =   "    <!-- Delete orphaned rows from Audit table. These values uuids cannot be found since the rows were deleted in the original table-->\n" + 
          "    <sql>\n" +
          "      DELETE ["+TABLENAME_AUD+"]\n" + 
          "      FROM ["+TABLENAME_AUD+"]\n" + 
          "        LEFT JOIN ["+TABLENAME+"] AS joinTable ON (["+TABLENAME_AUD+"].["+PRIMARYKEYNAME+"] = joinTable.["+PRIMARYKEYNAME+"])\n" + 
          "      WHERE joinTable.["+PRIMARYKEYNAME+"] IS null\n" +
          "    </sql>";

    public static final String CREATE_TEMP_INDEX
      =   "    <!-- Create Index on temporary id_temp field to make updates referencing it more efficient. -->\n" +
          "    <createIndex indexName=\"["+TABLENAME_LOWERCASE+"]_["+COLUMNNAME+"]_index\" tableName=\"["+TABLENAME+"]\" unique=\"true\">\n" + 
          "        <column name=\"["+COLUMNNAME+"]\" type=\"BIGINT\"/>\n" + 
          "    </createIndex>";
  }
  private static String userName = "root";
  private static String password = "root";
  private static boolean forceGenerate = false;

  final static String regex = "\\[[^\\]]*\\]";
  
  final static Pattern pattern = Pattern.compile(regex);
  
  private static List<String> tables = new ArrayList<String>();
  private static Map<String, TableInfo> tableInfo = new HashMap<String, TableInfo>();
  static {
    initializeBSISTableInfo();
  }

  public static void generateLiquibaseChangeSet(String tableName) throws Exception {

    TableInfo mainTableInformation = tableInfo.get(tableName);
    if(mainTableInformation == null) {
      System.err.println("The able that you specified does not exist");
    }

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

      map = createMapForTempFieldAdd(tableName, pkName);
      //add temp field for BIGINT id
      addAddColumnBasedOnMap(map);

      if(mainTableInformation.hasAuditTable) {
        map = createMapForTempFieldAdd(tableName, pkName, true);
        //add temp field for BIGINT id
        addAddColumnBasedOnMap(map);
      }
      //add temporary fields for foreign keys
      String oldTableName = null;
      for(ForeignKeyReference ref : refs) {
        map = createMapForTempFieldAdd(ref.tableName, ref.columnName, false);
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

      oldTableName = null;
      for(ForeignKeyReference ref : refs) {
        if(tableInfo.get(ref.tableName).hasAuditTable) {
          map = createMapForTempFieldAdd(ref.tableName, ref.columnName, true);
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
      }
      if(oldTableName != null) {
        System.out.println(TEMPLATE.ADD_NEW_COLUMN_CLOSE);
        System.out.println();
      }

      //update temp fields with original keys######################
      System.out.println(matchAndReplace(TEMPLATE.UPDATE_FIELD_WITH_ANOTHER_FIELD, createMapForUpdateFieldWithAnotherField(tableName, pkName)));
      System.out.println();

      //Create Temp Index on primary Key temp field######################
      Map<String, String> indexMap = createMapWithTableAndFieldName(tableName, pkName + "_temp");
      indexMap.put(TABLENAME_LOWERCASE, indexMap.get(TABLENAME).toLowerCase());
      System.out.println(matchAndReplace(TEMPLATE.CREATE_TEMP_INDEX, indexMap));
      System.out.println();

      if(mainTableInformation.hasAuditTable) {
        System.out.println(matchAndReplace(TEMPLATE.UPDATE_FIELD_WITH_ANOTHER_FIELD, createMapForUpdateFieldWithAnotherField(tableName, pkName, true)));
        System.out.println();
      }

      for(ForeignKeyReference ref : refs) {
        if(tableInfo.get(ref.tableName).hasAuditTable) {
          System.out.println(matchAndReplace(TEMPLATE.UPDATE_FIELD_WITH_ANOTHER_FIELD, createMapForUpdateFieldWithAnotherField(ref.tableName, ref.columnName, true)));
          System.out.println();
        }
      }

      for(ForeignKeyReference ref : refs) {
          System.out.println(matchAndReplace(TEMPLATE.UPDATE_FIELD_WITH_ANOTHER_FIELD, createMapForUpdateFieldWithAnotherField(ref.tableName, ref.columnName)));
          System.out.println();
      }
      //##########################################################

      //Remove auto increment on old primary key
      System.out.println(matchAndReplace(TEMPLATE.REMOVE_AUTO_INCREMENT, createMapForAutoIncrementAndPrimaryKeyUpdates(tableName, pkName)));
      System.out.println();
      
      /*System.out.println(matchAndReplace(TEMPLATE.DROP_AND_ADD_NEW_PRIMARY_KEY, createMapForAutoIncrementAndPrimaryKeyUpdates(tableName, pkName)));
      System.out.println();*/
      //##########################################################



      //modify id type to binary 16###########################################
      System.out.println(matchAndReplace(TEMPLATE.MODIFY_DATA_TYPE_TO_BINARY_16, createMapWithTableAndFieldName(tableName, pkName)));
      System.out.println();

      if(mainTableInformation.hasAuditTable) {
        System.out.println(matchAndReplace(TEMPLATE.MODIFY_DATA_TYPE_TO_BINARY_16, createMapWithTableAndFieldName(tableName, pkName, true)));
        System.out.println();
      }

      for(ForeignKeyReference ref : refs) {
        System.out.println(matchAndReplace(TEMPLATE.MODIFY_DATA_TYPE_TO_BINARY_16, createMapWithTableAndFieldName(ref.tableName, ref.columnName)));
      }
      System.out.println();
    
      for(ForeignKeyReference ref : refs) {
        if(tableInfo.get(ref.tableName).hasAuditTable) {
          System.out.println(matchAndReplace(TEMPLATE.MODIFY_DATA_TYPE_TO_BINARY_16, createMapWithTableAndFieldName(ref.tableName, ref.columnName, true)));
        }
      }
      System.out.println();

      //set uuid values###########################################
      map = new HashMap<String, String>();
      putTableNameIntoMap(tableName, false, map);
      map.put(FIELDTOUPDATE, pkName);
      System.out.println(matchAndReplace(TEMPLATE.SET_UUID_VALUES, map));
      System.out.println();
      //##########################################################




      //add virtual columns ######################################
      System.out.println(matchAndReplace(TEMPLATE.ADD_VIRTUAL_COLUMN, createMapForAddingVirtualColumn(tableName, pkName)));
      System.out.println();

      if(mainTableInformation.hasAuditTable) {
        System.out.println(matchAndReplace(TEMPLATE.ADD_VIRTUAL_COLUMN, createMapForAddingVirtualColumn(tableName, pkName, true)));
        System.out.println();
      }

      for(ForeignKeyReference ref : refs) {
        if(tableInfo.get(ref.tableName).hasAuditTable) {
          System.out.println(matchAndReplace(TEMPLATE.ADD_VIRTUAL_COLUMN, createMapForAddingVirtualColumn(ref.tableName, ref.columnName, true)));
          System.out.println();
        }
      }

      for(ForeignKeyReference ref : refs) {
        System.out.println(matchAndReplace(TEMPLATE.ADD_VIRTUAL_COLUMN, createMapForAddingVirtualColumn(ref.tableName, ref.columnName)));
        System.out.println();
      }
      //#########################################################



      //set aud value to new uuid value##########################
      if(mainTableInformation.hasAuditTable) {
        System.out.println(matchAndReplace(TEMPLATE.UPDATE_AUD_TABLE_WITH_NEW_UUID, createMapForUpdateAudTableWithNewUUIDIDValue(tableName, pkName)));
      }
      //#########################################################



      //set aud value to new uuid value##########################
      if(mainTableInformation.hasAuditTable) {
        System.out.println();
        System.out.println(matchAndReplace(TEMPLATE.DELETE_ORPHANED_AUDIT_RECORDS, createMapForDeletingOrphanedRows(tableName, pkName)));
      }
      //#########################################################



      //update all forieng key references with new uuid##########
      System.out.println();
      for(ForeignKeyReference ref : refs) {
        System.out.println(matchAndReplace(TEMPLATE.SET_FOREIGN_KEY_REF_VALUE, createMapForeignKeyRefUpdate(tableName, ref, pkName)));
        System.out.println();
      }
      
      for(ForeignKeyReference ref : refs) {
        if(tableInfo.get(ref.tableName).hasAuditTable) {
          System.out.println(matchAndReplace(TEMPLATE.SET_FOREIGN_KEY_REF_VALUE, createMapForeignKeyRefUpdate(tableName, ref, pkName, true)));
          System.out.println();
        }
      }
      //#########################################################

      //Drop temporary columns###################################
      System.out.println(matchAndReplace(TEMPLATE.DROP_COLUMN, createMapForDroppingPK(tableName, false, pkName)));
      if(mainTableInformation.hasAuditTable) {
        System.out.println();
        System.out.println(matchAndReplace(TEMPLATE.DROP_COLUMN, createMapForDroppingPK(tableName, true, pkName)));
      }

      System.out.println();
      for(ForeignKeyReference ref : refs) {
        System.out.println(matchAndReplace(TEMPLATE.DROP_COLUMN, createMapDroppingColumn(ref)));
        System.out.println();
      }

      for(ForeignKeyReference ref : refs) {
        if(tableInfo.get(ref.tableName).hasAuditTable) {
          System.out.println(matchAndReplace(TEMPLATE.DROP_COLUMN, createMapDroppingColumn(ref, true)));
          System.out.println();
        }
      }
      //#########################################################

      for(ForeignKeyReference ref : refs) {
        System.out.println(matchAndReplace(TEMPLATE.ADD_FOREIGN_KEY_CONSTRAINT, createMapForAddForeignConstraint(tableName, pkName, ref)));
        System.out.println();
      }
    }
  }
  
  private static Map<String,String> createMapWithTableAndFieldName(String tableName, String fieldName, boolean audTable) {
    Map<String,String> map = new HashMap<String, String>();
    putTableNameIntoMap(tableName, audTable, map);
    map.put(COLUMNNAME, fieldName);
    return map;
  }
  
  private static Map<String,String> createMapWithTableAndFieldName(String tableName, String fieldName) {
    return createMapWithTableAndFieldName(tableName, fieldName, false);
  }

  private static Map<String,String> createMapForUpdateFieldWithAnotherField(String tableName, String fieldName, boolean audTable) {
    Map<String,String> map = new HashMap<String, String>();
    putTableNameIntoMap(tableName, audTable, map);
    map.put(FIELDTOUPDATE, fieldName +"_temp");
    map.put(UPDATEFIELDTOTHISFIELD, fieldName);
    return map;
  }

  private static Map<String,String> createMapForUpdateFieldWithAnotherField(String tableName, String fieldName) {
    return createMapForUpdateFieldWithAnotherField(tableName, fieldName, false);
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

  private static Map<String,String> createMapForDeletingOrphanedRows(
      String tableName, 
      String pkName) {
    Map<String,String> map = new HashMap<String, String>();
    map.put(TABLENAME, tableName);
    map.put(TABLENAME_AUD, tableName +"_AUD");
    map.put(PRIMARYKEYNAME, pkName);
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
  
  private static Map<String,String> createMapForTempFieldAdd(String tableName, String fieldName, boolean audTable) {
    Map<String,String> map = new HashMap<String, String>();
    putTableNameIntoMap(tableName, audTable, map);
    map.put(NEWCOLUMNNAME, fieldName +"_temp");
    map.put(AFTERCOLUMNNAME, fieldName);
    return map;
  }
  
  private static Map<String,String> createMapForTempFieldAdd(String tableName, String fieldName) {
    return createMapForTempFieldAdd(tableName, fieldName, false);
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
    template = matchAndReplaceWithoutWarning(template, keyVals);
    final Matcher matcher = pattern.matcher(template);
    
    if(matcher.find()){
      System.err.println("WARNING NOT ALL VARS HAVE BEEN REPLACED IN following template \n" + template);
    }
    return template;
  }
  
  private static String matchAndReplaceWithoutWarning(String template, Map<String, String> keyVals) {
    Set<String> keys = keyVals.keySet();
    for(String key : keys) {
      template = template.replace("[" + key + "]", keyVals.get(key));
    }

    return template;
  }
  
  public static void main(String[] args) throws Exception {
    if (args.length < 1 || args.length > 4) {
      throw new Exception("Valid parameters are tableName [dbUserName] [dbPassword] [force]. dbUserName and dbPassword is mandatory if force is specified.");
    }
    
    if(args.length == 2) {
      throw new Exception("Valid parameters are tableName [dbUserName] [dbPassword] [force]. If dbUserName is set then password has to be provided. dbUserName and dbPassword is mandatory if force is specified.");
    } else if(args.length == 3) {
      userName = args[1];
      password = args[2];
    } else if(args.length == 4) {
      userName = args[1];
      password = args[2];
      if("force".equals(args[3])) {
        forceGenerate = true;
      }
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
            String table = rs.getString("TABLE_NAME");
            String constraint = rs.getString("CONSTRAINT_NAME"); 
            if (!forceGenerate && table.endsWith("_AUD")) {
              System.err.println("Audit table: " + table + " has a foreign key constraint: " + constraint + ", this is most likely not correct. Please contact someone!!! :)");
              System.exit(0);
            }
            ForeignKeyReference fkr = new ForeignKeyReference(
                constraint, table, rs.getString("COLUMN_NAME"));
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

  private static void initializeBSISTableInfo() {
    final String sql = "SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema = 'bsis'";
      
    try {
      Connection conn = getConnectionToBSISDatabase();
  
      PreparedStatement ps = conn.prepareStatement(sql);
      try {
        ResultSet rs = ps.executeQuery();
        try {
          while(rs.next()) {
            tables.add(rs.getString(1));
          }
        } finally {
          rs.close();
        }
      } finally {
        ps.close();conn.close();
      }
    }
    catch (SQLException sqle) {
      System.err.println("Could not initialize table Info: " + sqle.getMessage());
      System.exit(0);
    }
    
    Collections.sort(tables);
    for(String table : tables) {
      if(!table.endsWith("_AUD")) {
        boolean tableHasAuditTable = false;
        for (String tablesForAuditCheck : tables) {
          if(tablesForAuditCheck.startsWith(table) && tablesForAuditCheck.endsWith("_AUD")) {
            tableHasAuditTable = true;
            break;
          }
        }
        tableInfo.put(table, new TableInfo(table, tableHasAuditTable));
      } else {
        tableInfo.put(table, new TableInfo(table, false));
      }
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
    
    @Override
    public String toString() {
      return "ForeignKeyReference [constraintName=" + constraintName + ", tableName=" + tableName + ", columnName="
          + columnName + "]";
    }
  }
  
  static class TableInfo {
    public String tableName;
    public boolean hasAuditTable;

    public TableInfo(String tableName, boolean hasAuditTable) {
      this.tableName = tableName;
      this.hasAuditTable = hasAuditTable;
    }
    
    @Override
    public String toString() {
      return "TableInfo [tableName=" + tableName + ", hasAuditTable=" + hasAuditTable + "]";
    }
  }
}
