package org.pentaho.di.core.database;

import java.sql.ResultSet;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.*;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.plugins.DatabaseMetaPlugin;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.trans.steps.tableoutput.TableOutput;
/**
 * DatabaseMeta数据库插件-Kbase数据库
 */
@DatabaseMetaPlugin(type = "KBASE", typeDescription = "Kbase数据库")
public class KbaseDatabaseMeta extends BaseDatabaseMeta implements DatabaseInterface {
    static {
        System.load("C:\\Windows\\System32\\FTSClientU.dll");
        //System.load("C:\\Windows\\System32\\KBaseClient4jU.dll");
        System.load("C:\\Windows\\System32\\KBaseClientU.dll");
        System.load("C:\\Windows\\System32\\TPIClientU.dll");
        System.load("C:\\Windows\\System32\\TPIExtClientU.dll");
    }

    private static final String STRICT_BIGNUMBER_INTERPRETATION = "STRICT_NUMBER_38_INTERPRETATION";

    /**
     * ※
     *
     * @return 支持的连接类型
     * 默认使用第一个参数类型连接，这里支持NATIVE和JNDI
     */
    @Override
    public int[] getAccessTypeList() {
        return new int[]{DatabaseMeta.TYPE_ACCESS_NATIVE, DatabaseMeta.TYPE_ACCESS_JNDI};
    }

    /**
     * ※
     * Generates the SQL statement to add a column to the specified table
     * 生成向指定表添加列的SQL语句
     *
     * @param tablename   The table to add 表名
     * @param v           The column defined as a value
     * @param tk          the name of the technical key field
     * @param use_autoinc whether or not this field uses auto increment 是否自增
     * @param pk          the name of the primary key field 主键字段名
     * @param semicolon   whether or not to add a semi-colon behind the statement.是否在语句后添加分号
     * @return the SQL statement to add a column to the specified table
     */
    @Override
    public String getAddColumnStatement(String tablename, ValueMetaInterface v, String tk, boolean use_autoinc,
                                        String pk, boolean semicolon) {
        return "ALTER TABLE " + tablename + " ADD " + getFieldDefinition(v, tk, pk, use_autoinc, true, false);
    }

    /**
     * 获取驱动类
     *
     * @return
     */
    @Override
    public String getDriverClass() {
        return getAccessType() == DatabaseMeta.TYPE_ACCESS_NATIVE ? "com.kbase.jdbc.Driver" : null;
    }

    /**
     * 获取URL
     *
     * @param hostname
     * @param port
     * @param databaseName
     * @return
     * @throws KettleDatabaseException
     */
    @Override
    public String getURL(String hostname, String port, String databaseName) throws KettleDatabaseException {
        /*if (getAccessType() == DatabaseMeta.TYPE_ACCESS_ODBC) {
            return "jdbc:odbc:" + databaseName;
        } else */
        if (getAccessType() == DatabaseMeta.TYPE_ACCESS_NATIVE) {
            // <host>/<database>
            // <host>:<port>/<database>
            String _hostname = hostname;
            String _port = port;
            String _databaseName = databaseName;
            if (Utils.isEmpty(hostname)) {
                _hostname = "localhost";
            }
            if (Utils.isEmpty(port) || port.equals("-1")) {
                _port = "4567";
            }
            if (Utils.isEmpty(databaseName)) {
                _databaseName = "BDMS";
            }
            if (!databaseName.startsWith("/")) {
                _databaseName = "/" + databaseName;
            }
            return "jdbc:kbase://" + _hostname + (Utils.isEmpty(_port) ? "" : ":" + _port);
        } else {
            throw new KettleDatabaseException("不支持的数据库连接方式[" + getAccessType() + "]");
        }
    }

    /**
     * ※修改字段类型
     * Generates the SQL statement to modify a column in the specified table
     *
     * @param tablename   The table to add
     * @param v           The column defined as a value
     * @param tk          the name of the technical key field
     * @param use_autoinc whether or not this field uses auto increment
     * @param pk          the name of the primary key field
     * @param semicolon   whether or not to add a semi-colon behind the statement.
     * @return the SQL statement to modify a column in the specified table
     * demo：alter table KETTLE alter name as 姓名 char(20)
     */
    @Override
    public String getModifyColumnStatement(String tablename, ValueMetaInterface v, String tk, boolean use_autoinc,
                                           String pk, boolean semicolon) {

        return "ALTER TABLE" + tablename + "ALTER" + getFieldDefinition(v, tk, pk, use_autoinc, true, false);
    }

    /**
     * 定义字段属性
     *
     * @param v             The column defined as a value  kettle中定义的字段的元数据对象
     * @param tk            代理主键
     * @param pk            the name of the primary key field 主键
     * @param use_autoinc   whether or not this field uses auto increment 是否自增
     * @param add_fieldname 是否为新增的字段名
     * @param add_cr        whether or not to add a semi-colon behind the statement.是否在语句后添加分号
     * @return
     */
    @Override
    public String getFieldDefinition(ValueMetaInterface v, String tk, String pk, boolean use_autoinc,
                                     boolean add_fieldname, boolean add_cr) {
        StringBuilder retval = new StringBuilder(128);

        String fieldname = v.getName();//字段名
        int length = v.getLength();//字段长度
        int precision = v.getPrecision();//精度

        if (add_fieldname) {
            retval.append(fieldname).append(" ");
        }

        int type = v.getType();
        switch (type) {
            case ValueMetaInterface.TYPE_DATE:
                retval.append("DATE");
            case ValueMetaInterface.TYPE_TIMESTAMP:
                if (this.supportsTimestampDataType()) {
                    retval.append("TIMESTAMP");
                } else {
                    retval.append("DATE");
                }
                break;
            case ValueMetaInterface.TYPE_BOOLEAN:
                retval.append("CHAR(1)");
                break;
            case ValueMetaInterface.TYPE_NUMBER:
            case ValueMetaInterface.TYPE_INTEGER:
            case ValueMetaInterface.TYPE_BIGNUMBER:
                if (fieldname.equalsIgnoreCase(tk) || // Technical key
                        fieldname.equalsIgnoreCase(pk) // Primary key
                ) {
                    retval.append("BIGSERIAL");
                } else {
                    if (length > 0) {
                        if (precision > 0 || length > 18) {
                            // Numeric(Precision, Scale): Precision = total length; Scale = decimal places
                            retval.append("NUMERIC(").append(length + precision).append(", ").append(precision).append(")");
                        } else if (precision == 0) {
                            if (length > 9) {
                                retval.append("BIGINT");
                            } else {
                                if (length < 5) {
                                    retval.append("SMALLINT");
                                } else {
                                    retval.append("INT");
                                }
                            }
                        } else {
                            retval.append("FLOAT(53)");
                        }

                    } else {
                        retval.append("DOUBLE PRECISION");
                    }
                }
                break;
            case ValueMetaInterface.TYPE_STRING:
                if (length < 1 || length >= DatabaseMeta.CLOB_LENGTH) {
                    retval.append("TEXT");
                } else {
                    retval.append("VARCHAR(").append(length).append(")");
                }
                break;
            case ValueMetaInterface.TYPE_BINARY:
                retval.append("BLOB");
                break;
            default:
                retval.append(" UNKNOWN");
                break;
        }

        if (add_cr) {
            retval.append(Const.CR);
        }

        return retval.toString();
    }


    /**
     * 默认连接端口4567
     *
     * @return
     */
    @Override
    public int getDefaultDatabasePort() {
        return getAccessType() == DatabaseMeta.TYPE_ACCESS_NATIVE ? 4567 : -1;
    }

    /**
     * 当前数据库是否支持自增类型的字段
     */
    @Override
    public boolean supportsAutoInc() {
        return true;
    }

    /**
     * 获取限制读取条数的数据，追加在select语句后实现限制返回的结果数
     *
     * @see org.pentaho.di.core.database.DatabaseInterface#getLimitClause(int)
     */
    @Override
    public String getLimitClause(int nrRows) {
        return " LIMIT " + nrRows;
    }

    /**
     * 返回获取表所有字段信息的语句(WHERE 1=0 可以保证只返回表结构而没有数据)
     *
     * @param tableName
     * @return The SQL to launch.
     */
    @Override
    public String getSQLQueryFields(String tableName) {
        return "SELECT * FROM " + tableName + " WHERE 1=0";
    }

    /**
     * 根据表名判断表是否存在
     *
     * @param tablename
     * @return
     */
    @Override
    public String getSQLTableExists(String tablename) {
        return getSQLQueryFields(tablename);
    }

    /**
     * 根据表名和字段名判断字段是否存在
     *
     * @param columnname
     * @param tablename
     * @return
     */
    @Override
    public String getSQLColumnExists(String columnname, String tablename) {
        return getSQLQueryColumnFields(columnname, tablename);
    }

    public String getSQLQueryColumnFields(String columnname, String tableName) {
        return "SELECT " + columnname + " FROM " + tableName + " WHERE 1=0";
    }

    /**
     * 是否锁定所有表
     *
     * @return
     */
    @Override
    public boolean needsToLockAllTables() {
        return false;
    }


    /**
     * Oracle doesn't support options in the URL, we need to put these in a
     * Properties object at connection time...
     * Oracle 不支持在URL中设置选项，我们连接时需要将选项放到配置对象中
     */
    @Override
    public boolean supportsOptionsInURL() {
        return false;
    }

    /**
     * @return true if the database supports sequences（数据库是否支持序列化）
     */
    @Override
    public boolean supportsSequences() {
        return true;
    }

    /**
     * Check if a sequence exists.(检查序列是否存在)
     *
     * @param sequenceName
     *            The sequence to check
     * @return The SQL to get the name of the sequence back from the databases data
     *         dictionary
     */
   /* @Override
    public String getSQLSequenceExists(String sequenceName) {
        int dotPos = sequenceName.indexOf('.');  //获取序列中'.'的下标
        String sql = "";
        if (dotPos == -1) {//如果'.'不存在
            // if schema is not specified try to get sequence which belongs to current user
            sql = "SELECT * FROM USER_SEQUENCES WHERE SEQUENCE_NAME = '" + sequenceName.toUpperCase() + "'";
        } else {
            String schemaName = sequenceName.substring(0, dotPos);
            String seqName = sequenceName.substring(dotPos + 1);
            sql = "SELECT * FROM ALL_SEQUENCES WHERE SEQUENCE_NAME = '" + seqName.toUpperCase()
                    + "' AND SEQUENCE_OWNER = '" + schemaName.toUpperCase() + "'";
        }
        return sql;
    }*/

    /**
     * Get the current value of a database sequence 获取数据库序列当前值
     *
     * @param sequenceName
     *            The sequence to check
     * @return The current value of a database sequence
     */
/*    @Override
    public String getSQLCurrentSequenceValue(String sequenceName) {
        return "SELECT " + sequenceName + ".currval FROM DUAL";
    }*/

    /**
     * Get the SQL to get the next value of a sequence. (Oracle only)
     *
     * @param sequenceName
     *            The sequence name
     * @return the SQL to get the next value of a sequence. (Oracle only)
     */
   /* @Override
    public String getSQLNextSequenceValue(String sequenceName) {
        return "SELECT " + sequenceName + ".nextval FROM dual";
    }*/

   /* @Override
    public boolean supportsSequenceNoMaxValueOption() {
        return true;
    }*/

    /**
     * @return true if we need to supply the schema-name to getTables in order to
     * get a correct list of items.
     */
    @Override
    public boolean useSchemaNameForTableList() {
        return true;
    }

    /**
     * @return true if the database supports synonyms
     */
    @Override
    public boolean supportsSynonyms() {
        return true;
    }


    /**
     * Generates the SQL statement to drop a column from the specified table
     *
     * @param tablename   The table to add
     * @param v           The column defined as a value
     * @param tk          the name of the technical key field
     * @param use_autoinc whether or not this field uses auto increment
     * @param pk          the name of the primary key field
     * @param semicolon   whether or not to add a semi-colon behind the statement.
     * @return the SQL statement to drop a column from the specified table
     */
    @Override
    public String getDropColumnStatement(String tablename, ValueMetaInterface v, String tk, boolean use_autoinc,
                                         String pk, boolean semicolon) {
        return "ALTER TABLE " + tablename + " DROP COLUMN " + v.getName() + Const.CR;
    }


    /**
     * 预留字
     *
     * @return
     */
    @Override
    public String[] getReservedWords() {
        return new String[]{"ALIASNAME", "ADD", "ALL", "ALERT", "AND", "ASC", "AS", "AT", "AUTO", "AVG", "BY", "CREATE", "CREATEVIEW",
                "DATABASE", "DATE", "DEC", "DEFAULT", "DELETE", "DESC", "DISPLAYNAME", "DISTINCT", "DROP", "DUPDB", "FACTOR", "FROM",
                "GROUP", "INDEX", "INSERT", "INTO", "IS", "LIKE", "LOWER", "LTRIM", "MAINDB", "MANUAL", "MAX", "MIN", "NOT", "NULL", "ON", "OR",
                "ORDER", "PACK", "PATH", "REFCOL", "RELEVANT", "REPLACE", "RTRIM", "SELECT", "SET", "SUBSTR", "SUM", "TABLE", "TRIM", "UPDATE",
                "UPPER", "USING", "VALUES", "VARSELECT", "VIEW", "WHERE", "WITH", "XLS"
        };
    }


    @Override
    public String getSQLLockTables(String[] tableNames) {
        StringBuilder sql = new StringBuilder(128);
        for (int i = 0; i < tableNames.length; i++) {
            sql.append("LOCK TABLE ").append(tableNames[i]).append(" IN EXCLUSIVE MODE;").append(Const.CR);
        }
        return sql.toString();
    }

    @Override
    public String getSQLUnlockTables(String[] tableNames) {
        return null; // commit handles the unlocking!
    }

    /**
     * @return extra help text on the supported options on the selected database
     * platform.
     */
    @Override
    public String getExtraOptionsHelpText() {
        return "http://www.shentongdata.com/?bid=3&eid=249";
    }

    /**
     * ※
     *
     * @return
     */
    @Override
    public String[] getUsedLibraries() {
        return new String[]{"oscarJDBC.jar", "oscarJDBC14.jar", "oscarJDBC16.jar"};
    }

    /**
     * Verifies on the specified database connection if an index exists on the
     * fields with the specified name.
     *
     * @param database   a connected database
     * @param schemaName
     * @param tableName
     * @param idx_fields
     * @return true if the index exists, false if it doesn't.
     * @throws KettleDatabaseException
     */
    @Override
    public boolean checkIndexExists(Database database, String schemaName, String tableName, String[] idx_fields)
            throws KettleDatabaseException {

        String tablename = database.getDatabaseMeta().getQuotedSchemaTableCombination(schemaName, tableName);

        boolean[] exists = new boolean[idx_fields.length];
        for (int i = 0; i < exists.length; i++) {
            exists[i] = false;
        }

        try {
            //
            // Get the info from the data dictionary...
            //
            String sql = "SELECT * FROM USER_IND_COLUMNS WHERE TABLE_NAME = '" + tableName + "'";
            ResultSet res = null;
            try {
                res = database.openQuery(sql);
                if (res != null) {
                    Object[] row = database.getRow(res);
                    while (row != null) {
                        String column = database.getReturnRowMeta().getString(row, "COLUMN_NAME", "");
                        int idx = Const.indexOfString(column, idx_fields);
                        if (idx >= 0) {
                            exists[idx] = true;
                        }

                        row = database.getRow(res);
                    }

                } else {
                    return false;
                }
            } finally {
                if (res != null) {
                    database.closeQuery(res);
                }
            }

            // See if all the fields are indexed...
            boolean all = true;
            for (int i = 0; i < exists.length && all; i++) {
                if (!exists[i]) {
                    all = false;
                }
            }

            return all;
        } catch (Exception e) {
            throw new KettleDatabaseException("Unable to determine if indexes exists on table [" + tablename + "]", e);
        }
    }

    @Override
    public boolean requiresCreateTablePrimaryKeyAppend() {
        return true;
    }

    /**
     * Most databases allow you to retrieve result metadata by preparing a SELECT
     * statement.
     *
     * @return true if the database supports retrieval of query metadata from a
     * prepared statement. False if the query needs to be executed first.
     */
    @Override
    public boolean supportsPreparedStatementMetadataRetrieval() {
        return false;
    }

    /**
     * @return The maximum number of columns in a database, <=0 means: no known
     * limit
     */
    @Override
    public int getMaxColumnsInIndex() {
        return 32;
    }

    /**
     * @return The SQL on this database to get a list of sequences.
     */
    @Override
    public String getSQLListOfSequences() {
        return "SELECT SEQUENCE_NAME FROM all_sequences";
    }

    /**
     * @param string
     * @return A string that is properly quoted for use in an Oracle SQL statement
     * (insert, update, delete, etc)
     */
    @Override
    public String quoteSQLString(String string) {
        string = string.replaceAll("'", "''");
        string = string.replaceAll("\\n", "'||chr(13)||'");
        string = string.replaceAll("\\r", "'||chr(10)||'");
        return "'" + string + "'";
    }

    /**
     * Returns a false as Oracle does not allow for the releasing of savepoints.
     */
    @Override
    public boolean releaseSavepoint() {
        return false;
    }

    @Override
    public boolean supportsErrorHandlingOnBatchUpdates() {
        return false;
    }

    /**
     * @return true if Kettle can create a repository on this type of database.
     */
    @Override
    public boolean supportsRepository() {
        return true;
    }

    @Override
    public int getMaxVARCHARLength() {
        return 2000;
    }

    /**
     * Oracle does not support a construct like 'drop table if exists', which is
     * apparently legal syntax in many other RDBMSs. So we need to implement the
     * same behavior and avoid throwing 'table does not exist' exception.
     *
     * @param tableName Name of the table to drop
     * @return 'drop table if exists'-like statement for Oracle
     */
    @Override
    public String getDropTableIfExistsStatement(String tableName) {
        return "DROP TABLE IF EXISTS " + tableName;
    }

    @Override
    public SqlScriptParser createSqlScriptParser() {
        return new SqlScriptParser(false);
    }

    /**
     * @return true if using strict number(38) interpretation
     */
    public boolean strictBigNumberInterpretation() {
        return "Y".equalsIgnoreCase(getAttributes().getProperty(STRICT_BIGNUMBER_INTERPRETATION, "N"));
    }

    /**
     * @param strictBigNumberInterpretation true if use strict number(38) interpretation
     */
    public void setStrictBigNumberInterpretation(boolean strictBigNumberInterpretation) {
        getAttributes().setProperty(STRICT_BIGNUMBER_INTERPRETATION, strictBigNumberInterpretation ? "Y" : "N");
    }
}


