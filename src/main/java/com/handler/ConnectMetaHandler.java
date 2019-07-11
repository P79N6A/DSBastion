package com.handler;

import com.audit.AuditEvent;
import com.audit.AuditManager;
import com.bean.WrapConnect;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.Arrays;

import static com.handler.IOHandler.*;

public class ConnectMetaHandler {

    public static void handler(WrapConnect connect, ByteBuf src, ChannelHandlerContext out) throws SQLException {
        DatabaseMetaData metaData = connect.getMetaData();
        String mName = readByteLen(src);
        ResultSet rs = null;
        try {
            switch (mName) {
                case "allProceduresAreCallable":
                case "allTablesAreSelectable":
                case "isReadOnly":
                case "nullsAreSortedHigh":
                case "nullsAreSortedLow":
                case "nullsAreSortedAtStart":
                case "nullsAreSortedAtEnd":
                case "usesLocalFiles":
                case "usesLocalFilePerTable":
                case "supportsMixedCaseIdentifiers":
                case "storesUpperCaseIdentifiers":
                case "storesLowerCaseIdentifiers":
                case "storesMixedCaseIdentifiers":
                case "supportsMixedCaseQuotedIdentifiers":
                case "storesUpperCaseQuotedIdentifiers":
                case "storesLowerCaseQuotedIdentifiers":
                case "storesMixedCaseQuotedIdentifiers":
                case "supportsAlterTableWithAddColumn":
                case "supportsAlterTableWithDropColumn":
                case "supportsColumnAliasing":
                case "nullPlusNonNullIsNull":
                case "supportsTableCorrelationNames":
                case "supportsDifferentTableCorrelationNames":
                case "supportsExpressionsInOrderBy":
                case "supportsOrderByUnrelated":
                case "supportsGroupBy":
                case "supportsGroupByUnrelated":
                case "supportsGroupByBeyondSelect":
                case "supportsLikeEscapeClause":
                case "supportsMultipleResultSets":
                case "supportsMultipleTransactions":
                case "supportsNonNullableColumns":
                case "supportsMinimumSQLGrammar":
                case "supportsCoreSQLGrammar":
                case "supportsExtendedSQLGrammar":
                case "supportsANSI92EntryLevelSQL":
                case "supportsANSI92IntermediateSQL":
                case "supportsANSI92FullSQL":
                case "supportsIntegrityEnhancementFacility":
                case "supportsOuterJoins":
                case "supportsFullOuterJoins":
                case "supportsLimitedOuterJoins":
                case "isCatalogAtStart":
                case "supportsSchemasInDataManipulation":
                case "supportsSchemasInProcedureCalls":
                case "supportsSchemasInTableDefinitions":
                case "supportsSchemasInIndexDefinitions":
                case "supportsSchemasInPrivilegeDefinitions":
                case "supportsCatalogsInDataManipulation":
                case "supportsCatalogsInProcedureCalls":
                case "supportsCatalogsInTableDefinitions":
                case "supportsCatalogsInIndexDefinitions":
                case "supportsCatalogsInPrivilegeDefinitions":
                case "supportsPositionedDelete":
                case "supportsPositionedUpdate":
                case "supportsSelectForUpdate":
                case "supportsStoredProcedures":
                case "supportsSubqueriesInComparisons":
                case "supportsSubqueriesInExists":
                case "supportsSubqueriesInIns":
                case "supportsSubqueriesInQuantifieds":
                case "supportsCorrelatedSubqueries":
                case "supportsUnion":
                case "supportsUnionAll":
                case "supportsOpenCursorsAcrossCommit":
                case "supportsOpenCursorsAcrossRollback":
                case "supportsOpenStatementsAcrossCommit":
                case "supportsOpenStatementsAcrossRollback":
                case "doesMaxRowSizeIncludeBlobs":
                case "supportsTransactions":
                case "supportsDataDefinitionAndDataManipulationTransactions":
                case "supportsDataManipulationTransactionsOnly":
                case "dataDefinitionCausesTransactionCommit":
                case "dataDefinitionIgnoredInTransactions":
                case "supportsBatchUpdates":
                case "supportsSavepoints":
                case "supportsNamedParameters":
                case "supportsMultipleOpenResults":
                case "supportsGetGeneratedKeys":
                case "locatorsUpdateCopy":
                case "supportsStatementPooling":
                case "supportsStoredFunctionsUsingCallSyntax":
                case "autoCommitFailureClosesAllResultSets":
                case "generatedKeyAlwaysReturned":
                    AuditManager.getInstance().audit(new AuditEvent(connect.getAddress(),
                            connect.getAK(), mName));
                    Method method = metaData.getClass().getDeclaredMethod(mName);
                    boolean bool = (boolean) method.invoke(metaData);
                    out.write(writeShortStr(OK, bool));
                    break;
                case "getURL":
                case "getDatabaseProductName":
                case "getDatabaseProductVersion":
                case "getDriverName":
                case "getDriverVersion":
                case "getIdentifierQuoteString":
                case "getSQLKeywords":
                case "getNumericFunctions":
                case "getStringFunctions":
                case "getSystemFunctions":
                case "getTimeDateFunctions":
                case "getSearchStringEscape":
                case "getExtraNameCharacters":
                case "getSchemaTerm":
                case "getProcedureTerm":
                case "getCatalogTerm":
                case "getCatalogSeparator":
                    AuditManager.getInstance().audit(new AuditEvent(connect.getAddress(),
                            connect.getAK(), mName));
                    method = metaData.getClass().getDeclaredMethod(mName);
                    String str = (String) method.invoke(metaData);
                    out.write(writeShortStr(OK, str));
                    break;
                case "getUserName":
                    AuditManager.getInstance().audit(new AuditEvent(connect.getAddress(),
                            connect.getAK(), mName));
                    out.write(writeShortStr(OK, connect.getDbUser()));
                    break;
                case "getDriverMajorVersion":
                case "getDriverMinorVersion":
                case "getMaxBinaryLiteralLength":
                case "getMaxCharLiteralLength":
                case "getMaxColumnNameLength":
                case "getMaxColumnsInGroupBy":
                case "getMaxColumnsInIndex":
                case "getMaxColumnsInOrderBy":
                case "getMaxColumnsInSelect":
                case "getMaxColumnsInTable":
                case "getMaxConnections":
                case "getMaxCursorNameLength":
                case "getMaxIndexLength":
                case "getMaxSchemaNameLength":
                case "getMaxProcedureNameLength":
                case "getMaxCatalogNameLength":
                case "getMaxRowSize":
                case "getMaxStatementLength":
                case "getMaxStatements":
                case "getMaxTableNameLength":
                case "getMaxTablesInSelect":
                case "getMaxUserNameLength":
                case "getDefaultTransactionIsolation":
                case "getResultSetHoldability":
                case "getDatabaseMajorVersion":
                case "getDatabaseMinorVersion":
                case "getJDBCMajorVersion":
                case "getJDBCMinorVersion":
                case "getSQLStateType":
                    AuditManager.getInstance().audit(new AuditEvent(connect.getAddress(),
                            connect.getAK(), mName));
                    method = metaData.getClass().getDeclaredMethod(mName);
                    int i = (int) method.invoke(metaData);
                    out.write(writeInt(OK, i));
                    break;
                case "supportsConvert":
                    short mc = src.readByte();
                    if (0 == mc) {
                        AuditManager.getInstance().audit(new AuditEvent(connect.getAddress(),
                                connect.getAK(), mName));
                        out.write(writeShortStr(OK, metaData.supportsConvert()));
                    } else if (2 == mc) {
                        int fromType = src.readInt();
                        int toType = src.readInt();
                        AuditManager.getInstance().audit(new AuditEvent(connect.getAddress(),
                                connect.getAK(), mName, fromType, toType));
                        out.write(writeShortStr(OK, metaData.supportsConvert(fromType, toType)));
                    } else throw new SQLException("supportsConvert param num[" + mc + "] is not exist");
                    break;
                case "supportsTransactionIsolationLevel":
                case "supportsResultSetType":
                case "ownUpdatesAreVisible":
                case "ownDeletesAreVisible":
                case "ownInsertsAreVisible":
                case "othersUpdatesAreVisible":
                case "othersDeletesAreVisible":
                case "othersInsertsAreVisible":
                case "updatesAreDetected":
                case "deletesAreDetected":
                case "insertsAreDetected":
                case "supportsResultSetHoldability":
                    i = src.readInt();
                    AuditManager.getInstance().audit(new AuditEvent(connect.getAddress(),
                            connect.getAK(), mName, i));
                    method = metaData.getClass().getDeclaredMethod(mName, Integer.class);
                    bool = (boolean) method.invoke(metaData, i);
                    out.write(writeShortStr(OK, bool));
                    break;
                case "getProcedures":
                case "getTablePrivileges":
                case "getVersionColumns":
                case "getPrimaryKeys":
                case "getImportedKeys":
                case "getExportedKeys":
                case "getSuperTypes":
                case "getSuperTables":
                case "getFunctions":
                    str = readShortLen(src);
                    String str1 = readShortLen(src);
                    String str2 = readShortLen(src);
                    AuditManager.getInstance().audit(new AuditEvent(connect.getAddress(),
                            connect.getAK(), mName, str, str1, str2));
                    method = metaData.getClass().getDeclaredMethod(mName, String.class, String.class, String.class);
                    rs = (ResultSet) method.invoke(metaData, str, str1, str2);
                    writeResultSet(rs, out);
                    break;
                case "getProcedureColumns":
                case "getColumns":
                case "getColumnPrivileges":
                case "getAttributes":
                case "getFunctionColumns":
                case "getPseudoColumns":
                    str = readShortLen(src);
                    str1 = readShortLen(src);
                    str2 = readShortLen(src);
                    String str3 = readShortLen(src);
                    AuditManager.getInstance().audit(new AuditEvent(connect.getAddress(),
                            connect.getAK(), mName, str, str1, str2, str3));
                    method = metaData.getClass().getDeclaredMethod(mName, String.class, String.class, String.class,
                            String.class);
                    rs = (ResultSet) method.invoke(metaData, str, str1, str2, str3);
                    writeResultSet(rs, out);
                    break;
                case "getTables":
                    str = readShortLen(src);
                    str1 = readShortLen(src);
                    str2 = readShortLen(src);
                    String[] str4 = readShortLen(src.readShort(), src);
                    AuditManager.getInstance().audit(new AuditEvent(connect.getAddress(),
                            connect.getAK(), mName, str, str1, str2, Arrays.toString(str4)));
                    rs = metaData.getTables(str, str1, str2, str4);
                    writeResultSet(rs, out);
                    break;
                case "getCatalogs":
                case "getTableTypes":
                case "getTypeInfo":
                case "getClientInfoProperties":
                    AuditManager.getInstance().audit(new AuditEvent(connect.getAddress(),
                            connect.getAK(), mName));
                    method = metaData.getClass().getDeclaredMethod(mName);
                    rs = (ResultSet) method.invoke(metaData);
                    writeResultSet(rs, out);
                    break;
                case "getBestRowIdentifier":
                    str = readShortLen(src);
                    str1 = readShortLen(src);
                    str2 = readShortLen(src);
                    i = src.readInt();
                    str3 = readByteLen(src);
                    AuditManager.getInstance().audit(new AuditEvent(connect.getAddress(),
                            connect.getAK(), mName, str, str1, str2, i, str3));
                    rs = metaData.getBestRowIdentifier(str, str1, str2, i, "true".equals(str3));
                    writeResultSet(rs, out);
                    break;
                case "getCrossReference":
                    str = readShortLen(src);
                    str1 = readShortLen(src);
                    str2 = readShortLen(src);
                    str3 = readShortLen(src);
                    String str5 = readShortLen(src);
                    String str6 = readShortLen(src);
                    AuditManager.getInstance().audit(new AuditEvent(connect.getAddress(),
                            connect.getAK(), mName, str, str1, str2, str3, str5, str6));
                    rs = metaData.getCrossReference(str, str1, str2, str3, str5, str6);
                    writeResultSet(rs, out);
                    break;
                case "getIndexInfo":
                    str = readShortLen(src);
                    str1 = readShortLen(src);
                    str2 = readShortLen(src);
                    str3 = readByteLen(src);
                    str5 = readByteLen(src);
                    AuditManager.getInstance().audit(new AuditEvent(connect.getAddress(),
                            connect.getAK(), mName, str, str1, str2, str3, str5));
                    rs = metaData.getIndexInfo(str, str1, str2, "true".equals(str3), "true".equals(str5));
                    writeResultSet(rs, out);
                    break;
                case "supportsResultSetConcurrency":
                    i = src.readInt();
                    int i1 = src.readInt();
                    AuditManager.getInstance().audit(new AuditEvent(connect.getAddress(),
                            connect.getAK(), mName, i, i1));
                    out.write(writeShortStr(OK, metaData.supportsResultSetConcurrency(i, i1)));
                    break;
                case "getUDTs":
                    str = readShortLen(src);
                    str1 = readShortLen(src);
                    str2 = readShortLen(src);
                    int[] i2 = readInt(src.readShort(), src);
                    AuditManager.getInstance().audit(new AuditEvent(connect.getAddress(),
                            connect.getAK(), mName, str, str1, str2, Arrays.toString(i2)));
                    rs = metaData.getUDTs(str, str1, str2, i2);
                    writeResultSet(rs, out);
                    break;
                case "getRowIdLifetime":
                    AuditManager.getInstance().audit(new AuditEvent(connect.getAddress(),
                            connect.getAK(), mName));
                    RowIdLifetime rowIdLifetime = metaData.getRowIdLifetime();
                    switch (rowIdLifetime) {
                        case ROWID_UNSUPPORTED:
                            out.write(writeInt(OK, 1));
                            break;
                        case ROWID_VALID_OTHER:
                            out.write(writeInt(OK, 2));
                            break;
                        case ROWID_VALID_SESSION:
                            out.write(writeInt(OK, 3));
                            break;
                        case ROWID_VALID_TRANSACTION:
                            out.write(writeInt(OK, 4));
                            break;
                        case ROWID_VALID_FOREVER:
                            out.write(writeInt(OK, 5));
                            break;
                        default:
                            throw new SQLException("getRowIdLifetime[" + rowIdLifetime + "] is not defined");
                    }
                    break;
                case "getSchemas":
                    mc = src.readByte();
                    if (0 == mc) {
                        AuditManager.getInstance().audit(new AuditEvent(connect.getAddress(),
                                connect.getAK(), mName));
                        rs = metaData.getSchemas();
                    } else if (2 == mc) {
                        str = readShortLen(src);
                        str1 = readShortLen(src);
                        AuditManager.getInstance().audit(new AuditEvent(connect.getAddress(),
                                connect.getAK(), mName, str, str1));
                        rs = metaData.getSchemas(str, str1);
                    } else throw new SQLException("getSchemas param num[" + mc + "] is not exist");
                    writeResultSet(rs, out);
                    break;
                default:
                    throw new SQLException("DatabaseMetaData method[" + mName + "] is not support");
            }
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new SQLException(e);
        }
        try {
            if (rs != null) rs.close();
        } catch (SQLException ignored) {
        }
    }

    private static void writeResultSet(ResultSet rs, ChannelHandlerContext out) throws SQLException {
        out.write(writeByte(OK));
        if (rs != null) {
            out.write(writeShort(0x00));
            ResultSetMetaData rsMeta = rs.getMetaData();
            int colCount = rsMeta.getColumnCount();
            out.write(writeShort(colCount));
            for (int i = 1; i <= colCount; i++) {
                ByteBuf buf = Unpooled.buffer();
                writeShortString(rsMeta.getCatalogName(i), buf);
                writeShortString(rsMeta.getSchemaName(i), buf);
                writeShortString(rsMeta.getTableName(i), buf);
                writeShortString(rsMeta.getColumnLabel(i), buf);
                writeShortString(rsMeta.getColumnName(i), buf);
                writeShortString(rsMeta.getColumnTypeName(i), buf);
                buf.writeInt(rsMeta.getColumnDisplaySize(i));
                buf.writeInt(rsMeta.getPrecision(i));
                buf.writeInt(rsMeta.getScale(i));
                buf.writeInt(rsMeta.getColumnType(i));
                out.write(buf);
            }
            while (rs.next()) {
                ByteBuf buf = Unpooled.buffer();
                buf.writeByte(0x7e);
                for (int j = 1; j <= colCount; j++) {
                    byte[] bytes = rs.getBytes(j);
                    if (bytes == null) buf.writeInt(~0);
                    else {
                        buf.writeInt(bytes.length);
                        buf.writeBytes(bytes);
                    }
                }
                out.write(buf);
            }
            out.write(writeByte((byte) 0x7f));
        } else out.write(writeShort(-1));
    }
}
