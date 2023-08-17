package redshiftsql

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"go.uber.org/zap"
)

func GetColumnModifyString(diff *tidbsql.ColumnDiff) (string, error) {
	strs := make([]string, 0, 3)
	if diff.Before.Tp != diff.After.Tp || diff.Before.Precision != diff.After.Precision || diff.Before.Scale != diff.After.Scale {
		colStr, err := GetRedshiftTypeString(*diff.After)
		if err != nil {
			return "", errors.Trace(err)
		}
		strs = append(strs, fmt.Sprintf("COLUMN %s", colStr))
	}
	if diff.Before.Default != diff.After.Default {
		if diff.After.Default == nil {
			strs = append(strs, fmt.Sprintf("COLUMN %s DROP DEFAULT", diff.After.Name))
		} else {
			log.Warn("Redshift does not support update column default value", zap.String("column", diff.After.Name), zap.Any("before", diff.Before.Default), zap.Any("after", diff.After.Default))
		}
	}
	if diff.Before.Nullable != diff.After.Nullable {
		if diff.After.Nullable == "true" {
			strs = append(strs, fmt.Sprintf("COLUMN %s DROP NOT NULL", diff.After.Name))
		} else {
			strs = append(strs, fmt.Sprintf("COLUMN %s SET NOT NULL", diff.After.Name))
		}
	}
	return strings.Join(strs, ", "), nil
}

func GenDDLViaColumnsDiff(prevColumns []cloudstorage.TableCol, curTableDef cloudstorage.TableDefinition) ([]string, error) {
	if curTableDef.Type == timodel.ActionTruncateTable {
		return []string{fmt.Sprintf("TRUNCATE TABLE %s", curTableDef.Table)}, nil
	}
	if curTableDef.Type == timodel.ActionDropTable {
		return []string{fmt.Sprintf("DROP TABLE %s", curTableDef.Table)}, nil
	}
	if curTableDef.Type == timodel.ActionCreateTable {
		return nil, errors.New("Received create table ddl, which should not happen") // FIXME: drop table and create table
	}
	if curTableDef.Type == timodel.ActionRenameTables {
		return nil, errors.New("Received rename table ddl, new change data can not be capture by TiCDC any more." +
			"If you want to rename table, please start a new task to capture the new table") // FIXME: rename table to new table and rename back
	}
	// snowflake: Default CASCADE, redshift: Default RESTRICT
	if curTableDef.Type == timodel.ActionDropSchema {
		return []string{fmt.Sprintf("DROP SCHEMA %s CASCADE", curTableDef.Schema)}, nil
	}
	if curTableDef.Type == timodel.ActionCreateSchema {
		return nil, errors.New("Received create schema ddl, which should not happen") // FIXME: drop schema and create schema
	}

	columnDiff, err := tidbsql.GetColumnDiff(prevColumns, curTableDef.Columns)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ddls := make([]string, 0, len(columnDiff))
	for _, item := range columnDiff {
		ddl := ""
		switch item.Action {
		case tidbsql.ADD_COLUMN:
			ddl += fmt.Sprintf("ALTER TABLE %s ADD COLUMN ", curTableDef.Table)
			colStr, err := GetRedshiftColumnString(*item.After)
			if err != nil {
				return nil, errors.Trace(err)
			}
			ddl += colStr
		case tidbsql.DROP_COLUMN:
			ddl += fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", curTableDef.Table, item.Before.Name)
		// redshift does not support direct data type modify
		case tidbsql.MODIFY_COLUMN:
			return nil, errors.New("Received modify column ddl, which is not supported by redshift yet")
		case tidbsql.RENAME_COLUMN:
			ddl += fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", curTableDef.Table, item.Before.Name, item.After.Name)
		default:
			// UNCHANGE
		}
		if ddl != "" {
			ddl += ";"
			ddls = append(ddls, ddl)
		}
	}

	// TODO: handle primary key
	return ddls, nil
}

func getDefaultString(val interface{}) string {
	_, err := strconv.ParseFloat(fmt.Sprintf("%v", val), 64)
	if err != nil {
		return fmt.Sprintf("'%v'", val) // FIXME: escape
	}
	return fmt.Sprintf("%v", val)
}

// GetRedshiftColumnString returns a string describing the column in Redshift, e.g.
// "id INT NOT NULL DEFAULT '0'"
// Refer to:
// https://dev.mysql.com/doc/refman/8.0/en/data-types.html
// https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
func GetRedshiftColumnString(column cloudstorage.TableCol) (string, error) {
	var sb strings.Builder
	typeStr, err := GetRedshiftTypeString(column)
	if err != nil {
		return "", errors.Trace(err)
	}
	sb.WriteString(typeStr)
	if column.Nullable == "false" {
		sb.WriteString(" NOT NULL")
	}
	if column.Default != nil {
		sb.WriteString(fmt.Sprintf(` DEFAULT %s`, getDefaultString(column.Default)))
	} else if column.Nullable == "true" {
		sb.WriteString(" DEFAULT NULL")
	}
	return sb.String(), nil
}

// something wrong with the DATA_TYPE
func GetRedshiftTableColumn(db *sql.DB, sourceTable string) ([]cloudstorage.TableCol, error) {
	columnQuery := fmt.Sprintf(`SELECT COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, 
CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION
FROM information_schema.columns
WHERE table_name = '%s'`, sourceTable) // need to replace "" to ''
	rows, err := db.Query(columnQuery)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// TODO: Confirm with generated column, sequence.
	defer rows.Close()
	tableColumns := make([]cloudstorage.TableCol, 0)
	for rows.Next() {
		var column struct {
			ColumnName    string
			ColumnDefault *string
			IsNullable    string
			DataType      string
			CharMaxLength *int
			NumPrecision  *int
			NumScale      *int
			DateTimePrec  *int
		}
		err = rows.Scan(
			&column.ColumnName,
			&column.ColumnDefault,
			&column.IsNullable,
			&column.DataType,
			&column.CharMaxLength,
			&column.NumPrecision,
			&column.NumScale,
			&column.DateTimePrec,
		)
		if err != nil {
			return nil, errors.Trace(err)
		}
		var precision, scale, nullable string
		if column.NumPrecision != nil {
			precision = fmt.Sprintf("%d", *column.NumPrecision)
		} else if column.DateTimePrec != nil {
			precision = fmt.Sprintf("%d", *column.DateTimePrec)
		} else if column.CharMaxLength != nil {
			precision = fmt.Sprintf("%d", *column.CharMaxLength)
		}
		if column.NumScale != nil {
			scale = fmt.Sprintf("%d", *column.NumScale)
		}
		if column.IsNullable == "YES" {
			nullable = "true"
		} else {
			nullable = "false"
		}
		var defaultVal interface{}
		if column.ColumnDefault != nil {
			defaultVal = *column.ColumnDefault
		}
		tableCol := cloudstorage.TableCol{
			Name:      column.ColumnName,
			Tp:        column.DataType,
			Default:   defaultVal,
			Precision: precision,
			Scale:     scale,
			Nullable:  nullable,
		}
		tableColumns = append(tableColumns, tableCol)
	}
	return tableColumns, nil
}
