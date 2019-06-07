import { Db } from 'mongodb';
import { Table, QuerySyntaxEnum, Property } from '@chego/chego-api';
import { isRowId } from '@chego/chego-tools';
import { mergeTableB2TableA } from '../joins';
import { storeOnlyUniqueEntriesIfRequired, applyUnionsIfAny } from '../unions';
import { newPipelineBuilder } from '../pipelineBuilder'
import { Row, newRow, IQueryContext, DataMap, OutputDataSnapshot, newDataMap } from '@chego/chego-database-boilerplate';

export const parseRowsToArray = (result: any[], row: Row): any[] => (result.push(Object.assign({}, row.content)), result);

export const parseDataSnapshotToRows = (table: Table, data: any[]): Row[] => {
    const rows: Row[] = [];
    for (const content of data) {
        rows.push(newRow({
            table,
            key: content['_id'].toString(),
            scheme: Object.keys(content),
            content
        }))
    }
    return rows;
}

export const getTableContent = async (ref: Db, table: Table, queryContext: IQueryContext): Promise<any> =>
    new Promise(async (resolve, reject) => {
        const options: object[] = newPipelineBuilder().with(queryContext, table).build();
        return table
            ? ref.collection(table.name).aggregate(options).toArray()
                .then((results: object[]) => parseDataSnapshotToRows(table, results))
                .then(resolve)
                .catch(reject)
            : reject('table is undefined');
    });


const joinTablesIfRequired = async (ref: Db, queryContext: IQueryContext, results: DataMap): Promise<DataMap> =>
    new Promise(async (resolve, reject) => {
        for (const join of queryContext.joins) {
            if (join.type !== QuerySyntaxEnum.LeftJoin) {
                await getTableContent(ref, join.propertyB.table, queryContext)
                    .then(mergeTableB2TableA(join, results))
                    .catch(reject);
            }
        }
        return resolve(results);
    });

export const convertMapToOutputData = (tablesMap: DataMap): OutputDataSnapshot => {
    const results: OutputDataSnapshot = {};
    tablesMap.forEach((rows: Row[], table: string) => {
        Object.assign(results, { [table]: rows.reduce(parseRowsToArray, []) });
    }, results);
    return results;
}

const isIdNotRequired = (result: boolean, property: Property): boolean =>
    property.name === '_id' || isRowId(property) ? false : result;

const removeIdFromContent = (row: Row) => {
    delete row.content['_id'];
}

const removeIdsIfUnnecessary = (queryContext: IQueryContext) => (rows: Row[]): Row[] => {
    if (queryContext.data.reduce(isIdNotRequired, true)) {
        rows.forEach(removeIdFromContent);
    }
    return rows;
}

export const runSelectPipeline = async (ref: Db, queryContext: IQueryContext): Promise<any> =>
    new Promise(async (resolve, reject) => {
        const map: DataMap = newDataMap();
        for (const table of queryContext.tables) {
            await getTableContent(ref, table, queryContext)
                .then(removeIdsIfUnnecessary(queryContext))
                .then((content) => map.set(table.name, content))
                .catch(reject);
        }

        return joinTablesIfRequired(ref, queryContext, map)
            .then(storeOnlyUniqueEntriesIfRequired(queryContext))
            .then(applyUnionsIfAny(queryContext))
            .then(convertMapToOutputData)
            .then(resolve)
            .catch(reject);
    });
