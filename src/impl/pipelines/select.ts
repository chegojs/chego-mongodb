import { templates } from './../templates';
import { Db } from 'mongodb';
import { IQueryContext, DataMap, newDataMap, newRow, Row, Join, OutputDataSnapshot } from '@chego/chego-nosql';
import { Table, QuerySyntaxEnum, ExpressionOrExpressionScope, Expression, SortingOrderEnum, Property } from '@chego/chego-api';
import { isExpressionScope } from '@chego/chego-tools';
import { mergeTableB2TableA } from '../joins';
import { storeOnlyUniqueEntriesIfRequired, applyUnionsIfAny } from '../unions';

export const parseRowsToArray = (result: any[], row: Row): any[] => (result.push(Object.assign({}, row.content)), result);
// export const parseRowsToObject = (result: any, row: Row): any => (Object.assign(result, { [row.key]: row.content }), result);

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

const parseExpression = (expression: Expression): object => {
    if (!templates.has(expression.type)) {
        throw new Error(`Template for ${QuerySyntaxEnum[expression.type]} not found`)
    }
    const result: object = templates.get(expression.type)(expression.property, expression.value);
    return (expression.not)
        ? templates.get(QuerySyntaxEnum.Not)(result)
        : result;
}

const xxx = (match: object[], current: ExpressionOrExpressionScope): object[] => {
    if (isExpressionScope(current)) {
        const template = templates.get(current.type);
        const expressions: object[] = current.expressions.reduce(xxx, []);
        match.push(template(expressions));
    } else {
        match.push(parseExpression(current));
    }
    return match;
}

export const buildConditions = (match: object, current: ExpressionOrExpressionScope): object => {
    if (isExpressionScope(current)) {
        const template = templates.get(current.type);
        const expressions: object[] = current.expressions.reduce(xxx, []);
        return Object.assign(match, template(expressions));
    } else {
        return Object.assign(match, parseExpression(current))
    }
}

const handleLeftJoin = (list: object[], join: Join) => {
    if (join.type === QuerySyntaxEnum.LeftJoin) {
        const joins: object[] = <object[]>templates.get(QuerySyntaxEnum.LeftJoin)(join);
        list.push(...joins);
    }
    return list;
}

const buildPielineOptions = (queryContext: IQueryContext): object[] => {
    const options: object[] = [];

    if (queryContext.joins.length) {
        const joins: object[] = queryContext.joins.reduce(handleLeftJoin, []);
        options.push(...joins);
    }

    if (queryContext.conditions.length) {
        options.push({ $match: queryContext.conditions.reduce(buildConditions, {}) });
    }

    if (queryContext.data.length) {
        const selection: object | object[] = templates.get(QuerySyntaxEnum.Select)(queryContext.data);
        if (Array.isArray(selection)) {
            options.push(...selection);
        } else {
            options.push(selection);
        }
    }

    if (queryContext.groupBy.length) {
        options.push(templates.get(QuerySyntaxEnum.GroupBy)(queryContext.groupBy));
    }

    if (queryContext.orderBy.length) {
        options.push(templates.get(QuerySyntaxEnum.OrderBy)(queryContext.orderBy));
    }

    if (queryContext.limit) {
        const limit: object | object[] = templates.get(QuerySyntaxEnum.Limit)(queryContext.limit);
        if (Array.isArray(limit)) {
            options.push(...limit);
        } else {
            options.push(limit);
        }
    }

    return options;
}

export const getTableContent = async (ref: Db, table: Table, options: object[]): Promise<any> =>
    new Promise((resolve, reject) =>
        ref.collection(table.name).aggregate(options).toArray()
            .then((results: object[]) => parseDataSnapshotToRows(table, results))
            .then(resolve)
            .catch(reject));


const joinTablesIfRequired = async (ref: Db, queryContext: IQueryContext, results: DataMap, options: object[]): Promise<DataMap> => {
    for (const join of queryContext.joins) {
        if (join.type !== QuerySyntaxEnum.LeftJoin) {
            await getTableContent(ref, join.propertyB.table, options).then(mergeTableB2TableA(join, results));
        }
    }
    return Promise.resolve(results);
}

export const convertMapToOutputData = (tablesMap: DataMap): OutputDataSnapshot => {
    const results: OutputDataSnapshot = {};
    tablesMap.forEach((rows: Row[], table: string) => {
        Object.assign(results, { [table]: rows.reduce(parseRowsToArray, []) });
    }, results);
    return results;
}

const idIsNotRequired = (result:boolean, property: Property): boolean => 
    property.name !== '_id' ? true : result;

const removeIdFromContent = (row:Row) => {
    delete row.content['_id'];
}

const removeIdsIfUnnecessary = (queryContext: IQueryContext) => (rows:Row[]): Row[] => {
    if(queryContext.data.reduce(idIsNotRequired, false)) {
        rows.forEach(removeIdFromContent);
    }
    return rows;
}

export const runSelectPipeline = async (ref: Db, queryContext: IQueryContext): Promise<any> =>
    new Promise(async (resolve, reject) => {
        const map: DataMap = newDataMap();
        const pipelineOptions: object[] = buildPielineOptions(queryContext);
        for (const table of queryContext.tables) {
            // skip conditions not related to current table
            await getTableContent(ref, table, pipelineOptions)
            .then(removeIdsIfUnnecessary(queryContext))
            .then((content) => map.set(table.name, content))
            .catch(reject);
        }
        // parse map to object
        return joinTablesIfRequired(ref, queryContext, map, pipelineOptions)
            .then(storeOnlyUniqueEntriesIfRequired(queryContext))
            .then(applyUnionsIfAny(queryContext))
            .then(convertMapToOutputData)
            .then(resolve)
            .catch(reject);
        // console.log('-XXX', JSON.stringify(postProduction.get('superheroes')));
        // resolve({});
    });




/*
executeQuery(ref, queryContext)
    .then(joinTablesIfRequired(ref, queryContext))
    .then(storeOnlyUniqueEntriesIfRequired(queryContext))
    .then(applyUnionsIfAny(queryContext))
    .then(filterQueryResultsIfRequired(queryContext))
    .then(applyMySQLFunctionsIfAny(queryContext))
    .then(convertMapToOutputData)
    .then(groupResultsIfRequired(queryContext))
    .then(orderResultsIfRequired(queryContext))
    .then(spliceQueryResultsIfRequired(queryContext.limit))
    .then(resolve)
    .catch(reject)
*/