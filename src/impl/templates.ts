import { Limit, SortingData, Expression, AnyButFunction, Obj, Property, QuerySyntaxEnum } from '@chego/chego-api';
import { isAlias, isRowId, getLabel } from '@chego/chego-tools';
import { Row, Join, JoinType, isQueryResult } from '@chego/chego-nosql';
import { MongoSyntaxTemplate } from '../api/types';


export const getQueryResultValues = (data: AnyButFunction): AnyButFunction[] => {
    const results: AnyButFunction[] = [];
    Object.values(data).forEach((table: Obj) =>
        Object.values(table).forEach((row: Obj) => results.push(...Object.values(row)))
    );
    return results;
}


const isIn = (key: string, ...values: AnyButFunction[]): object => ({ [key]: { "$in": values } });
const isEq = (key: string, value: AnyButFunction): object => ({ [key]: value });
const isGt = (key: string, value: AnyButFunction): object => ({ [key]: { "$gt": value } });
const isLt = (key: string, value: AnyButFunction): object => ({ [key]: { "$lt": value } });
const isBetween = (key: string, min: AnyButFunction, max: AnyButFunction): object => ({ [key]: { "$gt": min, "$lt": max } });
const isLikeString = (key: string, value: string): object => ({ [key]: new RegExp(`^${value.replace(/(?<!\\)\%/g, '.*').replace(/(?<!\\)\_/g, '.')}$`, 'g') });

const runCondition = (condition: (key: string, ...values: AnyButFunction[]) => object, key: string, ...values: any[]): object => {
    const data: AnyButFunction[] = [];
    values.forEach((value: any) => {
        if (isQueryResult(value)) {
            const values: AnyButFunction[] = getQueryResultValues(value.getData());
            data.push(...values);
        } else {
            data.push(value);
        }
    });
    return condition(key, ...data);
}

const showDefinedProperties = (selection: object, property: Property) => 
    Object.assign(selection, { [property.name]: 1 });

const isLimitedSelection = (data: Property[]): boolean =>
    data.reduce((result, property:Property) => property.name !== '*' ? true : result, false);

const select: MongoSyntaxTemplate = (data: Property[]) => {
    const selection:any[] = [{
        $group: {
            _id: "$_id",
            "doc": {
                "$first": "$$ROOT"
            }
        }
    }, {
        "$replaceRoot": {
            "newRoot": "$doc"
        }
    }];
    if(isLimitedSelection(data)) {
        selection.push({ $project: data.reduce(showDefinedProperties, {}) })
    }
    return selection;
}

const conditionTemplate = (condition: (key: string, ...values: AnyButFunction[]) => object, property: Property, ...values: any[]): object =>
    isRowId(property)
        ? runCondition(condition, '_id', ...values)
        : runCondition(condition, property.name, ...values);

const whereIn: MongoSyntaxTemplate = (property: Property, ...values: any[]) =>
    conditionTemplate(isIn, property, ...values);

const eq: MongoSyntaxTemplate = (property: Property, value: any) =>
    conditionTemplate(isEq, property, value);

const isNull: MongoSyntaxTemplate = (property: Property) => eq(property, null);

const gt: MongoSyntaxTemplate = (property: Property, value: any) =>
    conditionTemplate(isGt, property, value);

const lt: MongoSyntaxTemplate = (property: Property, value: any) =>
    conditionTemplate(isLt, property, value);

const between: MongoSyntaxTemplate = (property: Property, min: number, max: number) =>
    conditionTemplate(isBetween, property, min, max);

const like: MongoSyntaxTemplate = (property: Property, value: any) =>
    typeof value === 'string'
        ? conditionTemplate(isLikeString, property, value)
        : conditionTemplate(isEq, property, value);

const exists: MongoSyntaxTemplate = (value: any) => {
    const data = value.getData();
    return Array.isArray(data) ? ({ $exists: data.length > 0 }) : null;
}

const parseOrderBy = (sorting: object, sort: SortingData) =>
    Object.assign(sorting, { [sort.property.name]: sort.order })

const orderBy: MongoSyntaxTemplate = (orderBy: SortingData[]) =>
    ({ $sort: orderBy.reduce(parseOrderBy, {}) });

const parseGroupBy = (sorting: object, sort: SortingData) =>
    Object.assign(sorting, { [sort.property.name]: `$${sort.property.name}` })

const groupBy: MongoSyntaxTemplate = (groupBy: SortingData[]) =>
    ({ $group: { _id: groupBy.reduce(parseGroupBy, {}) } });

const leftJoin: MongoSyntaxTemplate = (join: Join) => {
    const label: string = getLabel(join.propertyA.table);
    return [{
        $lookup: {
            from: join.propertyB.table.name,
            localField: join.propertyA.name,
            foreignField: join.propertyB.name,
            as: label
        }
    },
    {
        $unwind: label
    }];
}


const limit: MongoSyntaxTemplate = (limit: Limit) =>
    (limit.count)
        ? [{ $skip: limit.offsetOrCount }, { $limit: limit.count }]
        : { $limit: limit.offsetOrCount };

const and: MongoSyntaxTemplate = (expressions: object[]) => ({ $and: expressions });
const or: MongoSyntaxTemplate = (expressions: object[]) => ({ $or: expressions });
const not: MongoSyntaxTemplate = (expression: object) => ({ $not: expression });


export const templates: Map<QuerySyntaxEnum, MongoSyntaxTemplate> = new Map<QuerySyntaxEnum, MongoSyntaxTemplate>([
    [QuerySyntaxEnum.Select, select],
    [QuerySyntaxEnum.EQ, eq],
    [QuerySyntaxEnum.Null, isNull],
    [QuerySyntaxEnum.GT, gt],
    [QuerySyntaxEnum.LT, lt],
    [QuerySyntaxEnum.And, and],
    [QuerySyntaxEnum.Or, or],
    [QuerySyntaxEnum.Not, not],
    [QuerySyntaxEnum.Between, between],
    [QuerySyntaxEnum.Like, like],
    [QuerySyntaxEnum.In, whereIn],
    [QuerySyntaxEnum.Exists, exists],
    [QuerySyntaxEnum.Limit, limit],
    [QuerySyntaxEnum.GroupBy, groupBy],
    [QuerySyntaxEnum.OrderBy, orderBy],
    [QuerySyntaxEnum.LeftJoin, leftJoin],
    // [QuerySyntaxEnum.Join, exists],
    // [QuerySyntaxEnum.RightJoin, exists],
    // [QuerySyntaxEnum.FullJoin, exists]
    // [QuerySyntaxEnum.Union, exists],
    // [QuerySyntaxEnum.UnionAll, exists],
]);