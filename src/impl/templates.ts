import { Limit, SortingData, AnyButFunction, Obj, Property, QuerySyntaxEnum, FunctionData } from '@chego/chego-api';
import { isRowId, getLabel, isProperty, isMySQLFunction } from '@chego/chego-tools';
import { MongoSyntaxTemplate } from '../api/types';
import { isQueryResult, Join } from '@chego/chego-database-boilerplate';
import { isObject } from 'util';

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

const showDefinedProperties = (selection: object, data: Property | FunctionData) => {
    if (isRowId(data)) {
        return Object.assign(selection, { '_id': 1 });
    } else if (isMySQLFunction(data)) {
        const params = isObject(data.param) && !isProperty(data.param) ? Object.values(data.param) : [data.param];
        const entry = templates.get(data.type)(data.alias, ...params);
        return Object.assign(selection, entry);
    } else {
        return Object.assign(selection, { [(<Property>data).name]: 1 });
    }
}

const isLimitedSelection = (data: Property[]): boolean =>
    data.reduce((result, property: Property) => property.name !== '*' ? true : result, false);

const select: MongoSyntaxTemplate = (data: Property[]) => {
    const selection: any[] = [{
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
    if (isLimitedSelection(data)) {
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
        : [{ $limit: limit.offsetOrCount }];

const and: MongoSyntaxTemplate = (expressions: object[]) => ({ $and: expressions });
const or: MongoSyntaxTemplate = (expressions: object[]) => ({ $or: expressions });
const not: MongoSyntaxTemplate = (expression: object) => ({ $not: expression });

const listReferences = (props: string[], current: any) =>
    (props.push(isProperty(current) ? `$${current.name}` : current), props);

const min: MongoSyntaxTemplate = (alias: string, property: Property) => [{
    $project: {
        [alias]: `$${property.name}`
    }
},
{
    $sort: {
        [property.name]: 1
    }
},
{
    $limit: 1
}
];

const max: MongoSyntaxTemplate = (alias: string, property: Property) => [{
    $project: {
        [alias]: `$${property.name}`
    }
},
{
    $sort: {
        [property.name]: -1
    }
},
{
    $limit: 1
}
];

const least: MongoSyntaxTemplate = (label: string, ...values: any[]) => ({
    [label]: {
        $min: values.reduce(listReferences, [])
    }
});

const greatest: MongoSyntaxTemplate = (label: string, ...values: any[]) => ({
    [label]: {
        $max: values.reduce(listReferences, [])
    }
});

const sum: MongoSyntaxTemplate = (label: string, ...values: any[]) => ({
    [label]: {
        $sum: values.reduce(listReferences, [])
    }
});

const avg: MongoSyntaxTemplate = (label: string, ...values: any[]) => ({
    [label]: {
        $avg: values.reduce(listReferences, [])
    }
});

const sqrt: MongoSyntaxTemplate = (label: string, value: any) => ({
    [label]: {
        $sqrt: isProperty(value) ? `$${value.name}` : value
    }
});

const pow: MongoSyntaxTemplate = (label: string, value: any, exponent: number) => ({
    [label]: {
        $pow: [isProperty(value) ? `$${value.name}` : value, exponent]
    }
});

// ----

const abs: MongoSyntaxTemplate = (label: string, ...values: any[]) => ({
    [label]: {
        $abs: values.reduce(listReferences, [])
    }
});

const ceil: MongoSyntaxTemplate = (label: string, ...values: any[]) => ({
    [label]: {
        $ceil: values.reduce(listReferences, [])
    }
});

const div: MongoSyntaxTemplate = (label: string, ...values: any[]) => ({
    [label]: {
        $divide: values.reduce(listReferences, [])
    }
});

const exp: MongoSyntaxTemplate = (label: string, ...values: any[]) => ({
    [label]: {
        $exp: values.reduce(listReferences, [])
    }
});

const floor: MongoSyntaxTemplate = (label: string, ...values: any[]) => ({
    [label]: {
        $floor: values.reduce(listReferences, [])
    }
});

const ln: MongoSyntaxTemplate = (label: string, ...values: any[]) => ({
    [label]: {
        $ln: values.reduce(listReferences, [])
    }
});

const log: MongoSyntaxTemplate = (label: string, value: any) => ({
    [label]: {
        $log: [isProperty(value) ? `$${value.name}` : value, 10]
    }
});

const log2: MongoSyntaxTemplate = (label: string, value: any) => ({
    [label]: {
        $divide: [{ $ln: isProperty(value) ? `$${value.name}` : value }, { $ln: 2 }]
    }
});

const log10: MongoSyntaxTemplate = (label: string, value: any) => ({
    [label]: {
        $log10: [isProperty(value) ? `$${value.name}` : value]
    }
});

const mod: MongoSyntaxTemplate = (label: string, x: any, y: any) => ({
    [label]: {
        $mod: [isProperty(x) ? `$${x.name}` : x, isProperty(y) ? `$${y.name}` : y]
    }
});

const pi: MongoSyntaxTemplate = (label: string) => ({
    [label]: Math.PI
});

const rand: MongoSyntaxTemplate = (label: string) => ({
    [label]: Math.random()
});

const sign: MongoSyntaxTemplate = (label: string, value: any) => ({
    [label]: {
        $cmp: [isProperty(value) ? `$${value.name}` : value, 0]
    }
});

const truncate: MongoSyntaxTemplate = (label: string, value: any) => ({
    [label]: {
        $divide: [{
            $trunc: {
                $multiply: [isProperty(value) ? `$${value.name}` : value, 100]
            }
        }, 100]
    }
});

const charLength: MongoSyntaxTemplate = (label: string, value: any) => ({
    [label]: {
        $strLenCP: isProperty(value) ? `$${value.name}` : value
    }
});

const concat: MongoSyntaxTemplate = (label: string, ...values: any[]) => ({
    [label]: {
        $concat: values.reduce(listReferences, [])
    }
});

const doConcatWs = (separator: string) => (props: string[], current: any, i: number) => {
    if (i > 0) {
        props.push(separator);
    }
    props.push(isProperty(current) ? `$${current.name}` : current);
    return props;
}

const concatWs: MongoSyntaxTemplate = (label: string, separator: string, ...values: any[]) => ({
    [label]: {
        $concat: values.reduce(doConcatWs(separator), [])
    }
});

const field: MongoSyntaxTemplate = (label: string, search: any, ...values: any[]) => ({
    [label]: {
        $indexOfArray: [values.reduce(listReferences, []), isProperty(search) ? `$${search.name}` : search]
    }
});

const findInSet: MongoSyntaxTemplate = (label: string, search: any, set: any) => ({
    [label]: {
        $indexOfCP: [isProperty(set) ? `$${set.name}` : set, isProperty(search) ? `$${search.name}` : search]
    }
});

const instr: MongoSyntaxTemplate = (label: string, search: any, set: any) => ({
    [label]: {
        $indexOfCP: [isProperty(set) ? `$${set.name}` : set, isProperty(search) ? `$${search.name}` : search]
    }
});

const lcase: MongoSyntaxTemplate = (label: string, value: any) => ({
    [label]: {
        $toLower: isProperty(value) ? `$${value.name}` : value
    }
});

const length: MongoSyntaxTemplate = (label: string, value: any) => ({
    [label]: {
        $strLenBytes: isProperty(value) ? `$${value.name}` : value
    }
});

const ltrim: MongoSyntaxTemplate = (label: string, value: any) => ({
    [label]: {
        $ltrim: isProperty(value) ? `$${value.name}` : value
    }
});

const rtrim: MongoSyntaxTemplate = (label: string, value: any) => ({
    [label]: {
        $rtrim: isProperty(value) ? `$${value.name}` : value
    }
});

const trim: MongoSyntaxTemplate = (label: string, value: any) => ({
    [label]: {
        $trim: isProperty(value) ? `$${value.name}` : value
    }
});

const mid: MongoSyntaxTemplate = (label: string, value: any, start: number, length: number) => ({
    [label]: {
        $substr: [isProperty(value) ? `$${value.name}` : value, start, length]
    }
});

const substr: MongoSyntaxTemplate = (label: string, value: any, start: number, length: number) => ({
    [label]: {
        $substr: [isProperty(value) ? `$${value.name}` : value, start, length]
    }
});

const position: MongoSyntaxTemplate = (label: string, search: any, set: any) => ({
    [label]: {
        $indexOfCP: [isProperty(set) ? `$${set.name}` : set, isProperty(search) ? `$${search.name}` : search]
    }
});

const space: MongoSyntaxTemplate = (label: string, value: number) => ({
    [label]: Array<string>(value).fill(' ').join('')
});

const strcmp: MongoSyntaxTemplate = (label: string, value1: any, value2: any) => ({
    [label]: {
        $strcasecmp: [isProperty(value1) ? `$${value1.name}` : value1, isProperty(value2) ? `$${value2.name}` : value2]
    }
});

const ucase: MongoSyntaxTemplate = (label: string, value: any) => ({
    [label]: {
        $toUpper: isProperty(value) ? `$${value.name}` : value
    }
});

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
    [QuerySyntaxEnum.Max, max],
    [QuerySyntaxEnum.Min, min],
    [QuerySyntaxEnum.Greatest, greatest],
    [QuerySyntaxEnum.Least, least],
    [QuerySyntaxEnum.Sum, sum],
    [QuerySyntaxEnum.Avg, avg],
    [QuerySyntaxEnum.Sqrt, sqrt],
    [QuerySyntaxEnum.Pow, pow],
    [QuerySyntaxEnum.Abs, abs],
    [QuerySyntaxEnum.Ceil, ceil],
    [QuerySyntaxEnum.Div, div],
    [QuerySyntaxEnum.Exp, exp],
    [QuerySyntaxEnum.Floor, floor],
    [QuerySyntaxEnum.Ln, ln],
    [QuerySyntaxEnum.Log, log],
    [QuerySyntaxEnum.Log2, log2],
    [QuerySyntaxEnum.Log10, log10],
    [QuerySyntaxEnum.Mod, mod],
    [QuerySyntaxEnum.Pi, pi],
    [QuerySyntaxEnum.Sign, sign],
    [QuerySyntaxEnum.Truncate, truncate],
    [QuerySyntaxEnum.CharLength, charLength],
    [QuerySyntaxEnum.Concat, concat],
    [QuerySyntaxEnum.ConcatWs, concatWs],
    [QuerySyntaxEnum.Field, field],
    [QuerySyntaxEnum.FindInSet, findInSet],
    [QuerySyntaxEnum.Instr, instr],
    [QuerySyntaxEnum.Length, length],
    [QuerySyntaxEnum.Ltrim, ltrim],
    [QuerySyntaxEnum.Rtrim, rtrim],
    [QuerySyntaxEnum.Trim, trim],
    [QuerySyntaxEnum.Mid, mid],
    [QuerySyntaxEnum.Substr, substr],
    [QuerySyntaxEnum.Position, position],
    [QuerySyntaxEnum.Space, space],
    [QuerySyntaxEnum.Strcmp, strcmp],
    [QuerySyntaxEnum.Ucase, ucase],
    [QuerySyntaxEnum.Lcase, lcase],
    [QuerySyntaxEnum.Locate, position]
]);