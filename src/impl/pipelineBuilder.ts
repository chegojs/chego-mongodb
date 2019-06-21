import { isMySQLFunction } from '@chego/chego-tools';
import { mergeObjects } from './utils';
import { templates } from './templates';
import { IPipelineBuilder } from '../api/interfaces';
import { QuerySyntaxEnum, Table, SortingData, Limit, FunctionData } from '@chego/chego-api';
import { Join, IQueryContext, Expressions, ExpressionOrExpressionScope, isExpressionScope, Expression } from '@chego/chego-database-boilerplate';
import { pipeline } from 'stream';

const useTemplate = (expression: Expression): object => {
    if (!templates.has(expression.type)) {
        throw new Error(`Template for ${QuerySyntaxEnum[expression.type]} not found`)
    }
    const result: object = templates.get(expression.type)(expression.property, expression.value);
    return (expression.not)
        ? templates.get(QuerySyntaxEnum.Not)(result)
        : result;
}

const parseExpression = (table: Table) => (match: object[], current: ExpressionOrExpressionScope): object[] => {
    if (Array.isArray(current)) {
        match.push(...current.reduce(parseExpression(table),[]));
    } else {
        if (isExpressionScope(current)) {
            const template = templates.get(current.type);
            const expressions: object[] = current.expressions.reduce(parseExpression(table), []);
            if (expressions.length) {
                match.push(template(expressions));
            }
        } else {
            if (table.name === current.property.table.name) {
                match.push(useTemplate(current));
            }
        }
    }
    return match;
}

const handleLeftJoin = (list: object[], join: Join) => {
    if (join.type === QuerySyntaxEnum.LeftJoin) {
        const joins: object[] = <object[]>templates.get(QuerySyntaxEnum.LeftJoin)(join);
        list.push(...joins);
    }
    return list;
}

export const buildConditions = (table: Table) => (match: object, current: ExpressionOrExpressionScope): object => {
        if (isExpressionScope(current)) {
            const template = templates.get(current.type);
            const expressions: object[] = current.expressions.reduce(parseExpression(table), []);
            if (expressions.length) {
                Object.assign(match, template(expressions));
            }
        } else {
            if (table.name === current.property.table.name) {
                Object.assign(match, useTemplate(current));
            }
        }
    return match;
}

const useFacetUnionTemplate = (facet: object, unionKeys: string[]) => [{
    $facet: facet
}, {
    $project: {
        activity: {
            $setUnion: unionKeys
        }
    }
}, {
    $unwind: "$activity"
},
{
    $replaceRoot: {
        newRoot: "$activity"
    }
}];

const isMinMaxCount = (data: any): data is FunctionData =>
    isMySQLFunction(data) && (data.type === QuerySyntaxEnum.Max || data.type === QuerySyntaxEnum.Min || data.type === QuerySyntaxEnum.Count);

const parseKeysToReferences = (list: string[], key: string) => (list.push(`$${key}`), list);

export const newPipelineBuilder = (): IPipelineBuilder => {
    const _group: { [key: string]: any; } = {};
    const _joins: object[] = [];
    const _match: { [key: string]: any; } = {};
    const _orderBy: object = {};
    const _limit: object[] = [];
    const _pipeline: object[] = [];
    const queryLabel: string = 'Q';
    const facet: object = {};

    const buildRegularPipeline = () => {
        _pipeline.push(..._joins);
        if (Object.keys(_match).length) {
            _pipeline.push({ $match: _match });
        }
        for (const key of Object.keys(_group)) {
            _pipeline.push({ [key]: _group[key] });
        }
        if (Object.keys(_orderBy).length) {
            _pipeline.push(_orderBy);
        }
        _pipeline.push(..._limit);
        return _pipeline;
    }

    const withData = (data: any[]) => {
        const props: any[] = [];
        for (const entry of data) {
            if (isMinMaxCount(entry)) {
                const template = templates.get(entry.type);
                Object.assign(facet, { [`${queryLabel}${Object.keys(facet).length}`]: template(entry.alias, entry.param) })
            } else {
                props.push(entry);
            }
        }
        if (props.length) {
            const selection: object | object[] = templates.get(QuerySyntaxEnum.Select)(props);
            if (Array.isArray(selection) && selection.length) {
                mergeObjects(_group, ...selection);
            }
        }
    }
    const withJoins = (joins: Join[]) => {
        joins.reduce(handleLeftJoin, _joins);
    }

    const withOrderBy = (sortings: SortingData[]) => {
        if (sortings.length) {
            Object.assign(_orderBy, templates.get(QuerySyntaxEnum.OrderBy)(sortings));
        }
    }

    const withGroupBy = (sortings: SortingData[]) => {
        if (sortings.length) {
            mergeObjects(_group, templates.get(QuerySyntaxEnum.GroupBy)(sortings));
        }
    }

    const withLimit = (limit: Limit) => {
        if (limit) {
            const limits: object[] = <object[]>templates.get(QuerySyntaxEnum.Limit)(limit);
            _limit.push(...limits);
        }
    }

    const withExpressions = (table: Table, conditions: Expressions[]) => {
        Object.assign(_match, conditions.reduce(buildConditions(table), {}));
    }

    const _builder: IPipelineBuilder = {
        with: (queryContext: IQueryContext, defaultTable: Table): IPipelineBuilder => {
            withData(queryContext.data);
            withJoins(queryContext.joins)
            withOrderBy(queryContext.orderBy);
            withGroupBy(queryContext.groupBy);
            withLimit(queryContext.limit);
            withExpressions(defaultTable, queryContext.expressions);
            return _builder;
        },
        build: (): object[] => {
            const uniqueQueriesCount = Object.keys(facet).length;
            const hasReqularPipelineToBuild = Object.keys(_group).length;
            if (uniqueQueriesCount) {
                if (hasReqularPipelineToBuild) {
                    Object.assign(facet, {
                        [`${queryLabel}${uniqueQueriesCount}`]: buildRegularPipeline()
                    });
                }
                return useFacetUnionTemplate(facet, Object.keys(facet).reduce(parseKeysToReferences, []));
            } else {
                return buildRegularPipeline();
            }
        }
    }

    return _builder;
}
