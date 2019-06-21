import { Db } from 'mongodb';
import { Table } from '@chego/chego-api';
import { buildConditions } from '../pipelineBuilder';
import { IQueryContext, Expressions } from '@chego/chego-database-boilerplate';

export const removeDocuments = (ref: Db, table: Table, options: object) => async (): Promise<any> =>
    new Promise((resolve, reject) =>
        ref.collection(table.name).deleteMany(options).then(resolve).catch(reject));

const chainRemoveTasks = (ref: Db, expressions: Expressions[]) => (previousTask: Promise<any>, table: Table) =>
    previousTask.then(() => {
        const options: object = expressions.reduce(buildConditions(table), {});
        return removeDocuments(ref, table, options)();
    });

export const runDeletePipeline = async (ref: Db, queryContext: IQueryContext): Promise<any> =>
    new Promise(async (resolve, reject) => 
        queryContext.tables.reduce(chainRemoveTasks(ref, queryContext.expressions),
            Promise.resolve()).then(resolve).catch(reject));