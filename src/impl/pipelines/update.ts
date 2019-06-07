import { Db } from 'mongodb';
import { Table } from '@chego/chego-api';
import { buildConditions } from '../pipelineBuilder';
import { IQueryContext } from '@chego/chego-database-boilerplate';

export const updateContent = (ref: Db, table: Table, data:any, options:object) => async (): Promise<any> =>
    new Promise((resolve, reject) =>
        ref.collection(table.name).updateMany(options, { $set:data }).then(resolve).catch(reject));

const chainUpdateTasks = (ref: Db, data:any, options:object) => (previousTask:Promise<any>, table:Table) => 
    previousTask.then(updateContent(ref, table, data, options));

export const runUpdatePipeline = async (ref: Db, queryContext: IQueryContext): Promise<any> =>
    new Promise(async (resolve, reject) => {
        const filter:object = queryContext.conditions.reduce(buildConditions, {});
        return queryContext.tables.reduce(chainUpdateTasks(ref, queryContext.data[0], filter), 
            Promise.resolve()).then(resolve).catch(reject);
    });